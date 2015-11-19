# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "lumberjack/beats"
require "lumberjack/beats/server"
require "logstash/codecs/identity_map_codec"

# use Logstash provided json decoder
Lumberjack::Beats::json = LogStash::Json

# Allow Logstash to receive events from Beats
#
# https://github.com/elastic/filebeat[filebeat]
#
class LogStash::Inputs::Beats < LogStash::Inputs::Base
  config_name "beats"

  default :codec, "plain"

  # The IP address to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on.
  config :port, :validate => :number, :required => true

  # Events are by default send in plain text, you can
  # enable encryption by using `ssl` to true and configuring
  # the `ssl_certificate` and `ssl_key` options.
  config :ssl, :validate => :boolean, :default => false

  # SSL certificate to use.
  config :ssl_certificate, :validate => :path

  # SSL key to use.
  config :ssl_key, :validate => :path

  # SSL key passphrase to use.
  config :ssl_key_passphrase, :validate => :password

  # The number of seconds before we raise a timeout,
  # this option is useful to control how much time to wait if something is blocking the pipeline.
  config :congestion_threshold, :validate => :number, :default => 5

  # This is the default field that the specified codec will be applied
  config :target_field_for_codec, :validate => :string, :default => "message"

  # TODO(sissel): Add CA to authenticate clients with.
  BUFFERED_QUEUE_SIZE = 1
  RECONNECT_BACKOFF_SLEEP = 0.5

  def register
    require "concurrent"
    require "logstash/circuit_breaker"
    require "logstash/sized_queue_timeout"

    if !@ssl
      @logger.warn("Beats: SSL Certificate will not be used") unless @ssl_certificate.nil?
      @logger.warn("Beats: SSL Key will not be used") unless @ssl_key.nil?
    elsif !ssl_configured?
      raise LogStash::ConfigurationError, "Certificate or Certificate Key not configured"
    end

    @logger.info("Starting Beats input listener", :address => "#{@host}:#{@port}")
    @lumberjack = Lumberjack::Beats::Server.new(:address => @host, :port => @port,
      :ssl => @ssl, :ssl_certificate => @ssl_certificate, :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase)

    # Create a reusable threadpool, we do not limit the number of connections
    # to the input, the circuit breaker with the timeout should take care
    # of `blocked` threads and prevent logstash to go oom.
    @threadpool = Concurrent::CachedThreadPool.new(:idletime => 15)

    # in 1.5 the main SizeQueue doesnt have the concept of timeout
    # We are using a small plugin buffer to move events to the internal queue
    @buffered_queue = LogStash::SizedQueueTimeout.new(BUFFERED_QUEUE_SIZE)

    @circuit_breaker = LogStash::CircuitBreaker.new("Beats input",
                            :exceptions => [LogStash::SizedQueueTimeout::TimeoutError])

    # wrap the configured codec to support identity stream
    # from the producers
    @codec = LogStash::Codecs::IdentityMapCodec.new(@codec)
  end # def register

  def ssl_configured?
    !(@ssl_certificate.nil? || @ssl_key.nil?)
  end

  def target_codec_on_field?
    !@target_codec_on_field.empty?
  end

  def run(output_queue)
    start_buffer_broker(output_queue)

    while !stop? do
      # Wrapping the accept call into a CircuitBreaker
      if @circuit_breaker.closed?
        connection = @lumberjack.accept # call that creates a new connection
        next if connection.nil? # if the connection is nil the connection was close.

        invoke(connection) do |event|
          if stop?
            connection.close
            break
          end

          begin
            @circuit_breaker.execute {
              @buffered_queue.push(event, @congestion_threshold)
            }
          rescue => e
            raise e
          end
        end
      else
        @logger.warn("Beats input: the pipeline is blocked, temporary refusing new connection.")
        sleep(RECONNECT_BACKOFF_SLEEP)
      end
    end
  end # def run

  public
  def stop
    @lumberjack.close
  end

  public
  def create_event(map, identity_stream)

    # Filebeats uses the `message` key and LSF `line`
    target_field = target_field_for_codec ? map.delete(target_field_for_codec) : nil

    if target_field.nil?
      event = LogStash::Event.new(map)
      copy_beat_hostname(event)
      decorate(event)
      return event
    else
      # All codecs expects to work on string
      @codec.decode(target_field.to_s, identity_stream) do |decoded|
        ts = coerce_ts(map.delete("@timestamp"))
        decoded["@timestamp"] = ts unless ts.nil?
        map.each { |k, v| decoded[k] = v }
        copy_beat_hostname(decoded)
        decorate(decoded)
        return decoded
      end
    end
    return nil
  end

  # Copies the beat.hostname field into the host field unless
  # the host field is already defined
  private
  def copy_beat_hostname(event)
    host = event["beat"] ? event["beat"]["hostname"] : nil
    if host && event["host"].nil?
      event["host"] = host
    end
  end

  private
  def coerce_ts(ts)
    return nil if ts.nil?
    timestamp = LogStash::Timestamp.coerce(ts)
    return timestamp if timestamp

    @logger.warn("Unrecognized @timestamp value, setting current time to @timestamp",
      :value => ts.inspect)
  rescue LogStash::TimestampParserError => e
    @logger.warn("Error parsing @timestamp string, setting current time to @timestamp",
      :value => ts.inspect, :exception => e.message)
  end

  private
  def invoke(connection, &block)
    @threadpool.post do
      begin
        # If any errors occur in from the events the connection should be closed in the
        # library ensure block and the exception will be handled here
        connection.run do |map, identity_stream|
          event = create_event(map, identity_stream)
          block.call(event) unless event.nil?
        end

        # When too many errors happen inside the circuit breaker it will throw
        # this exception and start refusing connection. The bubbling of theses
        # exceptions make sure that the lumberjack library will close the current
        # connection which will force the client to reconnect and restransmit
        # his payload.
      rescue LogStash::CircuitBreaker::OpenBreaker,
             LogStash::CircuitBreaker::HalfOpenBreaker => e
        logger.warn("Beats input: The circuit breaker has detected a slowdown or stall in the pipeline, the input is closing the current connection and rejecting new connection until the pipeline recover.", :exception => e.class)
      rescue => e # If we have a malformed packet we should handle that so the input doesn't crash completely.
        @logger.error("Beats input: unhandled exception", :exception => e, :backtrace => e.backtrace)
      end
    end
  end

  # The default Logstash Sizequeue doesn't support timeouts.
  # This is problematic because this will make the client timeouts
  # and reconnect creating multiples threads and OOMing the jvm.
  #
  # We are using a proxy queue supporting blocking with a timeout and
  # this thread take the element from one queue into another one.
  def start_buffer_broker(output_queue)
    @threadpool.post do
      while !stop?
        output_queue << @buffered_queue.pop_no_timeout
      end
    end
  end
end # class LogStash::Inputs::Beats

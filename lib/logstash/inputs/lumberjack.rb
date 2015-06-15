# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

# Receive events using the lumberjack protocol.
#
# This is mainly to receive events shipped with lumberjack[http://github.com/jordansissel/lumberjack],
# now represented primarily via the
# https://github.com/elasticsearch/logstash-forwarder[Logstash-forwarder].
#
class LogStash::Inputs::Lumberjack < LogStash::Inputs::Base

  config_name "lumberjack"

  default :codec, "plain"

  # The IP address to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on.
  config :port, :validate => :number, :required => true

  # SSL certificate to use.
  config :ssl_certificate, :validate => :path, :required => true

  # SSL key to use.
  config :ssl_key, :validate => :path, :required => true

  # SSL key passphrase to use.
  config :ssl_key_passphrase, :validate => :password

  # The lumberjack input using a fixed thread pool to do the actual work and
  # will accept a number of client in a queue, before starting to refuse new
  # connection. This solve an issue when logstash-forwarder clients are 
  # trying to connect to logstash which have a blocked pipeline and will 
  # make logstash crash with an out of memory exception.
  config :max_clients, :validate => :number, :default => 1000

  # TODO(sissel): Add CA to authenticate clients with.

  BUFFERED_QUEUE_SIZE = 20
  RECONNECT_BACKOFF_SLEEP = 0.5
  
  def register
    require "lumberjack/server"
    require "concurrent/executors"
    require "logstash/circuit_breaker"
    require "logstash/sized_queue_timeout"

    @logger.info("Starting lumberjack input listener", :address => "#{@host}:#{@port}")
    @lumberjack = Lumberjack::Server.new(:address => @host, :port => @port,
      :ssl_certificate => @ssl_certificate, :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase)

    # Limit the number of thread that can be created by the
    # Limit the number of thread that can be created by the 
    # lumberjack output, if the queue is full the input will 
    # start rejecting new connection and raise an exception
    @threadpool = Concurrent::ThreadPoolExecutor.new(
      :min_threads => 1,
      :max_threads => @max_clients,
      :max_queue => 1, # in concurrent-ruby, bounded queue need to be at least 1.
      fallback_policy: :abort
    )
    @threadpool = Concurrent::CachedThreadPool.new(:idletime => 15)

    # in 1.5 the main SizeQueue doesnt have the concept of timeout
    # We are using a small plugin buffer to move events to the internal queue
    @buffered_queue = LogStash::SizedQueueTimeout.new(BUFFERED_QUEUE_SIZE)

    @circuit_breaker = LogStash::CircuitBreaker.new("Lumberjack input",
                            :exceptions => [LogStash::SizedQueueTimeout::TimeoutError])

  end # def register

  def run(output_queue)
    start_buffer_broker(output_queue)

    while true do
      begin
        # Wrapping the accept call into a CircuitBreaker
        if @circuit_breaker.closed?
          connection = @lumberjack.accept # Blocking call that creates a new connection

          invoke(connection, codec.clone) do |_codec, line, fields|
            _codec.decode(line) do |event|
              decorate(event)
              fields.each { |k,v| event[k] = v; v.force_encoding(Encoding::UTF_8) }

              @circuit_breaker.execute { @buffered_queue << event }
            end
          end
        else
          @logger.warn("Lumberjack input: the pipeline is blocked, temporary refusing new connection.")
          sleep(RECONNECT_BACKOFF_SLEEP)
        end
        # When too many errors happen inside the circuit breaker it will throw 
        # this exception and start refusing connection, we need to catch it but 
        # it's safe to ignore.
      rescue LogStash::CircuitBreaker::OpenBreaker => e
      end
    end
  rescue LogStash::ShutdownSignal
    @logger.info("Lumberjack input: received ShutdownSignal")
  rescue => e
    @logger.error("Lumberjack input: unhandled exception", :exception => e, :backtrace => e.backtrace)
  ensure
    shutdown(output_queue)
  end # def run

  private
  def accept(&block)
    connection = @lumberjack.accept # Blocking call that creates a new connection
    block.call(connection, @codec.clone)
  end

  private
  def invoke(connection, codec, &block)
    @threadpool.post do
      begin
        connection.run do |fields|
          block.call(codec, fields.delete("line"), fields)
        end
      rescue => e
        @logger.error("Exception in lumberjack input thread", :exception => e)
      end
    end
  end

  def start_buffer_broker(output_queue)
    @threadpool.post do
      while true
        output_queue << @buffered_queue.pop_no_timeout
      end
    end
  end
end # class LogStash::Inputs::Lumberjack

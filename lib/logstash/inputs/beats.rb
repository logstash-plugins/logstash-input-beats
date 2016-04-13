# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "lumberjack/beats"
require "lumberjack/beats/server"
require "logstash/codecs/identity_map_codec"
require "logstash/codecs/multiline"
require "logstash/inputs/beats_support/circuit_breaker"
require "logstash/inputs/beats_support/codec_callback_listener"
require "logstash/inputs/beats_support/connection_handler"
require "logstash/inputs/beats_support/event_transform_common"
require "logstash/inputs/beats_support/decoded_event_transform"
require "logstash/inputs/beats_support/raw_event_transform"
require "logstash/inputs/beats_support/synchronous_queue_with_offer"
require "logstash/util"
require "thread_safe"

# use Logstash provided json decoder
Lumberjack::Beats::json = LogStash::Json

# Allow Logstash to receive events from Beats
#
# https://github.com/elastic/filebeat[filebeat]
#

class LogStash::Codecs::Base
  # This monkey patch add callback based
  # flow to the codec until its shipped with core.
  # This give greater flexibility to the implementation by
  # sending more data to the actual block.
  if !method_defined?(:accept)
    def accept(listener)
      decode(listener.data) do |event|
        listener.process_event(event)
      end
    end
  end
  if !method_defined?(:auto_flush)
    def auto_flush(*)
    end
  end
end

class LogStash::Inputs::Beats < LogStash::Inputs::Base
  class InsertingToQueueTakeTooLong < Exception; end

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

  # Validate client certificates against theses authorities
  # You can defined multiples files or path, all the certificates will
  # be read and added to the trust store. You need to configure the `ssl_verify_mode`
  # to `peer` or `force_peer` to enable the verification.
  #
  # This feature only support certificate directly signed by your root ca.
  # Intermediate CA are currently not supported.
  # 
  config :ssl_certificate_authorities, :validate => :array, :default => []

  # By default the server dont do any client verification,
  # 
  # `peer` will make the server ask the client to provide a certificate,
  # if the client provide the certificate it will be validated.
  #
  # `force_peer` will make the server ask the client for their certificate, if the clients
  # doesn't provide it the connection will be closed.
  #
  # This option need to be used with `ssl_certificate_authorities` and a defined list of CA.
  config :ssl_verify_mode, :validate => ["none", "peer", "force_peer"], :default => "none"

  # The number of seconds before we raise a timeout,
  # this option is useful to control how much time to wait if something is blocking the pipeline.
  config :congestion_threshold, :validate => :number, :default => 5

  # This is the default field that the specified codec will be applied
  config :target_field_for_codec, :validate => :string, :default => "message", :deprecated => "This option is now deprecated, the plugin is now compatible with Filebeat and Logstash-Forwarder"

  # TODO(sissel): Add CA to authenticate clients with.
  RECONNECT_BACKOFF_SLEEP = 0.5

  def register
    if !@ssl
      @logger.warn("Beats input: SSL Certificate will not be used") unless @ssl_certificate.nil?
      @logger.warn("Beats input: SSL Key will not be used") unless @ssl_key.nil?
    elsif !ssl_configured?
      raise LogStash::ConfigurationError, "Certificate or Certificate Key not configured"
    end

    @logger.info("Beats inputs: Starting input listener", :address => "#{@host}:#{@port}")


    @lumberjack = Lumberjack::Beats::Server.new(:address => @host,
      :port => @port,
      :ssl => @ssl,
      :ssl_certificate => @ssl_certificate,
      :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase ? @ssl_key_passphrase.value : nil,
      :ssl_certificate_authorities => @ssl_certificate_authorities,
      :ssl_verify_mode => @ssl_verify_mode)

    # in 1.5 the main SizeQueue doesnt have the concept of timeout
    # We are using a small plugin buffer to move events to the internal queue
    @buffered_queue = LogStash::Inputs::BeatsSupport::SynchronousQueueWithOffer.new(@congestion_threshold)

    @circuit_breaker = LogStash::Inputs::BeatsSupport::CircuitBreaker.new("Beats input",
                            :exceptions => [InsertingToQueueTakeTooLong])

    # wrap the configured codec to support identity stream
    # from the producers if running with the multiline codec.
    #
    # If they dont need an identity map, codec are stateless and can be reused
    # accross multiples connections.
    if need_identity_map?
      @codec = LogStash::Codecs::IdentityMapCodec.new(@codec)
    end

    # Keep a list of active connections so we can flush their codec on shutdown

    # Use threadsafe gem, since we have a strict dependency on concurrent-ruby 0.9.2
    # in the core
    @connections_list = ThreadSafe::Hash.new
  end # def register

  def ssl_configured?
    !(@ssl_certificate.nil? || @ssl_key.nil?)
  end

  def target_codec_on_field?
    !@target_codec_on_field.empty?
  end

  def run(output_queue)
    @output_queue = output_queue
    start_buffer_broker

    while !stop? do
      # Wrapping the accept call into a CircuitBreaker
      if @circuit_breaker.closed?
        connection = @lumberjack.accept # call that creates a new connection
        # if the connection is nil the connection was closed upstream,
        # so we will try in another iteration to recover or stop.
        next if connection.nil?

        Thread.new do
          handle_new_connection(connection)
        end
      else
        @logger.warn("Beats input: the pipeline is blocked, temporary refusing new connection.",
                     :reconnect_backoff_sleep => RECONNECT_BACKOFF_SLEEP)
        sleep(RECONNECT_BACKOFF_SLEEP)
      end
    end
  end # def run

  def stop
    @logger.debug("Beats input: stopping the plugin")

    @lumberjack.close rescue nil

    # we may have some stuff in the codec buffer
    transformer = LogStash::Inputs::BeatsSupport::EventTransformCommon.new(self)

    # Go through all the active connection and flush their
    # codec content, some context data could be lost in this case
    # but at least the events main data would be persisted.
    @connections_list.each do |_, connection_handler|
      connection_handler.flush do |event|
        # We might loose some context of the
        transformer.transform(event)
        event.tag("beats_input_flushed_by_logtash_shutdown")
        @output_queue << event
      end
    end

    @logger.debug("Beats input: stopped")
  end

  # This Method is called inside a new thread
  def handle_new_connection(connection)
    @logger.debug? && @logger.debug("Beats inputs: accepting a new connection",
                                  :peer => connection.peer)

    LogStash::Util.set_thread_name("[beats-input]>connection-#{connection.peer}")

    connection_handler = LogStash::Inputs::BeatsSupport::ConnectionHandler.new(connection, self, @buffered_queue)
    @connections_list[connection] = connection_handler

    # All the errors handling is done here
    @circuit_breaker.execute { connection_handler.accept }
  rescue Lumberjack::Beats::Connection::ConnectionClosed => e
    # Only show theses errors if we run in debug mode,
    # This logger can get noisy if we use a proxy that check if the host is alive.
    @logger.debug? && @logger.debug("Beats Input: Remote connection closed",
                                  :peer => connection.peer,
                                  :exception => e)
  rescue LogStash::Inputs::BeatsSupport::CircuitBreaker::OpenBreaker,
    LogStash::Inputs::BeatsSupport::CircuitBreaker::HalfOpenBreaker => e
    @logger.warn("Beats input: The circuit breaker has detected a slowdown or stall in the pipeline, the input is closing the current connection and rejecting new connection until the pipeline recover.",
                :exception => e.class)
  rescue Exception => e # If we have a malformed packet we should handle that so the input doesn't crash completely.
    @logger.error("Beats input: unhandled exception",
                  :exception => e,
                  :backtrace => e.backtrace)
  ensure
    transformer = LogStash::Inputs::BeatsSupport::EventTransformCommon.new(self)

    connection_handler.flush do |event|
      # handle the basic event enrichment with tags
      # since at that time we lose all the context
      transformer.transform(event)
      event.tag("beats_input_flushed_by_end_of_connection")
      @output_queue << event
    end

    @connections_list.delete(connection)
    @logger.debug? && @logger.debug("Beats input: clearing the connection from the known clients",
                                    :peer => connection.peer)
  end

  # The default Logstash Sizequeue doesn't support timeouts.
  # This is problematic because this will make the client timeouts
  # and reconnect creating multiples threads and OOMing the jvm.
  #
  # We are using a proxy queue supporting blocking with a timeout and
  # this thread take the element from one queue into another one.
  def start_buffer_broker
    Thread.new do
      LogStash::Util.set_thread_name("[beats-input]-buffered-queue-broker")

      begin
        while !stop?
          @output_queue << @buffered_queue.take
        end
      rescue java.lang.InterruptedException => e
        # If we are shutting down without waiting the queue to unblock
        # we will get an `java.lang.InterruptionException` in that context we will not log it.
        @logger.error("Beats input: bufferered queue exception", :exception => e) unless stop?
      rescue => e
        @logger.error("Beats input: unexpected exception", :exception => e)
      end
    end
  end

  def need_identity_map?
    @codec.kind_of?(LogStash::Codecs::Multiline)
  end
end # class LogStash::Inputs::Beats

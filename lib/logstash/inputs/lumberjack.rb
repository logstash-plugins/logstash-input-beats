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

  # Number of maximum clients that the lumberjack input will accept, this allow you
  # to control the back pressure up to the client and stop logstash to go OOM with 
  # connection. This settings is a temporary solution and will be deprecated really soon.
  config :max_clients, :validate => :number, :default => 1000, :deprecated => true

  # TODO(sissel): Add CA to authenticate clients with.

  public
  def register
    require "lumberjack/server"
    require "concurrent"
    require "concurrent/executors"

    @logger.info("Starting lumberjack input listener", :address => "#{@host}:#{@port}")
    @lumberjack = Lumberjack::Server.new(:address => @host, :port => @port,
      :ssl_certificate => @ssl_certificate, :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase)

    # Limit the number of thread that can be created by the
    # lumberjack output, if the queue is full the input will 
    # start rejecting new connection and raise an exception
    @threadpool = Concurrent::ThreadPoolExecutor.new(
      :min_threads => 1,
      :max_threads => @max_clients,
      :max_queue => 1, # in concurrent-ruby, bounded queue need to be at least 1.
      fallback_policy: :abort
    )
  end # def register

  def run(output_queue)
    while true do
      accept do |connection, codec|
        invoke(connection, codec) do |_codec, line, fields|
          _codec.decode(line) do |event|
            decorate(event)
            fields.each { |k,v| event[k] = v; v.force_encoding(Encoding::UTF_8) }
            output_queue << event
          end
        end
      end
    end
  rescue => e
    @logger.error("Exception in lumberjack input", :exception => e)
    shutdown(output_queue)
  end # def run

  private
  def accept(&block)
    connection = @lumberjack.accept # Blocking call that creates a new connection
    
    if @threadpool.length < @threadpool.max_length
      block.call(connection, @codec.clone)
    else
      @logger.warn("Lumberjack input, maximum connection exceeded, new connection are rejected.", :max_clients => @max_clients_queue)
      connection.close
    end
  end

  def invoke(connection, codec, &block)
    @threadpool.post do
      connection.run do |fields|
        block.call(codec, fields.delete("line"), fields)
      end
    end
  end
end # class LogStash::Inputs::Lumberjack

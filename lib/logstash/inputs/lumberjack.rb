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

  # TODO(sissel): Add CA to authenticate clients with.

  public
  def register
    require "lumberjack/server"

    @logger.info("Starting lumberjack input listener", :address => "#{@host}:#{@port}")
    @lumberjack = Lumberjack::Server.new(:address => @host, :port => @port,
      :ssl_certificate => @ssl_certificate, :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase)
  end # def register

  public
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
    block.call(connection, @codec.clone)
  end

  private
  def invoke(connection, codec, &block)
    Thread.new(connection, codec) do |_connection, _codec|
      _connection.run do |fields|
        block.call(_codec, fields.delete("line"), fields)
      end
    end
  end

end # class LogStash::Inputs::Lumberjack

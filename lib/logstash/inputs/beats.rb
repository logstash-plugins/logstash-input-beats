# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/codecs/identity_map_codec"
require "logstash/codecs/multiline"
require "logstash/util"
require "logstash-input-beats-3.1.0.beta1.jar"
require "logstash-input-beats_jars"

import "org.logstash.beats.Server"
import "org.logstash.netty.SslSimpleBuilder"
import "org.logstash.netty.PrivateKeyConverter"
import "java.io.FileInputStream"

# This input plugin enables Logstash to receive events from the
# https://www.elastic.co/products/beats[Elastic Beats] framework.
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
  require "logstash/inputs/beats/codec_callback_listener"
  require "logstash/inputs/beats/event_transform_common"
  require "logstash/inputs/beats/decoded_event_transform"
  require "logstash/inputs/beats/raw_event_transform"
  require "logstash/inputs/beats/message_listener"
  require "logstash/inputs/beats/tls"

  config_name "beats"

  default :codec, "plain"

  # The IP address to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on.
  config :port, :validate => :number, :required => true

  # Events are by default sent in plain text. You can
  # enable encryption by setting `ssl` to true and configuring
  # the `ssl_certificate` and `ssl_key` options.
  config :ssl, :validate => :boolean, :default => false

  # SSL certificate to use.
  config :ssl_certificate, :validate => :path

  # SSL key to use.
  config :ssl_key, :validate => :path

  # SSL key passphrase to use.
  config :ssl_key_passphrase, :validate => :password

  # Validate client certificates against these authorities. 
  # You can define multiple files or paths. All the certificates will
  # be read and added to the trust store. You need to configure the `ssl_verify_mode`
  # to `peer` or `force_peer` to enable the verification.
  #
  # This feature only supports certificates that are directly signed by your root CA.
  # Intermediate CAs are currently not supported.
  # 
  config :ssl_certificate_authorities, :validate => :array, :default => []

  # By default the server doesn't do any client verification.
  # 
  # `peer` will make the server ask the client to provide a certificate. 
  # If the client provides a certificate, it will be validated.
  #
  # `force_peer` will make the server ask the client to provide a certificate.
  # If the client doesn't provide a certificate, the connection will be closed.
  #
  # This option needs to be used with `ssl_certificate_authorities` and a defined list of CAs.
  config :ssl_verify_mode, :validate => ["none", "peer", "force_peer"], :default => "none"

  # The number of seconds before we raise a timeout. 
  # This option is useful to control how much time to wait if something is blocking the pipeline.
  config :congestion_threshold, :validate => :number, :default => 5

  # This is the default field to which the specified codec will be applied.
  config :target_field_for_codec, :validate => :string, :default => "message", :deprecated => "This option is now deprecated, the plugin is now compatible with Filebeat and Logstash-Forwarder"

  # The minimum TLS version allowed for the encrypted connections. The value must be one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_min_version, :validate => :number, :default => TLS.min.version

  # The maximum TLS version allowed for the encrypted connections. The value must be the one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_max_version, :validate => :number, :default => TLS.max.version

  # The list of ciphers suite to use, listed by priorities.
  config :cipher_suites, :validate => :array, :default => org.logstash.netty.SslSimpleBuilder::DEFAULT_CIPHERS

  def register
    if !@ssl
      @logger.warn("Beats input: SSL Certificate will not be used") unless @ssl_certificate.nil?
      @logger.warn("Beats input: SSL Key will not be used") unless @ssl_key.nil?
    elsif !ssl_configured?
      raise LogStash::ConfigurationError, "Certificate or Certificate Key not configured"
    end

    @logger.info("Beats inputs: Starting input listener", :address => "#{@host}:#{@port}")

    # wrap the configured codec to support identity stream
    # from the producers if running with the multiline codec.
    #
    # If they dont need an identity map, codec are stateless and can be reused
    # accross multiples connections.
    if need_identity_map?
      @codec = LogStash::Codecs::IdentityMapCodec.new(@codec)
    end

    @server = create_server
  end # def register

  def create_server
    server = org.logstash.beats.Server.new(@port)
    if @ssl
      private_key_converter = org.logstash.netty.PrivateKeyConverter.new(ssl_key, ssl_key_passphrase)
      ssl_builder = org.logstash.netty.SslSimpleBuilder.new(FileInputStream.new(ssl_certificate), private_key_converter.convert(), ssl_key_passphrase)
        .setProtocols(convert_protocols)
        .setCipherSuites(normalized_ciphers)

      if client_authentification?
        if @ssl_verify_mode.upcase == "FORCE_PEER"
            ssl_builder.setVerifyMode(org.logstash.netty.SslSimpleBuilder::SslClientVerifyMode::FORCE_PEER)
        end

        ssl_builder.setCertificateAuthorities(@ssl_certificate_authorities)
      end

      server.enableSSL(ssl_builder)
    end

    server
  end

  def ssl_configured?
    !(@ssl_certificate.nil? || @ssl_key.nil?)
  end

  def target_codec_on_field?
    !@target_codec_on_field.empty?
  end

  def run(output_queue)
    message_listener = MessageListener.new(output_queue, self)
    @server.setMessageListener(message_listener)
    @server.listen
  end # def run

  def stop
    @server.stop
  end

  def need_identity_map?
    @codec.kind_of?(LogStash::Codecs::Multiline)
  end

  def client_authentification?
    @ssl_certificate_authorities && @ssl_certificate_authorities.size > 0
  end

  def normalized_ciphers
    @cipher_suites.map(&:upcase)
  end

  def convert_protocols
    TLS.get_supported(@tls_min_version..@tls_max_version).map(&:name)
  end
end

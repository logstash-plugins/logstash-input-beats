# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/codecs/multiline"
require "logstash/util"
require "logstash-input-beats_jars"
require "logstash/plugin_mixins/ecs_compatibility_support"
require 'logstash/plugin_mixins/event_support/event_factory_adapter'
require_relative "beats/patch"

# This input plugin enables Logstash to receive events from the
# https://www.elastic.co/products/beats[Elastic Beats] framework.
#
# The following example shows how to configure Logstash to listen on port
# 5044 for incoming Beats connections and to index into Elasticsearch:
#
# [source,ruby]
# ------------------------------------------------------------------------------
# input {
#   beats {
#     port => 5044
#   }
# }
# 
# output {
#   elasticsearch {
#     hosts => "localhost:9200"
#     manage_template => false
#     index => "%{[@metadata][beat]}-%{+YYYY.MM.dd}"
#     document_type => "%{[@metadata][type]}"
#   }
# }
# ------------------------------------------------------------------------------
#
# NOTE: The Beats shipper automatically sets the `type` field on the event.
# You cannot override this setting in the Logstash config. If you specify
# a setting for the <<plugins-inputs-beats-type,`type`>> config option in
# Logstash, it is ignored.
#
# IMPORTANT: If you are shipping events that span multiple lines, you need to
# use the configuration options available in Filebeat to handle multiline events
# before sending the event data to Logstash. You cannot use the
# <<plugins-codecs-multiline>> codec to handle multiline events.
#
class LogStash::Inputs::Beats < LogStash::Inputs::Base
  require "logstash/inputs/beats/codec_callback_listener"
  require "logstash/inputs/beats/event_transform_common"
  require "logstash/inputs/beats/decoded_event_transform"
  require "logstash/inputs/beats/raw_event_transform"
  require "logstash/inputs/beats/message_listener"
  require "logstash/inputs/beats/tls"

  # adds ecs_compatibility config which could be :disabled or :v1
  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled,:v1, :v8 => :v1)

  include LogStash::PluginMixins::EventSupport::EventFactoryAdapter

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
  # NOTE: This key need to be in the PKCS8 format, you can convert it with https://www.openssl.org/docs/man1.1.0/apps/pkcs8.html[OpenSSL]
  # for more information.
  config :ssl_key, :validate => :path

  # SSL key passphrase to use.
  config :ssl_key_passphrase, :validate => :password

  # Validate client certificates against these authorities. 
  # You can define multiple files or paths. All the certificates will
  # be read and added to the trust store. You need to configure the `ssl_verify_mode`
  # to `peer` or `force_peer` to enable the verification.
  # 
  config :ssl_certificate_authorities, :validate => :array, :default => []

  # Flag to determine whether to add host information (provided by the beat in the 'hostname' field) to the event
  config :add_hostname, :validate => :boolean, :default => false, :deprecated => 'This option will be removed in the future as beats determine the event schema'

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

  # Enables storing client certificate information in event's metadata. You need 
  # to configure the `ssl_verify_mode` to `peer` or `force_peer` to enable this.
  config :ssl_peer_metadata, :validate => :boolean, :default => false

  config :include_codec_tag, :validate => :boolean, :default => true

  # Time in milliseconds for an incomplete ssl handshake to timeout
  config :ssl_handshake_timeout, :validate => :number, :default => 10000

  # The minimum TLS version allowed for the encrypted connections. The value must be one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_min_version, :validate => :number, :default => TLS.min.version

  # The maximum TLS version allowed for the encrypted connections. The value must be the one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_max_version, :validate => :number, :default => TLS.max.version

  # The list of ciphers suite to use, listed by priorities.
  config :cipher_suites, :validate => :array, :default => org.logstash.netty.SslContextBuilder.getDefaultCiphers
  # Close Idle clients after X seconds of inactivity.
  config :client_inactivity_timeout, :validate => :number, :default => 60

  # Beats handler executor thread
  config :executor_threads, :validate => :number, :default => LogStash::Config::CpuCoreStrategy.maximum

  attr_reader :field_hostname, :field_hostip
  attr_reader :field_tls_protocol_version, :field_tls_peer_subject, :field_tls_cipher

  def register
    # For Logstash 2.4 we need to make sure that the logger is correctly set for the
    # java classes before actually loading them.
    #
    # if we don't do this we will get this error:
    # log4j:WARN No appenders could be found for logger (io.netty.util.internal.logging.InternalLoggerFactory)
    if defined?(LogStash::Logger) && LogStash::Logger.respond_to?(:setup_log4j)
      LogStash::Logger.setup_log4j(@logger)
    end

    if @ssl
      if @ssl_key.nil? || @ssl_key.empty?
        configuration_error "ssl_key => is a required setting when ssl => true is configured"
      end
      if @ssl_certificate.nil? || @ssl_certificate.empty?
        configuration_error "ssl_certificate => is a required setting when ssl => true is configured"
      end

      if require_certificate_authorities? && !client_authentification?
        configuration_error "ssl_certificate_authorities => is a required setting when ssl_verify_mode => '#{@ssl_verify_mode}' is configured"
      end

      if client_authentication_metadata? && !require_certificate_authorities?
        configuration_error "Configuring ssl_peer_metadata => true requires ssl_verify_mode => to be configured with 'peer' or 'force_peer'"
      end
    else
      @logger.warn("configured ssl_certificate => #{@ssl_certificate.inspect} will not be used") if @ssl_certificate
      @logger.warn("configured ssl_key => #{@ssl_key.inspect} will not be used") if @ssl_key
    end

    # Logstash 6.x breaking change (introduced with 4.0.0 of this gem)
    if @codec.kind_of? LogStash::Codecs::Multiline
      configuration_error "Multiline codec with beats input is not supported. Please refer to the beats documentation for how to best manage multiline data. See https://www.elastic.co/guide/en/beats/filebeat/current/multiline-examples.html"
    end

    # define ecs name mapping
    @field_hostname = ecs_select[disabled: "host", v1: "[@metadata][input][beats][host][name]"]
    @field_hostip = ecs_select[disabled: "[@metadata][ip_address]", v1: "[@metadata][input][beats][host][ip]"]
    @field_tls_protocol_version = ecs_select[disabled: "[@metadata][tls_peer][protocol]", v1: "[@metadata][input][beats][tls][version_protocol]"]
    @field_tls_peer_subject = ecs_select[disabled: "[@metadata][tls_peer][subject]", v1: "[@metadata][input][beats][tls][client][subject]"]
    @field_tls_cipher = ecs_select[disabled: "[@metadata][tls_peer][cipher_suite]", v1: "[@metadata][input][beats][tls][cipher]"]

    @logger.info("Starting input listener", :address => "#{@host}:#{@port}")

    @server = create_server
  end # def register

  def create_server
    server = org.logstash.beats.Server.new(@host, @port, @client_inactivity_timeout, @executor_threads)
    if @ssl
      ssl_context_builder = new_ssl_context_builder
      if client_authentification?
        if @ssl_verify_mode == "force_peer"
          ssl_context_builder.setVerifyMode(org.logstash.netty.SslContextBuilder::SslClientVerifyMode::FORCE_PEER)
        elsif @ssl_verify_mode == "peer"
          ssl_context_builder.setVerifyMode(org.logstash.netty.SslContextBuilder::SslClientVerifyMode::VERIFY_PEER)
        end
        ssl_context_builder.setCertificateAuthorities(@ssl_certificate_authorities)
      end
      server.setSslHandlerProvider(new_ssl_handshake_provider(ssl_context_builder))
    end
    server
  end

  def run(output_queue)
    message_listener = MessageListener.new(output_queue, self)
    @server.setMessageListener(message_listener)
    @server.listen
  end # def run

  def stop
    @server.stop unless @server.nil?
  end

  def ssl_configured?
    !(@ssl_certificate.nil? || @ssl_key.nil?)
  end

  def target_codec_on_field?
    !@target_codec_on_field.empty?
  end

  def client_authentification?
    @ssl_certificate_authorities && @ssl_certificate_authorities.size > 0
  end

  def client_authentication_metadata?
    @ssl_peer_metadata && ssl_configured? && client_authentification? 
  end

  def client_authentication_required?
    @ssl_verify_mode == "force_peer" 
  end

  def require_certificate_authorities?
    @ssl_verify_mode == "force_peer" || @ssl_verify_mode == "peer"
  end

  private

  def new_ssl_handshake_provider(ssl_context_builder)
    begin
      org.logstash.netty.SslHandlerProvider.new(ssl_context_builder.build_context, @ssl_handshake_timeout)
    rescue java.lang.IllegalArgumentException => e
      @logger.error("SSL configuration invalid", error_details(e))
      raise LogStash::ConfigurationError, e
    rescue java.lang.Exception => e # java.security.GeneralSecurityException
      @logger.error("SSL configuration failed", error_details(e, true))
      raise e
    end
  end

  def new_ssl_context_builder
    passphrase = @ssl_key_passphrase.nil? ? nil : @ssl_key_passphrase.value
    begin
      org.logstash.netty.SslContextBuilder.new(@ssl_certificate, @ssl_key, passphrase)
          .setProtocols(convert_protocols)
          .setCipherSuites(normalized_ciphers)
    rescue java.lang.IllegalArgumentException => e
      @logger.error("SSL configuration invalid", error_details(e))
      raise LogStash::ConfigurationError, e
    end
  end

  def normalized_ciphers
    @cipher_suites.map(&:upcase)
  end

  def convert_protocols
    TLS.get_supported(@tls_min_version..@tls_max_version).map(&:name)
  end

  def configuration_error(message)
    @logger.error message
    raise LogStash::ConfigurationError, message
  end

  def error_details(e, trace = false)
    error_details = { :exception => e.class, :message => e.message }
    error_details[:backtrace] = e.backtrace if trace || @logger.debug?
    cause = e.cause
    if cause && e != cause
      error_details[:cause] = { :exception => cause.class, :message => cause.message }
      error_details[:cause][:backtrace] = cause.backtrace if trace || @logger.debug?
    end
    error_details
  end

end

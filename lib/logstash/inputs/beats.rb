# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "logstash/codecs/multiline"
require "logstash/util"
require "logstash-input-beats_jars"
require "logstash/plugin_mixins/ecs_compatibility_support"
require 'logstash/plugin_mixins/plugin_factory_support'
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

  java_import 'org.logstash.netty.SslContextBuilder'

  # adds ecs_compatibility config which could be :disabled or :v1
  include LogStash::PluginMixins::ECSCompatibilitySupport(:disabled,:v1, :v8 => :v1)

  include LogStash::PluginMixins::EventSupport::EventFactoryAdapter

  include LogStash::PluginMixins::PluginFactorySupport

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
  config :ssl_peer_metadata, :validate => :boolean, :default => false, :deprecated => "use `enrich` option to configure which enrichments to perform"

  config :include_codec_tag, :validate => :boolean, :default => true, :deprecated => "use `enrich` option to configure which enrichments to perform"

  # Time in milliseconds for an incomplete ssl handshake to timeout
  config :ssl_handshake_timeout, :validate => :number, :default => 10000

  config :ssl_cipher_suites, :validate => SslContextBuilder::SUPPORTED_CIPHERS.to_a,
         :default => SslContextBuilder.getDefaultCiphers, :list => true

  config :ssl_supported_protocols, :validate => ['TLSv1.1', 'TLSv1.2', 'TLSv1.3'], :default => ['TLSv1.2', 'TLSv1.3'], :list => true

  # Close Idle clients after X seconds of inactivity.
  config :client_inactivity_timeout, :validate => :number, :default => 60

  # Beats handler executor thread
  config :executor_threads, :validate => :number, :default => LogStash::Config::CpuCoreStrategy.maximum

  # Flag to determine whether to add host information (provided by the beat in the 'hostname' field) to the event
  config :add_hostname, :validate => :boolean, :default => false, :deprecated => 'This option will be removed in the future as beats determine the event schema'

  # The list of ciphers suite to use, listed by priorities.
  config :cipher_suites, :validate => :array, :default => [], :deprecated => "Set 'ssl_cipher_suites' instead."

  # The minimum TLS version allowed for the encrypted connections. The value must be one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_min_version, :validate => :number, :default => TLS.min.version, :deprecated => "Set 'ssl_supported_protocols' instead."

  # The maximum TLS version allowed for the encrypted connections. The value must be the one of the following:
  # 1.0 for TLS 1.0, 1.1 for TLS 1.1, 1.2 for TLS 1.2
  config :tls_max_version, :validate => :number, :default => TLS.max.version, :deprecated => "Set 'ssl_supported_protocols' instead."

  ENRICH_DEFAULTS = {
    'source_metadata'   => true,
    'codec_metadata'    => true,
    'ssl_peer_metadata' => false,
  }.freeze

  ENRICH_ALL = ENRICH_DEFAULTS.keys.freeze
  ENRICH_DEFAULT = ENRICH_DEFAULTS.select { |_,v| v }.keys.freeze
  ENRICH_NONE = ['none'].freeze
  ENRICH_ALIASES = %w(none default all)

  config :enrich, :validate => (ENRICH_ALL | ENRICH_ALIASES), :list => true

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

      if original_params.key?('cipher_suites') && original_params.key?('ssl_cipher_suites')
        raise LogStash::ConfigurationError, "Both `ssl_cipher_suites` and (deprecated) `cipher_suites` were set. Use only `ssl_cipher_suites`."
      elsif original_params.key?('cipher_suites')
        @ssl_cipher_suites_final = @cipher_suites
      else
        @ssl_cipher_suites_final = @ssl_cipher_suites
      end

      if original_params.key?('tls_min_version') && original_params.key?('ssl_supported_protocols')
        raise LogStash::ConfigurationError, "Both `ssl_supported_protocols` and (deprecated) `tls_min_ciphers` were set. Use only `ssl_supported_protocols`."
      elsif original_params.key?('tls_max_version') && original_params.key?('ssl_supported_protocols')
        raise LogStash::ConfigurationError, "Both `ssl_supported_protocols` and (deprecated) `tls_max_ciphers` were set. Use only `ssl_supported_protocols`."
      else
        if original_params.key?('tls_min_version') || original_params.key?('tls_max_version')
          @ssl_supported_protocols_final = TLS.get_supported(tls_min_version..tls_max_version).map(&:name)
        else
          @ssl_supported_protocols_final = @ssl_supported_protocols
        end
      end
    else
      @logger.warn("configured ssl_certificate => #{@ssl_certificate.inspect} will not be used") if @ssl_certificate
      @logger.warn("configured ssl_key => #{@ssl_key.inspect} will not be used") if @ssl_key
    end

    @enrich = resolve_enriches

    @include_codec_tag = original_params.include?('include_codec_tag') ? params['include_codec_tag'] : include_codec_metadata?
    @ssl_peer_metadata = original_params.include?('ssl_peer_metadata') ? params['ssl_peer_metadata'] : @enrich.include?('ssl_peer_metadata')

    # intentionally ask users to provide codec when they want to use the codec metadata
    # second layer enrich is also a controller, provide enrich => ['codec_metadata' or 'default'] with codec if you override event original
    if !original_params.include?('codec') || (@enrich && (!@enrich.include?('codec_metadata') || @enrich.include?('none')))
      @codec = plugin_factory.codec('plain').new('ecs_compatibility' => 'disabled')
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
          ssl_context_builder.setVerifyMode(SslContextBuilder::SslClientVerifyMode::FORCE_PEER)
        elsif @ssl_verify_mode == "peer"
          ssl_context_builder.setVerifyMode(SslContextBuilder::SslClientVerifyMode::VERIFY_PEER)
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

  def include_source_metadata?
    return @enrich.include?('source_metadata')
  end

  def include_codec_metadata?
    return @enrich.include?('codec_metadata')
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
          .setProtocols(@ssl_supported_protocols_final)
          .setCipherSuites(normalized_cipher_suites)
    rescue java.lang.IllegalArgumentException => e
      @logger.error("SSL configuration invalid", error_details(e))
      raise LogStash::ConfigurationError, e
    end
  end

  def normalized_cipher_suites
    @ssl_cipher_suites_final.map(&:upcase)
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

  def resolve_enriches
    deprecated_flags_provided = %w(ssl_peer_metadata include_codec_tag) & original_params.keys
    if deprecated_flags_provided.any? && original_params.include?('enrich')
      raise LogStash::ConfigurationError, "both `enrich` and (deprecated) #{deprecated_flags_provided.join(',')} were provided; use only `enrich`"
    end

    aliases_provided = ENRICH_ALIASES & (@enrich || [])
    if aliases_provided.any? && @enrich.size > 1
      raise LogStash::ConfigurationError, "when an alias is provided to `enrich`, it must be the only value given (got: #{@enrich.inspect}, including #{aliases_provided.size > 1 ? 'aliases' : 'alias'} #{aliases_provided.join(',')})"
    end

    return ENRICH_ALL if aliases_provided.include?('all')
    return ENRICH_NONE if aliases_provided.include?('none')
    return ENRICH_DEFAULT if aliases_provided.include?('default') || !original_params.include?('enrich')

    return @enrich
  end
end

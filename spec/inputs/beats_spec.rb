# encoding: utf-8
require_relative "../spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "stud/temporary"
require "logstash/inputs/beats"
require "logstash/codecs/plain"
require "logstash/codecs/json"
require "logstash/codecs/multiline"
require "logstash/event"

describe LogStash::Inputs::Beats do
  let(:connection) { double("connection") }
  let(:certificate) { BeatsInputTest.certificate }
  let(:port) { BeatsInputTest.random_port }
  let(:client_inactivity_timeout) { 400 }
  let(:threads) { 1 + rand(9) }
  let(:queue)  { Queue.new }
  let(:config)   do
    {
        "port" => port,
        "ssl_certificate" => certificate.ssl_cert,
        "ssl_key" => certificate.ssl_key,
        "client_inactivity_timeout" => client_inactivity_timeout,
        "executor_threads" => threads,
        "type" => "example",
        "tags" => "beats"
    }
  end

  subject(:plugin) { LogStash::Inputs::Beats.new(config) }

  context "#register" do
    context "host related configuration" do
      let(:config) { super().merge("host" => host, "port" => port) }
      let(:host) { "192.168.1.20" }
      let(:port) { 9001 }

      it "sends the required options to the server" do
        expect(org.logstash.beats.Server).to receive(:new).with(host, port, client_inactivity_timeout, threads)
        subject.register
      end
    end

    it "raise no exception" do
      plugin = LogStash::Inputs::Beats.new(config)
      expect { plugin.register }.not_to raise_error
    end

    context "with ssl enabled" do

      let(:config) { { "ssl" => true, "port" => port, "ssl_key" => certificate.ssl_key, "ssl_certificate" => certificate.ssl_cert } }

      context "without certificate configuration" do
        let(:config) { { "port" => 0, "ssl" => true, "ssl_key" => certificate.ssl_key, "type" => "example" } }

        it "should fail to register the plugin with ConfigurationError" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "without key configuration" do
        let(:config) { { "port" => 0, "ssl" => true, "ssl_certificate" => certificate.ssl_cert, "type" => "example" } }
        it "should fail to register the plugin with ConfigurationError" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "with invalid key configuration" do
        let(:p12_key) { certificate.p12_key }
        let(:config) { { "port" => 0, "ssl" => true, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => p12_key } }
        it "should fail to register the plugin" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect( plugin.logger ).to receive(:error) do |msg, opts|
            expect( msg ).to match /.*?configuration invalid/
            expect( opts[:message] ).to match /does not contain valid private key/
          end
          expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "with invalid ciphers" do
        let(:config) { super().merge("cipher_suites" => "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA38") }

        it "should raise a configuration error" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect( plugin.logger ).to receive(:error) do |msg, opts|
            expect( msg ).to match /.*?configuration invalid/
            expect( opts[:message] ).to match /TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA38.*? not available/
          end
          expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "verify_mode" do
        context "verify_mode configured to PEER" do
          let(:config) { super().merge("ssl_verify_mode" => "peer") }

          it "raise a ConfigurationError when certificate_authorities is not set" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.to raise_error(LogStash::ConfigurationError, "ssl_certificate_authorities => is a required setting when ssl_verify_mode => 'peer' is configured")
          end

          it "doesn't raise a configuration error when certificate_authorities is set" do
            config.merge!({ "ssl_certificate_authorities" => [certificate.ssl_cert]})
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.not_to raise_error
          end
        end

        context "verify_mode configured to FORCE_PEER" do
          let(:config) { super().merge("ssl_verify_mode" => "force_peer") }

          it "raise a ConfigurationError when certificate_authorities is not set" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.to raise_error(LogStash::ConfigurationError, "ssl_certificate_authorities => is a required setting when ssl_verify_mode => 'force_peer' is configured")
          end

          it "doesn't raise a configuration error when certificate_authorities is set" do
            config.merge!({ "ssl_certificate_authorities" => [certificate.ssl_cert]})
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.not_to raise_error
          end
        end

        context "with ssl_cipher_suites and cipher_suites set" do
          let(:config) do
            super().merge('ssl_cipher_suites' => ['TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384'],
                          'cipher_suites' => ['TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384'])
          end

          it "should raise a configuration error" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect { plugin.register }.to raise_error LogStash::ConfigurationError, /Use only .?ssl_cipher_suites.?/i
          end
        end

        context "with ssl_supported_protocols and tls_min_version set" do
          let(:config) do
            super().merge('ssl_supported_protocols' => ['TLSv1.2'], 'tls_min_version' => 1.2)
          end

          it "should raise a configuration error" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect { plugin.register }.to raise_error LogStash::ConfigurationError, /Use only .?ssl_supported_protocols.?/i
          end
        end

        context "with ssl_supported_protocols and tls_max_version set" do
          let(:config) do
            super().merge('ssl_supported_protocols' => ['TLSv1.2'], 'tls_max_version' => 1.2)
          end

          it "should raise a configuration error" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect { plugin.register }.to raise_error LogStash::ConfigurationError, /Use only .?ssl_supported_protocols.?/i
          end
        end
      end
    end

    context "with ssl disabled" do
      context "and certificate configuration" do
        let(:config) { { "port" => 0, "ssl" => false, "ssl_certificate" => certificate.ssl_cert, "type" => "example", "tags" => "Beats" } }

        it "should not fail" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect {plugin.register}.not_to raise_error
        end
      end

      context "and certificate key configuration" do
        let(:config) {{ "port" => 0, "ssl" => false, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "beats" }}

        it "should not fail" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect {plugin.register}.not_to raise_error
        end
      end

      context "and no certificate or key configured" do
        let(:config) {{ "ssl" => false, "port" => 0, "type" => "example", "tags" => "beats" }}

        it "should work just fine" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect {plugin.register}.not_to raise_error
        end
      end
    end

    context "with multiline codec" do
      let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '^2015',
                                                    "what" => "previous",
                                                    "negate" => true) }
      let(:config) { super().merge({ "codec" => codec }) }

      it "raise a ConfigurationError when multiline codec is set" do
        plugin = LogStash::Inputs::Beats.new(config)
        expect { plugin.register }.to raise_error(LogStash::ConfigurationError, "Multiline codec with beats input is not supported. Please refer to the beats documentation for how to best manage multiline data. See https://www.elastic.co/guide/en/beats/filebeat/current/multiline-examples.html")
      end
    end

    context "enrich configuration" do
      # We define a shared example for each enrichment type that can independently
      # validate whether that enrichment is effectively enabled or disabled.
      #  - "#{enrichment} enabled"
      #  - "#{enrichment} disabled"

      let(:registered_plugin) { plugin.tap(&:register) }

      shared_examples "source_metadata enabled" do
        it "is configured to enrich source metadata" do
          expect(registered_plugin.include_source_metadata).to be true
        end
      end

      shared_examples "source_metadata disabled" do
        it "is configured to NOT enrich source metadata" do
          expect(registered_plugin.include_source_metadata).to be false
        end
      end

      shared_examples "include codec tag" do
        it "is configured to include the codec tag" do
          expect(registered_plugin.include_codec_tag).to be true
        end
      end

      shared_examples "exclude codec tag" do
        it "is configured to NOT include the codec tag" do
          expect(registered_plugin.include_codec_tag).to be false
        end
      end

      shared_examples "default codec configured to avoid metadata" do
        it "configures the default codec to NOT enrich codec metadata" do
          fail("spec setup error: not compatible with explicitly-given codec") if config.include?('codec')
          # note: disabling ECS is an _implementation detail_ of how we prevent
          # the codec from enriching the event with [event][original]
          expect(registered_plugin.codec.original_params).to include('ecs_compatibility' => 'disabled')
        end
      end

      shared_examples "codec is untouched" do
        it "does NOT configure the codec to avoid enriching codec metadata" do
          # note: disabling ECS is an _implementation detail_ of how we prevent
          # the codec from enriching the event with [event][original], so we ensure
          # the absence of the setting.
          expect(registered_plugin.codec.original_params).to_not include('ecs_compatibility')
        end
      end

      shared_examples "codec_metadata enabled" do
        include_examples "include codec tag"
        include_examples "codec is untouched"
      end

      shared_examples "codec_metadata disabled" do
        include_examples "exclude codec tag"
        include_examples "default codec configured to avoid metadata"

        context "with an explicitly-provided codec" do
          let(:config) { super().merge("codec" => "plain") }

          include_examples "exclude codec tag"
          include_examples "codec is untouched"
        end
      end

      shared_examples "ssl_peer_metadata enabled" do
        it "is configured to enrich ssl_peer_metadata" do
          expect(registered_plugin.ssl_peer_metadata).to be_truthy
        end
      end

      shared_examples "ssl_peer_metadata disabled" do
        it "is configured to NOT enrich ssl_peer_metadata" do
          expect(registered_plugin.ssl_peer_metadata).to be_falsey
        end
      end

      shared_examples "reject deprecated enrichment flags" do
        context "with deprecated `ssl_peer_metadata`" do
          let(:config) { super().merge("ssl_peer_metadata" => true) }
          it 'rejects the configuration with a helpful error message' do
            expect { plugin.register }.to raise_exception(LogStash::ConfigurationError, "both `enrich` and (deprecated) ssl_peer_metadata were provided; use only `enrich`")
          end
        end
        context "with deprecated `include_codec_tag`" do
          let(:config) { super().merge("include_codec_tag" => false) }
          it 'rejects the configuration with a helpful error message' do
            expect { plugin.register }.to raise_exception(LogStash::ConfigurationError, "both `enrich` and (deprecated) include_codec_tag were provided; use only `enrich`")
          end
        end
      end

      context "when `enrich` is NOT provided" do
        # validate defaults
        include_examples "codec_metadata enabled"
        include_examples "source_metadata enabled"
        include_examples "ssl_peer_metadata disabled"

        # validate interaction with deprecated settings
        context "with deprecated `ssl_peer_metadata => true`" do
          let(:config) { super().merge("ssl_peer_metadata" => true) }

          # intended delta
          include_examples "ssl_peer_metadata enabled"

          # ensure no side-effects
          include_examples "codec_metadata enabled"
          include_examples "source_metadata enabled"
        end

        context "with deprecated `include_codec_tag => false`" do
          let(:config) { super().merge("include_codec_tag" => false) }

          # intended delta
          include_examples "exclude codec tag"
          include_examples "codec is untouched"

          # ensure no side-effects
          include_examples "source_metadata enabled"
          include_examples "ssl_peer_metadata disabled"
        end
      end

      # validate aliases
      context "alias resolution" do
        context "with alias `enrich => all`" do
          let(:config) { super().merge("enrich" => "all") }

          include_examples "codec_metadata enabled"
          include_examples "source_metadata enabled"
          include_examples "ssl_peer_metadata enabled"

          include_examples "reject deprecated enrichment flags"
        end

        context "with alias `enrich => none`" do
          let(:config) { super().merge("enrich" => "none") }

          include_examples "codec_metadata disabled"
          include_examples "source_metadata disabled"
          include_examples "ssl_peer_metadata disabled"

          include_examples "reject deprecated enrichment flags"
        end
      end

      available_enrichments = %w(
        codec_metadata
        source_metadata
        ssl_peer_metadata
      )
      shared_examples "enrich activations" do |enrich_arg|
        activated = Array(enrich_arg)
        context "with `enrich => #{enrich_arg}`" do
          let(:config) { super().merge("enrich" => enrich_arg) }

          available_enrichments.each do |enrichment|
            include_examples "#{enrichment} #{activated.include?(enrichment) ? 'enabled' : 'disabled'}"
          end

          include_examples "reject deprecated enrichment flags"
        end
      end

      # ensure explicit empty-list does not activate defaults
      include_examples "enrich activations", []

      # ensure single enrichment does not activate others
      available_enrichments.each do |single_active_enrichment|
        include_examples "enrich activations", single_active_enrichment   # single
        include_examples "enrich activations", [single_active_enrichment] # list-of-one
      end

      # ensure any combination of two enrichment categories activates only those two
      available_enrichments.combination(2) do |active_enrichments|
        include_examples "enrich activations", active_enrichments
      end
    end
  end

  context "tls meta-data" do
    let(:config) do
      super().merge(
          "host" => host,
          "ssl_peer_metadata" => true,
          "ssl_certificate_authorities" => [ certificate.ssl_cert ],
          "ecs_compatibility" => 'disabled'
      )
    end
    let(:host) { "192.168.1.20" }
    let(:port) { 9002 }

    let(:queue) { Queue.new }
    let(:event) { LogStash::Event.new }

    subject(:plugin) { LogStash::Inputs::Beats.new(config) }

    before do
      @server = org.logstash.beats.Server.new(host, port, client_inactivity_timeout, threads)
      expect( org.logstash.beats.Server ).to receive(:new).with(host, port, client_inactivity_timeout, threads).and_return @server
      expect( @server ).to receive(:listen)

      subject.register
      subject.run(queue) # listen does nothing
      @message_listener = @server.getMessageListener

      allow( ssl_engine = double('ssl_engine') ).to receive(:getSession).and_return ssl_session
      allow( ssl_handler = double('ssl-handler') ).to receive(:engine).and_return ssl_engine
      allow( pipeline = double('pipeline') ).to receive(:get).and_return ssl_handler
      allow( @channel = double('channel') ).to receive(:pipeline).and_return pipeline
    end

    let(:ctx) do
      Java::io.netty.channel.ChannelHandlerContext.impl do |method, *args|
        fail("unexpected #{method}( #{args} )") unless method.eql?(:channel)
        @channel
      end
    end

    let(:ssl_session) do
      Java::javax.net.ssl.SSLSession.impl do |method, *args|
        case method
        when :getPeerCertificates
          [].to_java(java.security.cert.Certificate)
        when :getProtocol
          'TLS-Mock'
        when :getCipherSuite
          'SSL_NULL_WITH_TEST_SPEC'
        when :getPeerPrincipal
          javax.security.auth.x500.X500Principal.new('CN=TEST, OU=RSpec, O=Logstash, C=NL', {})
        else
          fail("unexpected #{method}( #{args} )")
        end
      end
    end

    let(:ssl_session_peer_principal) do
      javax.security.auth.x500.X500Principal
    end

    let(:message) do
      org.logstash.beats.Message.new(0, java.util.HashMap.new('foo' => 'bar'))
    end

    it 'sets tls fields' do
      @message_listener.onNewMessage(ctx, message)

      expect( queue.size ).to be 1
      expect( event = queue.pop ).to be_a LogStash::Event

      expect( event.get('[@metadata][tls_peer][status]') ).to eql 'verified'

      expect( event.get('[@metadata][tls_peer][protocol]') ).to eql 'TLS-Mock'
      expect( event.get('[@metadata][tls_peer][cipher_suite]') ).to eql 'SSL_NULL_WITH_TEST_SPEC'
      expect( event.get('[@metadata][tls_peer][subject]') ).to eql 'CN=TEST,OU=RSpec,O=Logstash,C=NL'
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end

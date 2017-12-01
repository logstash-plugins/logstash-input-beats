# encoding: utf-8
require_relative "../spec_helper"
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
  let(:queue)  { Queue.new }
  let(:config)   { { "port" => 0, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "beats"} }

  context "#register" do
    context "host related configuration" do
      let(:config) { super.merge!({ "host" => host, "port" => port, "client_inactivity_timeout" => client_inactivity_timeout, "executor_threads" => threads }) }
      let(:host) { "192.168.1.20" }
      let(:port) { 9000 }
      let(:client_inactivity_timeout) { 400 }
      let(:threads) { 10 }

      subject(:plugin) { LogStash::Inputs::Beats.new(config) }

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
      context "without certificate configuration" do
        let(:config) {{ "port" => 0, "ssl" => true, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "beats" }}

        it "should fail to register the plugin with ConfigurationError" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect {plugin.register}.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "without key configuration" do
        let(:config)   { { "port" => 0, "ssl" => true, "ssl_certificate" => certificate.ssl_cert, "type" => "example", "tags" => "Beats"} }
        it "should fail to register the plugin with ConfigurationError" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect {plugin.register}.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "with invalid ciphers" do
        let(:config)   { { "port" => 0, "ssl" => true, "ssl_certificate" => certificate.ssl_cert, "type" => "example", "tags" => "Beats", "cipher_suites" => "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA38"} }

        it "should raise a configuration error" do
          plugin = LogStash::Inputs::Beats.new(config)
          expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "verify_mode" do
        context "verify_mode configured to PEER" do
          let(:config)   { { "port" => 0, "ssl" => true, "ssl_verify_mode" => "peer", "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "Beats"} }

          it "raise a ConfigurationError when certificate_authorities is not set" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.to raise_error(LogStash::ConfigurationError, "Using `verify_mode` set to PEER or FORCE_PEER, requires the configuration of `certificate_authorities`")
          end

          it "doesn't raise a configuration error when certificate_authorities is set" do
            config.merge!({ "ssl_certificate_authorities" => [certificate.ssl_cert]})
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.not_to raise_error
          end
        end

        context "verify_mode configured to FORCE_PEER" do
          let(:config)   { { "port" => 0, "ssl" => true, "ssl_verify_mode" => "force_peer", "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "Beats"} }

          it "raise a ConfigurationError when certificate_authorities is not set" do
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.to raise_error(LogStash::ConfigurationError, "Using `verify_mode` set to PEER or FORCE_PEER, requires the configuration of `certificate_authorities`")
          end

          it "doesn't raise a configuration error when certificate_authorities is set" do
            config.merge!({ "ssl_certificate_authorities" => [certificate.ssl_cert]})
            plugin = LogStash::Inputs::Beats.new(config)
            expect {plugin.register}.not_to raise_error
          end
        end
      end
    end

    context "with ssl disabled" do
      context "and certificate configuration" do
        let(:config)   { { "port" => 0, "ssl" => false, "ssl_certificate" => certificate.ssl_cert, "type" => "example", "tags" => "Beats" } }

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
      let(:config) { super.merge({ "codec" => codec }) }

      it "raise a ConfigurationError when multiline codec is set" do
        plugin = LogStash::Inputs::Beats.new(config)
        expect {plugin.register}.to raise_error(LogStash::ConfigurationError, "Multiline codec with beats input is not supported. Please refer to the beats documentation for how to best manage multiline data. See https://www.elastic.co/guide/en/beats/filebeat/current/multiline-examples.html")
      end
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end

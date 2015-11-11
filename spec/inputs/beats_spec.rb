# encoding: utf-8
require_relative "../spec_helper"
require "stud/temporary"
require "logstash/inputs/beats"
require "logstash/codecs/plain"
require "logstash/codecs/multiline"
require "logstash/event"
require "lumberjack/beats/client"

describe LogStash::Inputs::Beats do
  let(:connection) { double("connection") }
  let(:certificate) { LogStashTest.certificate }
  let(:port) { LogStashTest.random_port }
  let(:queue)  { Queue.new }
  let(:config)   { { "port" => 0, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "beats"} }

  context "#register" do
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
  end

  describe "#processing of events" do
    subject(:beats) { LogStash::Inputs::Beats.new(config) }
    let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '\n', "what" => "previous") }

    let(:config) do
      { "port" => port, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key,
        "type" => "example", "codec" => codec }
    end

    before do
      beats.register
    end

    context "#create_event" do
      let(:config) { super.merge({ "add_field" => { "foo" => "bar", "[@metadata][hidden]" => "secret"}, "tags" => ["bonjour"]}) }
      let(:event_map) { { "hello" => "world" } }
      let(:codec) { LogStash::Codecs::Plain.new }
      let(:identity_stream) { "custom-type-input_type-source" }

      context "without a `target_field` defined" do
        it "decorates the event" do
          event = beats.create_event(event_map, identity_stream)
          expect(event["foo"]).to eq("bar")
          expect(event["[@metadata][hidden]"]).to eq("secret")
          expect(event["tags"]).to include("bonjour")
        end
      end

      context "with a `target_field` defined" do
        let(:event_map) { super.merge({"message" => "with a field"}) }

        it "decorates the event" do
          event = beats.create_event(event_map, identity_stream)
          expect(event["foo"]).to eq("bar")
          expect(event["[@metadata][hidden]"]).to eq("secret")
          expect(event["tags"]).to include("bonjour")
        end
      end

      context "when data is buffered in the codec" do
        let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '^\s', "what" => "previous") }
        let(:event_map) { {"message" => "hello?", "tags" => ["syslog"]} }

        it "retuns nil" do
          event = beats.create_event(event_map, identity_stream)
          expect(event).to be_nil
        end
      end
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end

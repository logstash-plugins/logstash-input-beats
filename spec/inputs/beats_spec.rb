# encoding: utf-8
require_relative "../spec_helper"
require "stud/temporary"
require "logstash/inputs/beats"
require "logstash/codecs/plain"
require "logstash/codecs/json"
require "logstash/codecs/multiline"
require "logstash/event"
require "lumberjack/beats/client"

describe LogStash::Inputs::Beats do
  let(:connection) { double("connection") }
  let(:certificate) { BeatsInputTest.certificate }
  let(:port) { BeatsInputTest.random_port }
  let(:queue)  { Queue.new }
  let(:config)   { { "port" => 0, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "beats"} }

  context "#register" do
    context "identity map" do
      subject(:plugin) { LogStash::Inputs::Beats.new(config) }
      before { plugin.register }

      context "when using the multiline codec" do
        let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '^2015',
                                                      "what" => "previous",
                                                      "negate" => true) }
        let(:config) { super.merge({ "codec" => codec }) }

        it "wraps the codec with the identity_map" do
          expect(plugin.codec).to be_kind_of(LogStash::Codecs::IdentityMapCodec)
        end
      end

      context "when using non buffered codecs" do
        let(:config) { super.merge({ "codec" => "json" }) }

        it "doesnt wrap the codec with the identity map" do
          expect(plugin.codec).to be_kind_of(LogStash::Codecs::JSON)
        end
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

  context "#handle_new_connection" do
    let(:config) {{ "ssl" => false, "port" => 0, "type" => "example", "tags" => "beats" }}
    let(:plugin) { LogStash::Inputs::Beats.new(config) }
    let(:connection) { BeatsInputTest::DummyConnection.new(events) }
    let(:buffer_queue) { BeatsInputTest::DummyNeverBlockedQueue.new }
    let(:pipeline_queue)  { [] }
    let(:events) {
      [
        { :map => { "id" => 1 }, :identity_stream => "/var/log/message" },
        { :map => { "id" => 2 }, :identity_stream => "/var/log/message_2" }
      ]
    }

    before :each do
      plugin.register

      # Event if we dont mock the actual socket work
      # we have to call run because it will correctly setup the queues
      # instances variables
      @t = Thread.new do
        plugin.run(pipeline_queue)
      end

      # Give a bit of time to the server to correctly
      # start the thread.
      sleep(1)
    end

    after :each do
      plugin.stop
      # I am forcing a kill on the thread since we are using a mock connection and we have not implemented
      # all the  blocking IO, joining the thread and waiting wont work..
      # I want to remove all the blockings in a new version real soon.
      # If we dont do this the thread can still be present in other tests causing this kind of issue:
      # https://github.com/logstash-plugins/logstash-input-beats/issues/48
      @t.kill rescue nil
    end

    context "when an exception occur" do
      let(:connection_handler) { LogStash::Inputs::BeatsSupport::ConnectionHandler.new(connection, plugin, buffer_queue) }
      before do
        expect(LogStash::Inputs::BeatsSupport::ConnectionHandler).to receive(:new).with(any_args).and_return(connection_handler)
      end

      it "calls flush on the handler and tag the events" do
        # try multiples times to make sure the
        # thread containing the `#run` is correctly started.
        try do
          expect(connection_handler).to receive(:accept) { raise LogStash::Inputs::Beats::InsertingToQueueTakeTooLong }
          expect(connection_handler).to receive(:flush).and_yield(LogStash::Event.new)
          plugin.handle_new_connection(connection)

          event = pipeline_queue.shift
          expect(event.get("tags")).to include("beats_input_flushed_by_end_of_connection")
        end
      end
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end

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

  context "#handle_new_connection" do
    let(:config) {{ "ssl" => false, "port" => 0, "type" => "example", "tags" => "beats" }}
    let(:plugin) { LogStash::Inputs::Beats.new(config) }
    let(:connection) { DummyConnection.new(events) }
    let(:buffer_queue) { DummyNeverBlockedQueue.new }
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
      t = Thread.new do
        plugin.run(pipeline_queue)
      end

      # Give a bit of time to the server to correctly 
      # start the thread.
      sleep(1) 
    end

    after :each do
      plugin.stop
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
          expect(event["tags"]).to include("beats_input_flushed_by_end_of_connection")
        end
      end
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end

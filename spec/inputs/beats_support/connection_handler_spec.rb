# encoding: utf-8
require "logstash/inputs/beats"
require_relative "../../support/logstash_test"
require "spec_helper"

describe LogStash::Inputs::BeatsSupport::ConnectionHandler do
  let(:config) do
    {
      "port" => 0,
      "type" => "example",
      "tags" => "beats"
    }
  end
  # logger is not used and DummyLogger needs implementing
  let(:logger) { DummyLogger.new  }
  let(:input) do
    LogStash::Inputs::Beats.new(config).tap do |i|
      i.register
    end
  end
  let(:connection) { double("connection") }
  let(:queue) { BeatsInputTest::DummyNeverBlockedQueue.new }

  subject { described_class.new(connection, input, queue) }

  context "#accept" do
    let(:connection) { BeatsInputTest::DummyConnection.new(events) }
    let(:events) {
      [
        { :map => { "id" => 1 }, :identity_stream => "/var/log/message" },
        { :map => { "id" => 2 }, :identity_stream => "/var/log/message_2" }
      ]
    }
    it "should delegate work to `#process` for each items" do
      events.each do |element|
        expect(subject).to receive(:process).with(element[:map], element[:identity_stream]).and_return(true)
      end

      subject.accept
    end
  end

  context "#process" do
    let(:identity_stream) { "/var/log/message" }

    context "without a `target_field_for_codec`" do
      let(:map) { { "hello" => "world" } }

      context "queue is not blocked" do
        it "insert the event into the queue" do
          subject.process(map, identity_stream)
          event = queue.take

          expect(event.get("hello")).to eq(map["hello"])
          expect(event.get("tags")).to include("beats_input_raw_event")
        end
      end

      context "queue is blocked" do
        let(:queue_timeout) { 1 }
        let(:queue) { LogStash::Inputs::BeatsSupport::SynchronousQueueWithOffer.new(queue_timeout) }

        it "raise an exception" do
          expect { subject.process(map, identity_stream) }.to raise_error(LogStash::Inputs::Beats::InsertingToQueueTakeTooLong)
        end
      end
    end

    context "with a codec" do
      let(:message) { "Hello world" }
      let(:map) { { "message" => message } }

      context "queue is not blocked" do
        it "insert the event into the queue" do
          subject.process(map, identity_stream)
          event = queue.take

          expect(event.get("message")).to eq(message)
          expect(event.get("tags")).to include("beats_input_codec_plain_applied")
        end
      end

      context "queue is blocked" do
        let(:queue_timeout) { 1 }
        let(:queue) { LogStash::Inputs::BeatsSupport::SynchronousQueueWithOffer.new(queue_timeout) }

        it "raise an exception" do
          expect { subject.process(map, identity_stream) }.to raise_error(LogStash::Inputs::Beats::InsertingToQueueTakeTooLong)
        end
      end
    end
  end
end

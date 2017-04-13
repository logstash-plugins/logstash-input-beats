# encoding: utf-8
require "logstash-core"
require "logstash/inputs/beats"
require "logstash/event"
require "logstash/inputs/beats/message_listener"
require "logstash/instrument/namespaced_null_metric"
require "thread"

class MockMessage
  def initialize(identity_stream, data = {})
    @identity_stream = identity_stream
    @data = data
  end

  def getData
    @data
  end

  def getIdentityStream
    @identity_stream
  end
end

class DummyCodec < LogStash::Codecs::Base
  DUMMY_EVENT = LogStash::Event.new

  def decode(message, &block)
    block.call(LogStash::Event.new({"message" => message}))
  end

  def flush(&block)
    block.call(DUMMY_EVENT)
  end
end

describe LogStash::Inputs::Beats::MessageListener do
  let(:queue)  { Queue.new }
  let(:codec) { DummyCodec.new }
  let(:input) { LogStash::Inputs::Beats.new({ "port" => 5555, "codec" => codec }) }
  let(:ctx) { double("ChannelHandlerContext") }
  let(:message) { MockMessage.new("abc", { "message" => "hello world"}) }

  subject { described_class.new(queue, input) }

  before do
    subject.onNewConnection(ctx)
  end

  context "onNewConnection" do
    it "register the connection to the connection list" do
      expect { subject.onNewConnection(double("ChannelHandlerContext")) }.to change { subject.connections_list.count }.by(1)
    end

    describe "metrics" do
      it "new connection should increment connection count" do
        expect(subject).to receive(:increment_connection_count).once
        subject.onNewConnection(double("ChannelHandlerContext"))
      end

      describe "peak connections" do
        it "closing and open connections should keep highest count" do
          expect(subject.instance_eval("@peak_connection_count").value).to eq(1)
          subject.onNewConnection(1)
          expect(subject.instance_eval("@peak_connection_count").value).to eq(2)
          subject.onNewConnection(2)
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onConnectionClose(2)
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onNewConnection(3)
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onNewConnection(4)
          expect(subject.instance_eval("@peak_connection_count").value).to eq(4)
        end
      end

    end
  end

  context "onNewMessage" do
    context "when the message is from filebeat" do
      let(:message) { MockMessage.new("abc", { "message" => "hello world" } )}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to eq("hello world")
      end
    end

    context "when the message is from LSF" do
      let(:message) { MockMessage.new("abc", { "line" => "hello world" } )}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to eq("hello world")
      end
    end

    context "when the message is from any libbeat" do
      let(:message) { MockMessage.new("abc",  { "metric" => 1, "name" => "super-stats"} )}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to be_nil
        expect(event.get("metric")).to eq(1)
        expect(event.get("name")).to eq("super-stats")
      end
    end
  end

  context "onException" do
    it "remove the connection to the connection list" do
      expect { subject.onException(ctx, double("Exception")) }.to change { subject.connections_list.count }.by(-1)
    end

    it "calls flush on codec" do
      subject.onConnectionClose(ctx)
      expect(queue).not_to be_empty
    end
  end

  context "onConnectionClose" do
    it "remove the connection to the connection list" do
      expect { subject.onConnectionClose(ctx) }.to change { subject.connections_list.count }.by(-1)
    end

    it "calls flush on codec" do
      subject.onConnectionClose(ctx)
      expect(queue).not_to be_empty
    end
  end
end

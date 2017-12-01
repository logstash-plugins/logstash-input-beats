# encoding: utf-8
require "logstash-core"
require "logstash/inputs/beats"
require "logstash/event"
require "logstash/inputs/beats/message_listener"
require "logstash/instrument/namespaced_null_metric"
require "thread"

java_import java.util.HashMap

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

# General purpose single method mock class. Will keep generating mocks until requested method name (no args) is found.
class OngoingMethodMock
  def initialize(method_name, return_value)
    @method_name = method_name
    @return_value = return_value
  end

  def method_missing(method)
    if(method.to_s.eql? @method_name)
      return @return_value
    else
      return OngoingMethodMock.new(@method_name, @return_value)
    end
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

  let(:ip_address) { "10.0.0.1" }
  let(:remote_address) { OngoingMethodMock.new("getHostAddress", ip_address) }
  let(:ctx) {OngoingMethodMock.new("remoteAddress", remote_address)}

  let(:message) { MockMessage.new("abc", { "message" => "hello world"}) }

  subject { described_class.new(queue, input) }

  before do
    subject.onNewConnection(ctx)
  end

  context "onNewConnection" do
    let(:second_ip_address) { "10.0.0.2" }
    let(:second_ctx) {OngoingMethodMock.new("getHostAddress", second_ip_address)}

    shared_examples 'a valid new connection' do
      it "register the connection to the connection list" do
        expect { subject.onNewConnection(second_ctx) }.to change { subject.connections_list.count }.by(1)
      end
    end

    it_behaves_like 'a valid new connection'

    context 'with a nil remote address' do
      let(:second_ip_address) { nil}

      it_behaves_like 'a valid new connection'
    end

    context 'when the channel throws retrieving remote address' do
      before do
        allow(second_ctx).to receive(:channel).and_raise('nope')
      end

      it_behaves_like 'a valid new connection'
    end

    describe "metrics" do
      it "new connection should increment connection count" do
        expect(subject).to receive(:increment_connection_count).once
        subject.onNewConnection(second_ctx)
      end

      describe "peak connections" do
        let (:ctxes) { [1, 2, 3, 4].inject([]) do |result, element|
          result << OngoingMethodMock.new("getHostAddress", "10.0.0.#{element}")
          result
        end
        }
        it "closing and open connections should keep highest count" do
          expect(subject.instance_eval("@peak_connection_count").value).to eq(1)
          subject.onNewConnection(ctxes[0])
          expect(subject.instance_eval("@peak_connection_count").value).to eq(2)
          subject.onNewConnection(ctxes[1])
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onConnectionClose(ctxes[1])
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onNewConnection(ctxes[2])
          expect(subject.instance_eval("@peak_connection_count").value).to eq(3)
          subject.onNewConnection(ctxes[3])
          expect(subject.instance_eval("@peak_connection_count").value).to eq(4)
        end
      end

    end
  end

  context "onNewMessage" do
    context "when the message is from filebeat" do
      let(:message) { MockMessage.new("abc", { "message" => "hello world", "@metadata" => {} } )}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to eq("hello world")
      end
    end

    context "when the message is from LSF" do
      let(:message) { MockMessage.new("abc", { "line" => "hello world", '@metadata' => {} } )}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to eq("hello world")
      end
    end

    context "when the message is from any libbeat" do
      #Requires data modeled as Java, not Ruby since the actual code pulls from Java backed (Netty) object
      let(:data) do
        d = HashMap.new
        d.put('@metadata', HashMap.new)
        d.put('metric',  1)
        d.put('name', "super-stats")
        d
      end

      let(:message) { MockMessage.new("abc",  data)}

      it "extract the event" do
        subject.onNewMessage(ctx, message)
        event = queue.pop
        expect(event.get("message")).to be_nil
        expect(event.get("metric")).to eq(1)
        expect(event.get("name")).to eq("super-stats")
        expect(event.get("[@metadata][ip_address]")).to eq(ip_address)
      end

      context 'when the remote address is nil' do
        let(:ctx) { OngoingMethodMock.new("remoteAddress", nil)}

        it 'extracts the event' do
          subject.onNewMessage(ctx, message)
          event = queue.pop
          expect(event.get("message")).to be_nil
          expect(event.get("metric")).to eq(1)
          expect(event.get("name")).to eq("super-stats")
          expect(event.get("[@metadata][ip_address]")).to eq(nil)
        end
      end

      context 'when getting the remote address raises' do
        let(:raising_ctx) { double("context")}

        before  do
          allow(raising_ctx).to receive(:channel).and_raise("nope")
          subject.onNewConnection(raising_ctx)
        end

        it 'extracts the event' do
          subject.onNewMessage(raising_ctx, message)
          event = queue.pop
          expect(event.get("message")).to be_nil
          expect(event.get("metric")).to eq(1)
          expect(event.get("name")).to eq("super-stats")
          expect(event.get("[@metadata][ip_address]")).to eq(nil)
        end
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

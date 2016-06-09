# encoding: utf-8
require "logstash/inputs/beats/message_listener"
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

class CodecSpy
end

describe LogStash::Inputs::Beats::MessageListener do
  let(:queue)  { Queue.new }
  let(:ctx) { double("ChannelHandlerContext") }
  let(:message) { MockMessage.new("abc", { "message" => "hello world"}) }

  subject { described_class.new(queue, input) }

  context "onNewConnection" do
    it "register the connection to the connection list" do
    end
  end
end

# encoding: utf-8
require "logstash/inputs/beats"
require "logstash/event"
require "spec_helper"
require "thread"

Thread.abort_on_exception = true
describe LogStash::Inputs::BeatsSupport::CodecCallbackListener do
  let(:data) { "Hello world" }
  let(:map) do
    {
      "beat" => { "hostname" => "newhost" }
    }
  end
  let(:path) { "/var/log/message" }
  let(:transformer) { double("codec_transformer") }
  let(:queue_timeout) { 1 }
  let(:queue) { LogStash::Inputs::BeatsSupport::SynchronousQueueWithOffer.new(queue_timeout) }
  let(:event) { LogStash::Event.new }

  before do
    allow(transformer).to receive(:transform).with(event, map).and_return(event)
  end

  subject { described_class.new(data, map, path, transformer, queue) }

  it "expose the data" do
    expect(subject.data).to eq(data)
  end

  it "expose the path" do
    expect(subject.path).to eq(path)
  end
  
  context "when the queue is not blocked for too long" do
    let(:queue_timeout) { 5 }

    it "doesnt raise an exception" do
      Thread.new do
        loop { queue.take }
      end
      sleep(0.1)
      expect { subject.process_event(event) }.not_to raise_error
    end
  end

  context "when the queue is blocked for too long" do
    it "raises an exception" do
      expect { subject.process_event(event) }.to raise_error(LogStash::Inputs::Beats::InsertingToQueueTakeTooLong)
    end
  end
end

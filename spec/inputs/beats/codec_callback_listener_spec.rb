# encoding: utf-8
require "spec_helper"
require "logstash/inputs/beats"
require "logstash/event"
require "thread"

describe LogStash::Inputs::Beats::CodecCallbackListener do
  let(:data) { "Hello world" }
  let(:map) do
    {
      "beat" => { "hostname" => "newhost" }
    }
  end
  let(:path) { "/var/log/message" }
  let(:transformer) { double("codec_transformer") }
  let(:queue_timeout) { 1 }
  let(:event) { LogStash::Event.new }
  let(:queue) { Queue.new }

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
end

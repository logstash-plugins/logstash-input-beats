# encoding: utf-8
require "logstash/event"
require "logstash/inputs/beats"
require_relative "../../support/shared_examples"
require "spec_helper"

describe LogStash::Inputs::Beats::RawEventTransform do
  let(:config) do
    {
      "port" => 0,
      "type" => "example",
      "tags" => "beats"
    }
  end

  let(:input) { LogStash::Inputs::Beats.new(config) }
  let(:event) { LogStash::Event.new }

  subject { described_class.new(input).transform(event) }

  include_examples "Common Event Transformation"

  it "tags the event" do
    expect(subject.get("tags")).to include("beats_input_raw_event")
  end
end

# encoding: utf-8
require "logstash/event"
require "logstash/timestamp"
require "logstash/inputs/beats"
require_relative "../../support/shared_examples"
require "spec_helper"

describe LogStash::Inputs::BeatsSupport::DecodedEventTransform do
  let(:config) do
    {
      "port" => 0,
      "type" => "example",
      "tags" => "beats"
    }
  end

  let(:input) do
    LogStash::Inputs::Beats.new(config).tap do |i|
      i.register
    end
  end
  let(:event) { LogStash::Event.new }
  let(:map) do
    {
      "@metadata" =>  { "hello" => "world"},
      "super" => "mario"
    }
  end

  subject { described_class.new(input).transform(event, map) }

  include_examples "Common Event Transformation"

  it "tags the event" do
    expect(subject["tags"]).to include("beats_input_codec_plain_applied")
  end

  it "merges the other data from the map to the event" do
    expect(subject["super"]).to eq(map["super"])
    expect(subject["@metadata"]).to include(map["@metadata"])
  end

  context "map contains a timestamp" do
    context "when its valid" do
      let(:timestamp) { Time.now }
      let(:map) { super.merge({"@timestamp" => timestamp }) }
     
      it "uses as the event timestamp" do
        expect(subject["@timestamp"]).to eq(LogStash::Timestamp.coerce(timestamp)) 
      end
    end

    context "when its not valid" do
      let(:map) { super.merge({"@timestamp" => "invalid" }) }

      it "fallback the current time" do
        expect(subject["@timestamp"]).to be_kind_of(LogStash::Timestamp)
      end
    end
  end

  context "when the map doesn't provide a timestamp" do
    it "fallback the current time" do
      expect(subject["@timestamp"]).to be_kind_of(LogStash::Timestamp)
    end
  end
end

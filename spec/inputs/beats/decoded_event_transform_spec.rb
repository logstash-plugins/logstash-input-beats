# encoding: utf-8
require "logstash/event"
require "logstash/timestamp"
require "logstash/inputs/beats"
require_relative "../../support/shared_examples"
require "spec_helper"

describe LogStash::Inputs::Beats::DecodedEventTransform do
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
    expect(subject.get("tags")).to include("beats_input_codec_plain_applied")
  end

  it "merges the other data from the map to the event" do
    expect(subject.get("super")).to eq(map["super"])
    expect(subject.get("@metadata")).to include(map["@metadata"])
  end

  context "map contains a timestamp" do
    context "when its valid" do
      let(:timestamp) { Time.now }
      let(:map) { super.merge({"@timestamp" => timestamp }) }
     
      it "uses as the event timestamp" do
        expect(subject.get("@timestamp")).to eq(LogStash::Timestamp.coerce(timestamp)) 
      end
    end

    context "when its not valid" do
      let(:map) { super.merge({"@timestamp" => "invalid" }) }

      it "fallback the current time" do
        expect(subject.get("@timestamp")).to be_kind_of(LogStash::Timestamp)
      end
    end
  end

  context "when the map doesn't provide a timestamp" do
    it "fallback the current time" do
      expect(subject.get("@timestamp")).to be_kind_of(LogStash::Timestamp)
    end
  end

  context "when the codec is a base_codec wrapper" do
    before { config.update("codec" => BeatsInputTest::DummyCodec.new) }
    it "gets the codec config name from the base codec" do
      expect(subject.get("tags")).to include("beats_input_codec_dummy_applied")
    end
  end

  context "when configured to not include codec tag" do
    before { config.update("include_codec_tag" => false) }
    it "does not tag the event" do
      expect(subject.get("tags")).not_to include("beats_input_codec_plain_applied")
    end
  end
end

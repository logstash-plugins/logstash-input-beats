# encoding: utf-8

require_relative "../spec_helper"
require 'logstash/inputs/lumberjack'
require "logstash/inputs/base"
require "logstash/codecs/plain"

describe LogStash::Inputs::Lumberjack::ConnectionDecorator do
  let(:decoration) { LogStash::Inputs::Lumberjack::EventDecoration.new }
  let(:codec) { LogStash::Codecs::Plain.new }
  let(:events) { Array.new }
  let(:receiver) { Proc.new { |event| events << event.to_hash } }
  let(:connection) {double("connection")}
  let(:client) {
    LogStash::Inputs::Lumberjack::ConnectionDecorator.new(decoration, codec.clone, connection)
  }

  context "Receiving data frame" do
    it "should accept event with line field" do
      allow(connection).to receive(:run).and_yield(:data, {
        "line" => "foobar line",
        "extra" => "extra value"
      })
      client.run(&receiver)

      event = events.shift
      expect(event["message"]).to eq("foobar line")
      expect(event["extra"]).to eq("extra value")
    end

    it "should not accept event without line field" do
      allow(connection).to receive(:run).and_yield(:data, {
        "extra" => "extra value"
      })
      expect {client.run(&receiver) }.to raise_error
    end
  end

  context "Receiving json data frame" do
    it "should accept and decode event with line field (backwards compatibility)" do
      allow(connection).to receive(:run).and_yield(:json, {
        "line" => "foobar line",
        "extra" => "extra value"
      })
      client.run(&receiver)

      event = events.shift
      expect(event["message"]).to eq("foobar line")
      expect(event["extra"]).to eq("extra value")
    end

    it "should accept event without line field" do
      allow(connection).to receive(:run).and_yield(:json, {
        "extra" => "extra value"
      })

      client.run(&receiver)

      event = events.shift
      expect(event["extra"]).to eq("extra value")
    end

    it "should accept and split json array into multiple events" do
      allow(connection).to receive(:run).and_yield(:json, [
        {"line" => "event0"},
        {"line" => "event1"},
        {"line" => "event2"},
      ])
      client.run(&receiver)

      expect(events.length).to eq(3)
      expect(events[0]["message"]).to eq("event0")
      expect(events[1]["message"]).to eq("event1")
      expect(events[2]["message"]).to eq("event2")
    end
  end

end

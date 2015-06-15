# encoding: utf-8
require "spec_helper"
require "stud/temporary"
require 'logstash/inputs/lumberjack'
require "logstash/codecs/plain"
require "logstash/codecs/multiline"
require "logstash/event"
require "lumberjack/client"

describe LogStash::Inputs::Lumberjack do
  let(:connection) { double("connection") }
  let(:certificate) { LogStashTest.certificate }
  let(:port) { LogStashTest.random_port }
  let(:queue)  { Queue.new }
  let(:config)   { { "port" => 0, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key, "type" => "example", "tags" => "lumberjack" } }

  subject(:lumberjack) { LogStash::Inputs::Lumberjack.new(config) }

  context "#register" do
    it "raise no exception" do
      plugin = LogStash::Inputs::Lumberjack.new(config)
      expect { plugin.register }.not_to raise_error
    end
  end

  describe "#processing of events" do
    let(:lines) { {"line" => "one\ntwo\n  two.2\nthree\n", "tags" => ["syslog"]} }

    before do
      allow(connection).to receive(:run).and_yield(lines)
      lumberjack.register
      expect_any_instance_of(Lumberjack::Server).to receive(:accept).and_return(connection)
    end

    context "#codecs" do
      let(:config) do
        { "port" => port, "ssl_certificate" => certificate.ssl_cert, "ssl_key" => certificate.ssl_key,
          "type" => "example", "codec" => codec }
      end

      let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '\n', "what" => "previous") }
      it "clone the codec per connection" do
        expect(lumberjack.codec).to receive(:clone).once
        expect(lumberjack).to receive(:invoke).and_throw(:msg)
        lumberjack.run(queue)
      end
    end
  end
end

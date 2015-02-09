# encoding: utf-8
require "spec_helper"
require "stud/temporary"
require 'logstash/inputs/lumberjack'
require "logstash/codecs/plain"
require "logstash/codecs/multiline"

describe LogStash::Inputs::Lumberjack do

  let(:ssl_cert) { Stud::Temporary.pathname("ssl_certificate") }
  let(:ssl_key)  { Stud::Temporary.pathname("ssl_key") }

  let(:queue)  { Queue.new }
  let(:config)   { { "port" => 0, "ssl_certificate" => ssl_cert, "ssl_key" => ssl_key, "type" => "example" } }


  before do
    system("openssl req -x509  -batch -nodes -newkey rsa:2048 -keyout #{ssl_key} -out #{ssl_cert} -subj /CN=localhost > /dev/null 2>&1")
  end

  context "#register" do

    it "raise no exception" do
      plugin = LogStash::Inputs::Lumberjack.new(config)
      expect { plugin.register}.not_to raise_error
    end
  end

  describe "#processing of events" do

    context "#codecs" do

      let(:config) do
        { "port" => 6969, "ssl_certificate" => ssl_cert, "ssl_key" => ssl_key,
          "type" => "example", "codec" => codec }
      end

      let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '\n', "what" => "previous") }
      subject!(:lumberjack) { LogStash::Inputs::Lumberjack.new(config) }
      let(:connection) { double("connection") }
      let(:lines) { {"line" => "one\ntwo\n  two.2\nthree\n"} }

      before(:each) do
        allow(connection).to receive(:run).and_yield(lines)
        lumberjack.register
      end

      it "clone the codec per connection" do
        expect_any_instance_of(Lumberjack::Server).to receive(:accept).and_return(connection)
        expect(lumberjack.codec).to receive(:clone).once
        expect(lumberjack).to receive(:invoke).and_throw(:msg)
        lumberjack.run(queue)
      end

    end
  end
end

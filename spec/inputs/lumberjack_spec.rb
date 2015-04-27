# encoding: utf-8
require "spec_helper"
require "stud/temporary"
require 'logstash/inputs/lumberjack'
require "logstash/codecs/plain"
require "logstash/codecs/multiline"
require "logstash/event"
require "lumberjack/client"
require_relative "../support/logstash_test"

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

  context "when we have the maximum clients connected" do
    let(:max_clients) { 1 }
    let(:window_size) { 1 }
    let(:config) do 
      {
        "port" => port,
        "ssl_certificate" => certificate.ssl_cert,
        "ssl_key" => certificate.ssl_key,
        "type" => "testing",
        "max_clients" => max_clients
      }
    end

    let(:client_options) do
      {
        :port => port,
        :address => "127.0.0.1",
        :ssl_certificate => certificate.ssl_cert,
        :window_size => window_size
      }
    end

    before do
      lumberjack.register

      @server = Thread.new do
        lumberjack.run(queue)
      end

      sleep(0.1) # wait for the server to correctly accept messages
    end

    after do
      @server.raise(LogStash::ShutdownSignal)
      @server.join
    end

    it "stops accepting new connection" do
      client1 = Lumberjack::Socket.new(client_options)
      client2 = Lumberjack::Socket.new(client_options)

      expect { 
        (window_size + 1).times do
          client2.write_hash({"line" => "message"}) 
        end
      }.to raise_error(IOError)
    end
 end
end

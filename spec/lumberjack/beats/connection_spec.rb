# encoding: utf-8
require "lumberjack/beats/server"
require "spec_helper"
require "flores/random"

describe "Connnection" do
  let(:ip) { "192.168.1.2" }
  let(:port) { Flores::Random.port }
  let(:server) { double("server", :closed? => false) }
  let(:socket) { double("socket", :closed? => false) }
  let(:connection) { Lumberjack::Beats::Connection.new(socket, server) }
  let(:payload) { {"line" => "foobar" } }
  let(:start_sequence) { Flores::Random.integer(0..2000) }
  let(:random_number_of_events) { Flores::Random.integer(2..200) }

  subject { Lumberjack::Beats::Connection.new(socket, server) }

  before do
    allow(socket).to receive(:peeraddr).and_return(["AF_INET", port, "test.elastic.co", ip])
  end

  context "#peer" do
    let(:socket) { double("socket", :closed? => false) }

    it "return the ip and the port" do
      expect(subject.peer).to eq("#{ip}:#{port}")
    end
  end

  context "#identity_stream" do
    let(:map) { { "message" => "Hello world" } }

    context "with a map containing the beats.id and the file_id" do
      let(:map) { super.merge({
        "beat" => { "name" => "testing-host", "id" => "abc1234" },
        "type" => "log",
        "resource_id" => "123",
        "input_type" => "propector",
        "source" => "/var/log/message" }) }

      it "generate a identity stream from an event start" do
        expect(subject.identity_stream(map)).to eq("#{map["beat"]["id"]}-#{map["resource_id"]}")
      end
    end

    context "with a map containing all the information" do
      let(:map) { super.merge({
        "beat" => { "name" => "testing-host" },
        "type" => "log",
        "input_type" => "propector",
        "source" => "/var/log/message" }) }

      it "generate a identity stream from an event start" do
        expect(subject.identity_stream(map)).to eq("#{map["beat"]["name"]}-#{map["source"]}")
      end
    end

    context "with a map containing no information" do
      it "use the ip and the port" do
        expect(subject.identity_stream(map)).to eq("")
      end
    end
  end

  context "when the server is running" do
    before do
      allow(socket).to receive(:sysread).with(Lumberjack::Beats::Connection::READ_SIZE).and_return("")
      allow(socket).to receive(:syswrite).with(anything).and_return(true)
      allow(socket).to receive(:close)
    end

    it "should ack the end of a sequence" do
      expectation = receive(:feed)
        .with("")
        .and_yield(:version, Lumberjack::Beats::Parser::PROTOCOL_VERSION_1)
        .and_yield(:window_size, random_number_of_events)

      random_number_of_events.times { |n| expectation.and_yield(:data, start_sequence + n + 1, payload) }

      expect_any_instance_of(Lumberjack::Beats::Parser).to expectation
      expect(socket).to receive(:syswrite).with(["1A", random_number_of_events + start_sequence].pack("A*N"))
      connection.read_socket
    end

    it "should not ignore any exception raised by `#sysread`" do
      expect(server).to receive(:closed?).and_return(false)
      expect(connection).to receive(:read_socket).and_raise("Something went wrong")
      expect { |b| connection.run(&b) }.to raise_error
    end
  end

  context "when the server stop" do
    let(:server) { double("server", :closed? => true) }

    before do
      expect(socket).to receive(:close).and_return(true)
    end

    it "stop reading from the socket" do
      expect { |b| connection.run(&b) }.not_to yield_control
    end

    it "should ignore any exception raised by `#sysread`" do
      expect(server).to receive(:closed?).and_return(false)
      expect(server).to receive(:closed?).and_return(true)
      expect(connection).to receive(:read_socket).and_raise("Something went wrong")
      expect { |b| connection.run(&b) }.not_to raise_error
    end
  end
end

# encoding: utf-8
require "lumberjack/beats/server"
require "spec_helper"
require "flores/random"

describe "Connnection" do
  let(:server) { double("server", :closed? => false) }
  let(:socket) { double("socket", :closed? => false) }
  let(:connection) { Lumberjack::Beats::Connection.new(socket, server) }
  let(:payload) { {"line" => "foobar" } }
  let(:start_sequence) { Flores::Random.integer(0..2000) }
  let(:random_number_of_events) { Flores::Random.integer(2..200) }

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

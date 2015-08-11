# encoding: utf-8
require "lumberjack/server"
require "spec_helper"
require "flores/random"

describe "Server" do
  let(:socket) { double("socket") }
  let(:connection) { Lumberjack::Connection.new(socket) }
  let(:payload) { {"line" => "foobar" } }
  let(:start_sequence) { Flores::Random.integer(0..2000) }
  let(:random_number_of_events) { Flores::Random.integer(2..200) }

  before do
    expect(socket).to receive(:sysread).at_least(:once).with(Lumberjack::Connection::READ_SIZE).and_return("")
    allow(socket).to receive(:syswrite).with(anything).and_return(true)
    allow(socket).to receive(:close)

    expectation = receive(:feed)
      .with("")
      .and_yield(:window_size, random_number_of_events)

    random_number_of_events.times { |n| expectation.and_yield(:data, start_sequence + n + 1, payload) }

    expect_any_instance_of(Lumberjack::Parser).to expectation
  end

  describe "Connnection" do
    it "should ack the end of a sequence" do
      expect(socket).to receive(:syswrite).with(["1A", random_number_of_events + start_sequence].pack("A*N"))
      connection.read_socket
    end
  end
end

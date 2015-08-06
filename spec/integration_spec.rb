# encoding: utf-8
require "lumberjack/client"
require "lumberjack/server"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"
Thread.abort_on_exception = true

describe "A client" do
  let(:certificate) { Flores::PKI.generate }
  let(:certificate_file_crt) { "certificate.crt" }
  let(:certificate_file_key) { "certificate.key" }
  let(:port) { Flores::Random.integer(1024..65335) }
  let(:host) { "127.0.0.1" }
  let(:queue) { [] }

  before do
    expect(File).to receive(:read).at_least(1).with(certificate_file_crt) { certificate.first.to_s }
    expect(File).to receive(:read).at_least(1).with(certificate_file_key) { certificate.last.to_s }
    
    server = Lumberjack::Server.new(:port => port,
                                    :address => host,
                                    :ssl_certificate => certificate_file_crt,
                                    :ssl_key => certificate_file_key)

    @server = Thread.new do
      server.run { |data| queue << data }
    end
  end

  context "with a valid certificate" do
    it "successfully connect to the server" do
      expect { 
        Lumberjack::Client.new(:port => port, 
                               :host => host,
                               :addresses => host,
                               :ssl_certificate => certificate_file_crt)

      }.not_to raise_error
    end
  end

  context "with an invalid certificate" do
    let(:invalid_certificate) { Flores::PKI.generate }
    let(:invalid_certificate_file) { "invalid.crt" }

    before do
      expect(File).to receive(:read).with(invalid_certificate_file) { invalid_certificate.first.to_s }
    end

    it "should refuse to connect" do
      expect { 
        Lumberjack::Client.new(:port => port, 
                               :host => host,
                               :addresses => host,
                               :ssl_certificate => invalid_certificate_file)

      }.to raise_error(OpenSSL::SSL::SSLError, /certificate verify failed/)
    end
  end

  context "Should receive ack after transmitting a payload" do
    let(:random_number_of_events) { Flores::Random.integer(2..10) }

    let(:payload) { { "line" => "foobar" } }
    let(:client) do
      Lumberjack::Client.new(:port => port, 
                             :host => host,
                             :addresses => host,
                             :ssl_certificate => certificate_file_crt)
    end

    it "on every events" do
      expect_any_instance_of(Lumberjack::Socket).to receive(:ack).at_least(random_number_of_events - 1) # -1, because we ack on next event

      random_number_of_events.times { client.write(payload) }

      # I've send case of the events not yet parsed.
      sleep(0.5)

      expect(queue.size).to eq(random_number_of_events)
    end
  end
end

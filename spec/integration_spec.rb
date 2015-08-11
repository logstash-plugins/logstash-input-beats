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
  
  shared_examples "send payload" do
    it "supports single element" do
      (1..random_number_of_events).each do |n|
        expect(client.write(payload)).to eq(sequence_start + n)
      end
      sleep(0.5) # give time to the server to read the events
      expect(queue.size).to eq(random_number_of_events)
    end

    it "support sending multiple elements in one payload" do
      expect(client.write(batch_payload)).to eq(sequence_start + batch_size)
      sleep(0.5)

      expect(queue.size).to eq(batch_size)
      expect(queue).to match_array(batch_payload)
    end
  end

  context "When transmitting a payload" do
    let(:random_number_of_events) { Flores::Random.integer(2..10) }
    let(:payload) { { "line" => "foobar" } }
    let(:client) do
      Lumberjack::Client.new(:port => port, 
                             :host => host,
                             :addresses => host,
                             :ssl_certificate => certificate_file_crt)
    end
    let(:batch_size) { Flores::Random.integer(1..1024) }
    let(:batch_payload) do
      batch = []
      batch_size.times do |n|
        batch <<  { "line" => "foobar #{n}" }
      end
      batch
    end

    context "when sequence start at 0" do
      let(:sequence_start) { 0 }

      include_examples "send payload"
    end

    context "when sequence doesn't start at zero" do
      let(:sequence_start) { Flores::Random.integer(1..2000) }

      before do
        client.instance_variable_get(:@socket).instance_variable_set(:@sequence, sequence_start)
      end

      include_examples "send payload"
    end

    context "when the sequence rollover" do
      let(:batch_size) { 100 }
      let(:sequence_start) { Lumberjack::SEQUENCE_MAX - batch_size / 2 }

      before do
        client.instance_variable_get(:@socket).instance_variable_set(:@sequence, sequence_start)
      end

      it "adjusts the ack" do
        expect(client.write(batch_payload)).to eq(batch_size / 2)
        sleep(0.5)
        expect(queue.size).to eq(batch_size)
        expect(queue).to match_array(batch_payload)
      end
    end
  end
end

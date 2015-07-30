# encoding: utf-8
require "lib/lumberjack/client"
require "lib/lumberjack/server"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"

describe "A client" do
  let(:certificate) { Flores::PKI.generate }
  let(:certificate_file_crt) { "certificate.crt" }
  let(:certificate_file_key) { "certificate.key" }
  let(:port) { Flores::Random.integer(1024..65335) }
  let(:host) { "127.0.0.1" }

  before do
    expect(File).to receive(:read).at_least(1).with(certificate_file_crt) { certificate.first.to_s }
    expect(File).to receive(:read).at_least(1).with(certificate_file_key) { certificate.last.to_s }
    
    server = Lumberjack::Server.new(:port => port,
                                    :address => host,
                                    :ssl_certificate => certificate_file_crt,
                                    :ssl_key => certificate_file_key)

    Thread.new do
      server.run { |data| }
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
end

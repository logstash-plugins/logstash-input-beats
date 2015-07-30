# encoding: utf-8
require "lib/lumberjack/client"
require "lib/lumberjack/server"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"

describe "A client" do
  let(:port) { Flores::Random.integer(1024..65335) }
  let(:csr) { Flores::PKI::CertificateSigningRequest.new }
  let(:key_bits) { 1024 }
  let(:key) { OpenSSL::PKey::RSA.generate(key_bits, 65537) }
  let(:certificate_duration) { Flores::Random.number(1..86400) }
  let(:certificate_subject) { "CN=server.example.com" }
  let(:certificate) { csr.create }
  let(:certificate_file_crt) { "certificate.crt" }
  let(:certificate_file_key) { "certificate.key" }
  let(:host) { "127.0.0.1" }
  let(:queue) { Queue.new }
  let(:payload) { {"line" => "hello world" } }

  before do
    csr.subject = certificate_subject
    csr.public_key = key.public_key
    csr.start_time = Time.now
    csr.expire_time = csr.start_time + certificate_duration
    csr.signing_key = key
    csr.want_signature_ability = true

    expect(File).to receive(:read).twice.with(certificate_file_crt) { certificate.to_s }
    expect(File).to receive(:read).with(certificate_file_key) { key.to_s }
    
    server = Lumberjack::Server.new(:port => port,
                                    :address => host,
                                    :ssl_certificate => certificate_file_crt,
                                    :ssl_key => certificate_file_key)

    Thread.new do
      server.run { |data| queue.push(data) }
    end
  end

  it "should require a certificate" do
    expect { 
      client = Lumberjack::Client.new(:port => port, 
                             :host => host,
                             :addresses => host,
                             :ssl_certificate => certificate_file_crt)

      client.write(payload)
    }.not_to raise_error

    expect(queue.pop).to eq(payload)
  end
end

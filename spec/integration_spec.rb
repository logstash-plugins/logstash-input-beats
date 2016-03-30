# encoding: utf-8
require "lumberjack/beats/client"
require "lumberjack/beats/server"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"
require_relative "./support/file_helpers"
require_relative "./support/flores_extensions"

Thread.abort_on_exception = true
describe "A client" do
  include FileHelpers

  let(:keysize) { 1024 }
  let(:exponent) { 65537 }

  let(:certificate) { Flores::PKI.generate }
  let(:certificate_file_crt) do
     p = Stud::Temporary.file
     p.write(certificate.first.to_s)
     p.close
     p.path
  end

  let(:certificate_file_key) do
    p = Stud::Temporary.file
    p.write(certificate.last.to_s)
    p.close
    p.path
  end
  let(:port) { Flores::Random.port }
  let(:tcp_port) { Flores::Random.port }
  let(:host) { "127.0.0.1" }
  let(:queue) { [] }
  let(:config_ssl) do
    {
      :port => port,
      :address => host,
      :ssl_certificate => certificate_file_crt,
      :ssl_key => certificate_file_key
    }
  end

  let(:config_tcp) do
    { :port => tcp_port, :address => host, :ssl => false }
  end

  before :each do
    tcp_server = Lumberjack::Beats::Server.new(config_tcp)
    ssl_server = Lumberjack::Beats::Server.new(config_ssl)

    @tcp_server = Thread.new do
      tcp_server.run { |data, identity_stream| queue << [data, identity_stream] }
      while true
          tcp_server.accept do |socket|
            next if socket.nil?

            begin
              con = Lumberjack::Beats::Connection.new(socket, tcp_server)
              con.run { |data, identity_stream| queue << [data, identity_stream] }
            rescue
              # Close connection on failure. For example SSL client will make
              # parser for TCP based server trip.
              # Connection is closed by Server connection object
            end
          end
      end
    end

    @ssl_server = Thread.new do
      ssl_server.run { |data, identity_stream| queue << [data, identity_stream] }
    end

    sleep(0.1) while @ssl_server.status != "run" && @tcp_server != "run"
  end

  after :each do
    @tcp_server.kill
    @ssl_server.kill
  end

  shared_examples "send payload" do
    it "supports single element" do
      (1..random_number_of_events).each do |n|
        expect(client.write(payload)).to eq(sequence_start + n)
      end
      sleep(0.5) # give time to the server to read the events
      expect(queue.size).to eq(random_number_of_events)
      expect(queue.collect(&:last).uniq.size).to eq(1)
    end

    it "support sending multiple elements in one payload" do
      expect(client.write(batch_payload)).to eq(sequence_start + batch_size)
      sleep(0.5)

      expect(queue.size).to eq(batch_size)
      expect(queue.collect(&:last).uniq.size).to eq(1)
    end
  end

  shared_examples "transmit payloads" do
    let(:random_number_of_events) { Flores::Random.integer(2..10) }
    let(:payload) { { "line" => "foobar" } }
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
      let(:sequence_start) { Lumberjack::Beats::SEQUENCE_MAX - batch_size / 2 }

      before do
        client.instance_variable_get(:@socket).instance_variable_set(:@sequence, sequence_start)
      end

      it "adjusts the ack" do
        expect(client.write(batch_payload)).to eq(batch_size / 2)
        sleep(0.5)
        expect(queue.size).to eq(batch_size)
        expect(queue.collect(&:first)).to match_array(batch_payload)
      end
    end
  end

  context "using plain tcp connection" do
    it "should successfully connect to tcp server if ssl explicitely disabled" do
      expect {
        Lumberjack::Beats::Client.new(:port => tcp_port, :host => host, :addresses => host, :ssl => false)
      }.not_to raise_error
    end

    it "should fail to connect to tcp server if ssl not explicitely disabled" do
      expect {
        Lumberjack::Beats::Client.new(:port => tcp_port, :host => host, :addresses => host)
      }.to raise_error(RuntimeError, /Must set a ssl certificate/)
    end

    it "should fail to communicate to ssl based server" do
      expect {
        client = Lumberjack::Beats::Client.new(:port => port,
                                               :host => host,
                                               :addresses => host,
                                               :ssl => false)
        client.write({ "line" => "foobar" })
      }.to raise_error
    end

    context "When transmitting a payload" do
      let(:options) { {:port => tcp_port, :host => host, :addresses => host, :ssl => false } }
      let(:client) { Lumberjack::Beats::Client.new(options) }

      context "json" do
        let(:options) { super.merge({ :json => true }) }
        include_examples "transmit payloads"
      end

      context "v1 frame" do
        include_examples "transmit payloads"
      end
    end
  end

  context "using TLS/SSL encrypted connection" do
    context "without a ca chain" do
      context "with a valid certificate" do
        it "successfully connect to the server" do
          expect {
            Lumberjack::Beats::Client.new(:port => port,
                                   :host => host,
                                   :addresses => host,
                                   :ssl_certificate_authorities => certificate_file_crt)
          }.not_to raise_error
        end

        it "should fail connecting to plain tcp server" do
          expect {
            Lumberjack::Beats::Client.new(:port => tcp_port,
                                   :host => host,
                                   :addresses => host,
                                   :ssl_certificate_authorities => certificate_file_crt)
          }.to raise_error(OpenSSL::SSL::SSLError)
        end
      end

      context "with an invalid certificate" do
        let(:invalid_certificate) { Flores::PKI.generate }
        let(:invalid_certificate_file) { Stud::Temporary.file.path }

        it "should refuse to connect" do
          expect {
            Lumberjack::Beats::Client.new(:port => port,
                                          :host => host,
                                          :addresses => host,
                                          :ssl_certificate_authorities => invalid_certificate_file)

          }.to raise_error(OpenSSL::SSL::SSLError, /certificate verify failed/)
        end
      end

      context "When transmitting a payload" do
        let(:client) do
          Lumberjack::Beats::Client.new(:port => port,
                                        :host => host,
                                        :addresses => host,
                                        :ssl_certificate_authorities => certificate_file_crt)
        end

        context "json" do
          let(:options) { super.merge({ :json => true }) }
          include_examples "transmit payloads"
        end

        context "v1 frame" do
          include_examples "transmit payloads"
        end

        context "when the key has a passphrase" do
          let(:passphrase) { "bonjour la famille" }
          let(:cipher) { OpenSSL::Cipher.new("des3") }
          let(:encrypted_key) { certificate.last.to_pem(cipher, passphrase) }

          let_tmp_file(:certificate_file_key) { encrypted_key }

          let(:config_ssl) do
            super.merge({ :ssl_key_passphrase => passphrase })
          end

          include_examples "transmit payloads"
        end
      end
    end
  end

  def write_to_tmp_file(content)
    file = Stud::Temporary.file
    file.write(content.to_s)
    file.close
    file.path
  end

  context "Mutual validation" do
    let(:host) { "localhost" }
    let(:certificate_duration) { Flores::Random.number(1000..2000) }
    let(:client) { Lumberjack::Beats::Client.new(options) }
    let(:options) { { :ssl => true, :port => port, :addresses => host } }
    let(:config_ssl) do
      {
        :port => port,
        :address => host,
        :ssl => true,
        :ssl_verify_mode => "force_peer"
      }
    end

    context "with a self signed certificate" do
      let(:certificate) { Flores::PKI.generate }
      let_tmp_file(:certificate_file) { certificate.first }
      let_tmp_file(:certificate_key_file) { certificate.last }
      let(:options) do
        super.merge({ :ssl_certificate => certificate_file, 
                      :ssl_certificate_key => certificate_key_file, 
                      :ssl_certificate_authorities => certificate_file })
      end

      let(:config_ssl) do
        super.merge({ :ssl_certificate_authorities => certificate_file,
                      :ssl_certificate => certificate_file,
                      :ssl_key => certificate_key_file })
      end

      include_examples "transmit payloads"
    end

    context "with certificate authorities" do
      let(:root_ca) { Flores::PKI.generate("CN=root.logstash") }
      let(:root_ca_certificate) { root_ca.first }
      let(:root_ca_key) { root_ca.last }
      let_tmp_file(:root_ca_certificate_file) { root_ca_certificate }

      context "directly signing a certificate" do
        let(:client_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:client_certificate_key_file) { client_certificate_key }
        let(:client_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = client_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = root_ca_key
          csr.signing_certificate = root_ca_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:client_certificate_file) { client_certificate }

        let(:server_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let(:server_certificate_key_file) { write_to_tmp_file(server_certificate_key) }
        let(:server_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = server_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = root_ca_key
          csr.signing_certificate = root_ca_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:server_certificate_file) { server_certificate }

        let(:options) do
          super.merge({ :ssl_certificate => client_certificate_file, 
                        :ssl_certificate_key => client_certificate_key_file, 
                        :ssl_certificate_authorities => root_ca_certificate_file })
        end

        let(:config_ssl) do
          super.merge({ :ssl_certificate_authorities => root_ca_certificate_file,
                        :ssl_certificate => server_certificate_file,
                        :ssl_key => server_certificate_key_file, 
                        :verify_mode => :force_peer })
        end
        
        include_examples "transmit payloads"
      end

      # Doesnt work because of this issues in `jruby-openssl`
      # https://github.com/jruby/jruby-openssl/issues/84
      xcontext "CA given with client providing intermediate and client certificate" do
        let(:intermediate_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:intermediate_certificate_key_file) { intermediate_certificate_key }
        let(:intermediate_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = intermediate_certificate_key.public_key
          csr.subject = "CN=intermediate 1"
          csr.signing_key = root_ca_key
          csr.signing_certificate = root_ca_certificate
          csr.want_signature_ability = true
          csr.create
        end
        let_tmp_file(:intermediate_certificate_file) { intermediate_certificate }
        let(:client_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:client_certificate_key_file) { client_certificate_key }
        let(:client_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = client_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = intermediate_certificate_key
          csr.signing_certificate = intermediate_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:client_certificate_file) { client_certificate }

        let(:server_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:server_certificate_key_file) { server_certificate_key }
        let(:server_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = server_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = intermediate_certificate_key
          csr.signing_certificate = intermediate_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:server_certificate_file) { server_certificate }
        let_tmp_file(:certificate_chain_file) { Flores::PKI.chain_certificates(root_ca_certificate, intermediate_certificate) }

        let(:options) do
          super.merge({ :ssl_certificate => client_certificate_file, 
                        :ssl_certificate_key => client_certificate_key_file, 
                        :ssl_certificate_authorities => certificate_chain_file })
        end

        let(:config_ssl) do
          super.merge({ :ssl_certificate_authorities => certificate_chain_file,
                        :ssl_certificate => server_certificate_file,
                        :ssl_key => server_certificate_key_file, 
                        :verify_mode => :force_peer })


        end

        include_examples "transmit payloads"
      end

      context "Mutiple CA different clients" do
        let(:secondary_root_ca) { Flores::PKI.generate("CN=secondary.root.logstash") }
        let(:secondary_root_ca_certificate) { root_ca.first }
        let(:secondary_root_ca_key) { root_ca.last }
        let_tmp_file(:secondary_root_ca_certificate_file) { root_ca_certificate }

        let(:secondary_client_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let(:secondary_client_certificate_key_file) { write_to_tmp_file(secondary_client_certificate_key) }
        let(:secondary_client_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = secondary_client_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = secondary_root_ca_key
          csr.signing_certificate = secondary_root_ca_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:secondary_client_certificate_file) { secondary_client_certificate }
        let(:client_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:client_certificate_key_file) { client_certificate_key }
        let(:client_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = client_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = root_ca_key
          csr.signing_certificate = root_ca_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:client_certificate_file) { client_certificate }

        let(:server_certificate_key) { OpenSSL::PKey::RSA.generate(keysize, exponent) } 
        let_tmp_file(:server_certificate_key_file) { server_certificate_key }
        let(:server_certificate) do
          csr = Flores::PKI::CertificateSigningRequest.new
          csr.start_time = Time.now
          csr.expire_time = csr.start_time + certificate_duration
          csr.public_key = server_certificate_key.public_key
          csr.subject = "CN=localhost"
          csr.signing_key = root_ca_key
          csr.signing_certificate = root_ca_certificate
          csr.want_signature_ability = false
          csr.create
        end
        let_tmp_file(:server_certificate_file) { server_certificate }

        shared_examples "multiples client" do
          context "client from primary CA" do
            let(:options) do
              super.merge({ :ssl_certificate => client_certificate_file, 
                            :ssl_certificate_key => client_certificate_key_file, 
                            :ssl_certificate_authorities => [root_ca_certificate_file] })
            end

            include_examples "transmit payloads"
          end

          context "client from secondary CA" do
            let(:options) do
              super.merge({ :ssl_certificate => secondary_client_certificate_file, 
                            :ssl_certificate_key => secondary_client_certificate_key_file, 
                            :ssl_certificate_authorities => [root_ca_certificate_file] })
            end

            include_examples "transmit payloads"
          end
        end

        context "when the CA are defined individually in the configuration" do
          let(:config_ssl) do
            super.merge({ :ssl_certificate_authorities => [secondary_root_ca_certificate_file, root_ca_certificate_file],
                          :ssl_certificate => server_certificate_file,
                          :ssl_key => server_certificate_key_file, 
                          :verify_mode => "force_peer" })
          end

          include_examples "multiples client"
        end

        context "when the CA are defined by a path in the configuration" do
          let(:ca_path) do
            p = Stud::Temporary.directory
            
            # Doing this here make sure the path is populated when the server
            # is started in the main before block
            FileUtils.mkdir_p(p)
            FileUtils.cp(secondary_root_ca_certificate_file, p)
            FileUtils.cp(root_ca_certificate_file, p)

            p
          end

          after :each do
            FileUtils.rm_rf(ca_path)
          end

          let(:config_ssl) do
            super.merge({ :ssl_certificate_authorities => ca_path,
                          :ssl_certificate => server_certificate_file,
                          :ssl_key => server_certificate_key_file, 
                          :verify_mode => "force_peer" })
          end

          include_examples "multiples client"
        end
      end
    end
  end
end

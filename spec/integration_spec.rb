# encoding: utf-8
require "lumberjack/beats/client"
require "lumberjack/beats/server"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"

Thread.abort_on_exception = true
describe "A client" do
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
  let(:port) { Flores::Random.integer(1024..65335) }
  let(:tcp_port) { port + 1 }
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
        let(:invalid_certificate_file) { "invalid.crt" }

        before do
          expect(File).to receive(:read).with(invalid_certificate_file) { invalid_certificate.first.to_s }
        end

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
      end
    end
  end

  context "when validating the client with a CA chain" do
    let(:certificate_authorities) { File.join(File.dirname(__FILE__), "fixtures", "certificate.chain.pem") }

    let(:server_certificate) { File.join(File.dirname(__FILE__), "fixtures", "localhost.crt") }
    let(:server_key) { File.join(File.dirname(__FILE__), "fixtures", "localhost.key") }

    let(:client_certificate) { File.join(File.dirname(__FILE__), "fixtures", "127.0.0.1.crt") }
    let(:client_key) { File.join(File.dirname(__FILE__), "fixtures", "127.0.0.1.key") }
    let(:ssl_key_passphrase) { "password" }
    let(:host) { "localhost" }

    let(:options) do
       { :port => port,
         :host => host,
         :addresses => host,
         :ssl_certificate => server_certificate,
         :ssl_certificate_key => server_key,
         :ssl_certificate_password => ssl_key_passphrase,
         :ssl_certificate_authorities => certificate_authorities,
         :ssl => true
       }
    end

    let(:client) { Lumberjack::Beats::Client.new(options) }

    let(:config_ssl) do
      super.merge({
        "ssl_certificate_authorities" => certificate_authorities,
        "ssl_certificate" => server_certificate,
        "ssl_key" => server_key,
        "ssl_key_passphrase" => ssl_key_passphrase
      })
    end

    include_examples "transmit payloads"
  end
end

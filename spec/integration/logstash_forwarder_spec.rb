# encoding: utf-8
require "logstash/inputs/beats"
require "logstash/json"
require "fileutils"
require_relative "../support/flores_extensions"
require_relative "../support/file_helpers"
require_relative "../support/integration_shared_context"
require_relative "../support/client_process_helpers"
require "spec_helper"

LSF_BINARY = File.expand_path(File.join(File.dirname(__FILE__), "..", "..", "vendor", "logstash-forwarder", "logstash-forwarder"))

describe "Logstash-Forwarder", :integration => true do
  include ClientProcessHelpers

  before :all do
    unless File.exist?(LSF_BINARY)
      raise "Cannot find `logstash-forwarder` binary in `vendor/logstash-forwarder` Did you run `bundle exec rake test:integration:setup` before running the integration suite?"
    end
  end

  include FileHelpers
  include_context "beats configuration"

  let(:client_wait_time) { 5 }

  # Filebeat related variables
  let(:cmd) { [lsf_exec, "-config", lsf_config_path] }

  let(:lsf_exec) { LSF_BINARY }
  let(:registry_file) { File.expand_path(File.join(File.dirname(__FILE__), "..", "..", ".logstash-forwarder")) }
  let_tmp_file(:lsf_config_path) { lsf_config }
  let(:log_file) { f = Stud::Temporary.file; f.close; f.path }
  let(:lsf_config) do
 <<-CONFIG
{
  "network": {
    "servers": [ "#{host}:#{port}" ],
    "ssl ca": "#{certificate_authorities}"
  },
  "files": [
    { "paths": [ "#{log_file}" ] }
  ]
}
 CONFIG
  end
  #

  before :each do
    FileUtils.rm_rf(registry_file) # ensure clean state between runs
    start_client
    sleep(1)
    # let LSF start and than write the logs
    File.open(log_file, "a") do |f|
      f.write(events.join("\n") + "\n")
    end
    sleep(1) # give some time to the clients to pick up the changes
  end

  after :each do
    stop_client
  end

  xcontext "Plain TCP" do
    include ClientProcessHelpers

    let(:certificate_authorities) { "" }

    it "should not send any events" do
      expect(queue.size).to eq(0), @execution_output
    end
  end

  context "TLS" do
    context "Server Verification" do
      let(:input_config) do
        super.merge({ 
          "ssl" => true,
          "ssl_certificate" => certificate_file,
          "ssl_key" => certificate_key_file,
        })
      end

      let(:certificate_data) { Flores::PKI.generate }
      let_tmp_file(:certificate_file) { certificate_data.first }
      let_tmp_file(:certificate_key_file) { convert_to_pkcs8(certificate_data.last) }
      let(:certificate_authorities) { certificate_file }

      context "self signed certificate" do
        include_examples "send events"
      end

      context "invalid CA on the client" do
        let(:invalid_data) { Flores::PKI.generate }
        let(:certificate_authorities) { f = Stud::Temporary.file; f.close; f.path }

        it "should not send any events" do
          expect(queue.size).to eq(0), @execution_output
        end
      end
    end
  end
end

# encoding: utf-8
require "logstash/inputs/beats"
require "stud/temporary"
require "flores/pki"
require "fileutils"
require "thread"
require "spec_helper"
require "yaml"
require "fileutils"
require "flores/pki"
require_relative "../support/flores_extensions"
require_relative "../support/file_helpers"
require_relative "../support/integration_shared_context"
require_relative "../support/client_process_helpers"

FILEBEAT_BINARY = File.expand_path(File.join(File.dirname(__FILE__), "..", "..", "vendor", "filebeat", "filebeat"))

describe "Filebeat", :integration => true do
  include ClientProcessHelpers
  include FileHelpers

  before :all do
    unless File.exist?(FILEBEAT_BINARY)
      raise "Cannot find `Filebeat` binary in `vendor/filebeat`. Did you run `bundle exec rake test:integration:setup` before running the integration suite?"
    end
  end

  include_context "beats configuration"

  # Filebeat related variables
  let(:cmd) { [filebeat_exec, "-c", filebeat_config_path, "-e", "-v"] }

  let(:filebeat_exec) { FILEBEAT_BINARY }

  let_empty_tmp_file(:registry_file)
  let(:filebeat_config) do
    { 
      "filebeat" => { 
        "prospectors" => [{ "paths" => [log_file],  "input_type" => "log" }],
        "registry_file" => registry_file,
        "scan_frequency" => "1s"
      },
      "output" => { 
        "logstash" => { "hosts" => ["#{host}:#{port}"] },
        "logging" => { "level" => "debug" }
      }
    }
  end

  let_tmp_file(:filebeat_config_path) { YAML.dump(filebeat_config) }
  before :each do
    start_client
    sleep(2) # give some time to FB to send somthing
    stop_client
  end

  ###########################################################
  shared_context "Root CA" do
    let(:root_ca) { Flores::PKI.generate("CN=root.localhost") }
    let(:root_ca_certificate) { root_ca.first }
    let(:root_ca_key) { root_ca.last }
    let_tmp_file(:root_ca_certificate_file) { root_ca_certificate }
  end

  shared_context "Intermediate CA" do
    let(:intermediate_ca) { Flores::PKI.create_intermediate_certificate("CN=intermediate.localhost", root_ca_certificate, root_ca_key) }
    let(:intermediate_ca_certificate) { intermediate_ca.first }
    let(:intermediate_ca_key) { intermediate_ca.last }
    let_tmp_file(:certificate_authorities_chain) { Flores::PKI.chain_certificates(root_ca_certificate, intermediate_ca_certificate) }
  end

  ############################################################
  # Actuals tests 
  context "Plain TCP" do
    include_examples "send events"
  end

  context "TLS" do
    context "Server verification" do
      let(:filebeat_config) do
        super.merge({
          "output" => {
            "logstash" => {
              "hosts" => ["#{host}:#{port}"],
              "tls" => { "certificate_authorities" => certificate_authorities }
            },
            "logging" => { "level" => "debug" }
          }})
      end

      let(:input_config) do
        super.merge({ 
          "ssl" => true,
          "ssl_certificate" => certificate_file,
          "ssl_key" => certificate_key_file
        })
      end

      let(:certificate_authorities) { [certificate_file] }
      let(:certificate_data) { Flores::PKI.generate }
      let_tmp_file(:certificate_key_file) { certificate_data.last } 
      let_tmp_file(:certificate_file) { certificate_data.first }

      context "self signed certificate" do
        include_examples "send events"
      end

      context "CA root" do
        include_context "Root CA"

        context "directly signed client certificate" do
          let(:certificate_authorities) { [root_ca_certificate_file] }
          let(:certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", root_ca_certificate, root_ca_key) }

          include_examples "send events"
        end

        context "intermediate CA signs client certificate" do
          include_context "Intermediate CA"

          let(:certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", intermediate_ca_certificate, intermediate_ca_key) }
          let(:certificate_authorities) { [certificate_authorities_chain] }
          
          include_examples "send events"
        end
      end

      context "Client verification / Mutual validation" do
        let(:filebeat_config) do
          super.merge({
            "output" => {
              "logstash" => {
                "hosts" => ["#{host}:#{port}"],
                "tls" => { 
                  "certificate_authorities" => certificate_authorities,
                  "certificate" => certificate_file,
                  "certificate_key" => certificate_key_file
                }
              },
              "logging" => { "level" => "debug" }
            }})
        end

        let(:input_config) do
          super.merge({ 
            "ssl" => true,
            "ssl_certificate_authorities" => certificate_authorities,
            "ssl_certificate" => server_certificate_file,
            "ssl_key" => server_certificate_key_file,
            "ssl_verify_mode" => "force_peer"
          })
        end

        context "with a self signed certificate" do
          let(:certificate_authorities) { [certificate_file] }
          let(:certificate_data) { Flores::PKI.generate }
          let_tmp_file(:certificate_key_file) { certificate_data.last } 
          let_tmp_file(:certificate_file) { certificate_data.first }
          let_tmp_file(:server_certificate_file) { certificate_data.first }
          let_tmp_file(:server_certificate_key_file) { certificate_data.last }

          include_examples "send events"
        end

        context "CA root" do
          include_context "Root CA"

          let_tmp_file(:server_certificate_file) { server_certificate_data.first }
          let_tmp_file(:server_certificate_key_file) { server_certificate_data.last }

          context "directly signed client certificate" do
            let(:certificate_authorities) { [root_ca_certificate_file] }
            let(:certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", root_ca_certificate, root_ca_key) }
            let(:server_certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", root_ca_certificate, root_ca_key) }

            include_examples "send events"
          end


          # Doesnt work because of this issues in `jruby-openssl`
          # https://github.com/jruby/jruby-openssl/issues/84
          context "intermediate create server and client certificate" do
            include_context "Intermediate CA"

            let(:certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", intermediate_ca_certificate, intermediate_ca_key) }
            let(:server_certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", intermediate_ca_certificate, intermediate_ca_key) } 
            let(:certificate_authorities) { [certificate_authorities_chain] }

            include_examples "send events"
          end

          context "and Secondary CA multiples clients" do
            context "with CA in different files" do
              let(:secondary_ca) { Flores::PKI.generate }
              let(:secondary_ca_key) { secondary_ca.last }
              let(:secondary_ca_certificate) { secondary_ca.first }
              let_tmp_file(:secondary_ca_certificate_file) { secondary_ca.first }

              let(:secondary_client_certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", secondary_ca_certificate, secondary_ca_key) }
              let_tmp_file(:secondary_client_certificate_file) { secondary_client_certificate_data.first }
              let_tmp_file(:secondary_client_certificate_key_file) { secondary_client_certificate_data.last }
              let(:certificate_authorities) { [root_ca_certificate_file, secondary_ca_certificate_file] }
              let(:certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", root_ca_certificate, root_ca_key) }

              let(:server_certificate_data) { Flores::PKI.create_client_certicate("CN=localhost", root_ca_certificate, root_ca_key) }

              context "client from primary CA" do 
                include_examples "send events"
              end

              context "client from secondary CA" do
                let(:filebeat_config) do
                  super.merge({
                    "output" => {
                      "logstash" => {
                        "hosts" => ["#{host}:#{port}"],
                        "tls" => { 
                          "certificate_authorities" => certificate_authorities,
                          "certificate" => secondary_client_certificate_file,
                          "certificate_key" => secondary_client_certificate_key_file
                        }
                      },
                      "logging" => { "level" => "debug" }
                    }})
                end

                include_examples "send events"
              end
            end
          end
        end
      end
    end
  end
end

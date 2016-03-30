# encoding: utf-8
require "flores/pki"
require "flores/random"

module Flores
  module Random
    DEFAULT_PORT_RANGE = 1024..65535
    DEFAULT_PORT_CHECK_TIMEOUT = 1
    DEFAULT_MAXIMUM_PORT_FIND_TRY = 15

    class << self
      def port(range = DEFAULT_PORT_RANGE)
        try = 0
        while try < DEFAULT_MAXIMUM_PORT_FIND_TRY
          candidate = integer(range)

          if port_available?(candidate)
            break
          else
            try += 1
          end
        end
        
        raise "Flores.random_port: Cannot find an available port, tried #{DEFAULT_MAXIMUM_PORT_FIND_TRY} times, range was: #{range}" if try == DEFAULT_MAXIMUM_PORT_FIND_TRY

        candidate
      end
      
      def port_available?(port)
        begin
          server = TCPServer.new(port)
          available = true
        rescue # Assume that any errors can do this
          available = false
        ensure
          server.close if server
        end

        return available
      end
    end
  end

  module PKI
    DEFAULT_CERTIFICATE_OPTIONS = {
      :duration => Flores::Random.number(100..2000),
      :key_size => GENERATE_DEFAULT_KEY_SIZE, 
      :exponent => GENERATE_DEFAULT_EXPONENT,
      :want_signature_ability => false
    }

    def self.chain_certificates(*certificates)
      certificates.join("\n")
    end

    def self.create_intermediate_certificate(subject, signing_certificate, signing_private_key, options  = {})
      create_a_signed_certificate(subject, signing_certificate, signing_private_key, options.merge({ :want_signature_ability => true }))
    end

    def self.create_client_certicate(subject, signing_certificate, signing_private_key, options = {})
      create_a_signed_certificate(subject, signing_certificate, signing_private_key, options)
    end

    private
    def self.create_a_signed_certificate(subject, signing_certificate, signing_private_key, options = {})
      options = DEFAULT_CERTIFICATE_OPTIONS.merge(options)

      client_key = OpenSSL::PKey::RSA.new(options[:key_size], options[:exponent])

      csr = Flores::PKI::CertificateSigningRequest.new
      csr.start_time = Time.now
      csr.expire_time = csr.start_time + options[:duration]
      csr.public_key = client_key.public_key
      csr.subject = subject
      csr.signing_key = signing_private_key
      csr.signing_certificate = signing_certificate
      csr.want_signature_ability = options[:want_signature_ability]

      [csr.create, client_key]
    end
  end
end

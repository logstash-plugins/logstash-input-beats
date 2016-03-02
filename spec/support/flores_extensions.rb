# encoding: utf-8
require "flores/pki"
module Flores
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

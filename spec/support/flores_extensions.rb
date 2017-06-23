# encoding: utf-8
require "flores/pki"
require "flores/random"
require "socket"

module Flores
  module Random
    DEFAULT_PORT_RANGE = 1024..65535
    class << self
      def port(range = DEFAULT_PORT_RANGE)
        integer(range)
      end
    end
  end

  module PKI

    # Monkey patched the fix for https://github.com/jordansissel/ruby-flores/issues/9
    # TODO: remove this once Flores is released with fix.
    CertificateSigningRequest.class_eval do
      def create
        validate!
        extensions = OpenSSL::X509::ExtensionFactory.new
        extensions.subject_certificate = certificate
        extensions.issuer_certificate = self_signed? ? certificate : signing_certificate

        certificate.issuer = extensions.issuer_certificate.subject
        certificate.add_extension(extensions.create_extension("subjectKeyIdentifier", "hash", false))

        if want_signature_ability?
          # Create a CA.
          certificate.add_extension(extensions.create_extension("basicConstraints", "CA:TRUE", true))
          # Rough googling seems to indicate at least keyCertSign is required for CA and intermediate certs.
          certificate.add_extension(extensions.create_extension("keyUsage", "keyCertSign, cRLSign, digitalSignature", true))
        else
          # Create a client+server certificate
          #
          # It feels weird to create a certificate that's valid as both server and client, but a brief inspection of major
          # web properties (apple.com, google.com, yahoo.com, github.com, fastly.com, mozilla.com, amazon.com) reveals that
          # major web properties have certificates with both clientAuth and serverAuth extended key usages. Further,
          # these major server certificates all have digitalSignature and keyEncipherment for key usage.
          #
          # Here's the command I used to check this:
          #    echo mozilla.com apple.com github.com google.com yahoo.com fastly.com elastic.co amazon.com \
          #    | xargs -n1 sh -c 'openssl s_client -connect $1:443 \
          #    | sed -ne "/-----BEGIN CERTIFICATE-----/,/-----END CERTIFICATE-----/p" \
          #    | openssl x509 -text -noout | sed -ne "/X509v3 extensions/,/Signature Algorithm/p" | sed -e "s/^/$1 /"' - \
          #    | grep -A2 'Key Usage'
          certificate.add_extension(extensions.create_extension("keyUsage", "digitalSignature, keyEncipherment", true))
          certificate.add_extension(extensions.create_extension("extendedKeyUsage", "clientAuth, serverAuth", false))
        end

        if @subject_alternates
          certificate.add_extension(extensions.create_extension("subjectAltName", @subject_alternates.join(",")))
        end

        certificate.serial = OpenSSL::BN.new(serial)
        certificate.sign(signing_key, digest_method)
        certificate
      end
    end


    DEFAULT_CERTIFICATE_OPTIONS = {
      :duration => 86400, #one day
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


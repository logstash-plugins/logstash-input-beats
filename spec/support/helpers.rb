# encoding: utf-8
require "open3"
OPEN_SSL_TOPK8 = "openssl pkcs8 -nocrypt -topk8 -inform PEM -outform PEM"

# Netty only accepts PKC8 format for the private key, which in the real world is fine
# because any newer version of OpenSSL with use that format by default.
#
# But in Ruby or Jruby-OpenSSL, the private key will be generates in PKCS7, which netty doesn't support.
# Converting the format is a bit of hassle to do in code so In this case its just easier to use the `openssl` binary to do the work.
#
#
def convert_to_pkcs8(key)
  out, e, s = Open3.capture3(OPEN_SSL_TOPK8, :stdin_data => key.to_s)
  # attempt to address random failures by trying again
  unless s.success?
    sleep 1
    out, e, s = Open3.capture3(OPEN_SSL_TOPK8, :stdin_data => key.to_s)
    raise e if e != ""
    out
  end

  out
end

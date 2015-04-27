require "stud/temporary"
module LogStashTest
  class Certicate
    attr_reader :ssl_key, :ssl_cert

    def initialize
      @ssl_cert = Stud::Temporary.pathname("ssl_certificate")
      @ssl_key = Stud::Temporary.pathname("ssl_key")

      system("openssl req -x509  -batch -nodes -newkey rsa:2048 -keyout #{ssl_key} -out #{ssl_cert} -subj /CN=localhost > /dev/null 2>&1")
    end
  end

  class << self
    def certificate
      Certicate.new
    end

    def random_port
      rand(2000..10000)
    end
  end
end

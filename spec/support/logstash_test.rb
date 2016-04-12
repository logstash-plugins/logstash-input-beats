require "stud/temporary"


# namespace the Dummy* classes, they are reused names
# use a more specific module name to prevent clashes
module BeatsInputTest
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

  class DummyNeverBlockedQueue < Array
    def offer(element, timeout = nil)
      push(element)
    end

    alias_method :take, :shift
  end

  class DummyConnection
    def initialize(events)
      @events = events
    end

    def run
      @events.each do |element|
        yield element[:map], element[:identity_stream]
      end
    end

    def peer
      "localhost:5555"
    end
  end

  class DummyCodec
    def register() end
    def decode(*) end
    def clone() self; end
    def base_codec
      self
    end
    def self.config_name
      "dummy"
    end
  end
end




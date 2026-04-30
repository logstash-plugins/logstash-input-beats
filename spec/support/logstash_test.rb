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

    def p12_key
      p12_key = Stud::Temporary.pathname("p12_key")
      system "openssl pkcs12 -export -passout pass:123 -inkey #{ssl_key} -in #{ssl_cert} -out #{p12_key}"
      p12_key
    end

  end

  class << self
    def certificate
      Certicate.new
    end

    def random_port
      rand(2000..10000)
    end

    ##
    # Returns the IP address of an interfaace we own that is neither loopback nor multicast.
    def own_ip_address
      Socket.ip_address_list.lazy
            .select(&:ip?)
            .reject(&:ipv4_loopback?).reject(&:ipv6_loopback?)
            .reject(&:ipv4_multicast?).reject(&:ipv6_multicast?)
            .map(&:ip_address)
            .first || fail("no serviceable IP addresses on this host: #{Socket.ip_address_list}")
    end

    ##
    # yield the block with a port that is available
    # @return [Integer]: a port that is available
    def find_available_port(host:"::")
      with_bound_port(host: host, &:itself)
    end

    ##
    # Yields block with a port that is unavailable
    # @yieldparam port [Integer]
    # @yieldreturn [Object]
    # @return [Object]
    def with_bound_port(host:"::", port:0, &block)
      server = TCPServer.new(host, port)

      return yield(server.local_address.ip_port)
    ensure
      server.close
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




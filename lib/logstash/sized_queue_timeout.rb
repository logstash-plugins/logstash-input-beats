require "concurrent/atomic/condition"
require "thread"

module LogStash
  # Minimal subset implement of a SizedQueue supporting
  # a timeout option on the lock.
  #
  # This will be part of the main Logstash's sized queue
  class SizedQueueTimeout
    class TimeoutError < StandardError; end

    DEFAULT_TIMEOUT = 2 # in seconds

    def initialize(max_size, options = {})
      @condition_in = Concurrent::Condition.new
      @condition_out = Concurrent::Condition.new

      @max_size = max_size
      @queue = []
      @mutex = Mutex.new
    end

    def push(obj, timeout = DEFAULT_TIMEOUT)
      @mutex.synchronize do
        while full? # wake up check
          result = @condition_out.wait(@mutex, timeout) 
          raise TimeoutError if result.timed_out?
        end

        @queue << obj
        @condition_in.signal

        return obj
      end
    end
    alias_method :<<, :push

    def size
      @mutex.synchronize { @queue.size }
    end

    def pop_no_timeout
      @mutex.synchronize do
        @condition_in.wait(@mutex) while @queue.empty? # Wake up check

        obj = @queue.shift
        @condition_out.signal

        return obj
      end
    end

    private
    def full?
      @queue.size == @max_size
    end
  end
end

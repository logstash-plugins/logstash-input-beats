# encoding: utf-8
module LogStash::Inputs::BeatsSupport
  # Wrap the `Java SynchronousQueue` to acts as the synchronization mechanism
  # this queue can block for a maximum amount of time logstash's queue 
  # doesn't implement that feature.
  #  
  # See proposal for core: https://github.com/elastic/logstash/pull/4408
  #
  # See https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/SynchronousQueue.html
  java_import "java.util.concurrent.SynchronousQueue"
  java_import "java.util.concurrent.TimeUnit"
  class SynchronousQueueWithOffer
    def initialize(timeout, fairness_policy = true)
      # set Fairness policy to `FIFO`
      #
      # In the context of the input it makes sense to
      # try to deal with the older connection before
      # the newer one, since the older will be closer to
      # reach the connection timeout.
      #
      @timeout = timeout
      @queue = java.util.concurrent.SynchronousQueue.new(fairness_policy)
    end

    # This method will return true if it successfully added the element to the queue.
    # If the timeout is reached and it wasn't inserted successfully to 
    # the queue it will return false.
    def offer(element, timeout = nil)
      @queue.offer(element, timeout || @timeout, java.util.concurrent.TimeUnit::SECONDS)
    end

    def take
      @queue.take
    end
  end
end

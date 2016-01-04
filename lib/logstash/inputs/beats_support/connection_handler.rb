# encoding: utf-8
require "logstash/inputs/beats"
require "logstash/inputs/beats_support/decoded_event_transform"
require "logstash/inputs/beats_support/raw_event_transform"

module LogStash::Inputs::BeatsSupport
  # Handle the data coming from a connection
  # Decide which Process should be used to decode the data coming
  # from the beat library.
  #
  # - Should we use a codec on specific field?
  # - Should we just take the raw content of the parsed json frame
  class ConnectionHandler
    def initialize(connection, input, queue)
      @connection = connection

      @input = input
      @queue = queue
      @logger = input.logger

      # We need to clone the codec per connection, so we can flush a specific 
      # codec when a connection is closed.
      @codec = input.codec.dup

      @nocodec_transformer = RawEventTransform.new(@input)
      @codec_transformer = DecodedEventTransform.new(@input)
    end

    def accept
      @logger.debug("Beats input: waiting from new events from remote host",
                    :peer => @connection.peer)

      @connection.run { |hash, identity_stream| process(hash, identity_stream) }
    end

    def process(hash, identity_stream)
      @logger.debug? && @logger.debug("Beats input: new event received",
                                      :event_hash => hash,
                                      :identity_stream => identity_stream,
                                      :peer => @connection.peer)

      # Filebeats uses the `message` key and LSF `line`
      target_field = @input.target_field_for_codec ? hash.delete(@input.target_field_for_codec) : nil

      if target_field.nil?
        @logger.debug? && @logger.debug("Beats input: not using the codec for this event, can't find the codec target field",
                                        :target_field_for_codec => @input.target_field_for_codec,
                                        :event_hash => hash)

        event = LogStash::Event.new(hash)
        @nocodec_transformer.transform(event)

        raise LogStash::Inputs::Beats::InsertingToQueueTakeTooLong if !@queue.offer(event)
      else
        @logger.debug? && @logger.debug("Beats input: decoding this event with the codec",
                                        :target_field_value =>  target_field)

        @codec.accept(CodecCallbackListener.new(target_field,
                                                hash,
                                                identity_stream,
                                                @codec_transformer,
                                                @queue))
      end
    end

    # OOB call to flush the codec buffer, 
    #
    # This method is a bit tricky to decide when to be called, in the current case,
    # this will be call on any exception raised, either is a circuit breaker or the
    # remote host closed the connection, its better to make sure we clear their
    # data and create duplicates then losing the data.
    def flush(&block)
      @logger.debug? && @logger.debug("Beats input, out of band call for flushing the content of this connection",
                                      :peer => @connection.peer)

      @codec.flush(&block)
    end
  end
end

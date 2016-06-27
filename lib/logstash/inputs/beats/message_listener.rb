# encoding: utf-8
require "thread_safe"
require "logstash-input-beats_jars"
import "org.logstash.beats.MessageListener"

module LogStash module Inputs class Beats
  class MessageListener
    include org.logstash.beats.IMessageListener

    FILEBEAT_LOG_LINE_FIELD = "message".freeze
    LSF_LOG_LINE_FIELD = "line".freeze

    ConnectionState = Struct.new(:ctx, :codec)

    attr_reader :logger, :input, :connections_list

    def initialize(queue, input)
      @connections_list = ThreadSafe::Hash.new
      @queue = queue
      @logger = input.logger
      @input = input

      @nocodec_transformer = RawEventTransform.new(@input)
      @codec_transformer = DecodedEventTransform.new(@input)
    end

    def onNewMessage(ctx, message)
      hash = message.getData()

      target_field = extract_target_field(hash)

      if target_field.nil?
        event = LogStash::Event.new(hash)
        @nocodec_transformer.transform(event)
        @queue << event
      else
        codec(ctx).accept(CodecCallbackListener.new(target_field,
                                                    hash,
                                                    message.getIdentityStream(),
                                                    @codec_transformer,
                                                    @queue))
      end
    end

    def onNewConnection(ctx)
      register_connection(ctx)
    end

    def onConnectionClose(ctx)
      unregister_connection(ctx)
    end

    def onException(ctx)
      unregister_connection(ctx)
    end

    private
    def codec(ctx)
      connections_list[ctx].codec
    end

    def register_connection(ctx)
      connections_list[ctx] = ConnectionState.new(ctx, input.codec.dup)
    end

    def unregister_connection(ctx)
      flush_buffer(ctx)
      connections_list.delete(ctx)
    end

    def flush_buffer(ctx)
      transformer = EventTransformCommon.new(@input)

      codec(ctx).flush do |event|
        transformer.transform(event)
        @queue << event
      end
    end

    def extract_target_field(hash)
      if from_filebeat?(hash)
        hash.delete(FILEBEAT_LOG_LINE_FIELD).to_s
      elsif from_logstash_forwarder?(hash)
        hash.delete(LSF_LOG_LINE_FIELD).to_s
      end
    end

    def from_filebeat?(hash)
      !hash[FILEBEAT_LOG_LINE_FIELD].nil?
    end

    def from_logstash_forwarder?(hash)
      !hash[LSF_LOG_LINE_FIELD].nil?
    end
  end
end; end; end

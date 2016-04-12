# encoding: utf-8
require "logstash/inputs/beats_support/event_transform_common"
module LogStash::Inputs::BeatsSupport
  # Take the extracted content from the codec, merged with the other data coming
  # from beats, apply the configured tags, normalize the host and try to coerce
  # the timestamp if it was provided in the hash.
  class DecodedEventTransform < EventTransformCommon
    def transform(event, hash)
      ts = coerce_ts(hash.delete("@timestamp"))

      event["@timestamp"] = ts unless ts.nil?
      hash.each do |k, v|
        # LSF doesn't always return utf-8 values
        v = v.force_encoding(Encoding::UTF_8)
        event[k] = v
      end
      super(event)
      event.tag("beats_input_codec_#{codec_name}_applied")
      event
    end

    private
    def coerce_ts(ts)
      return nil if ts.nil?
      timestamp = LogStash::Timestamp.coerce(ts)

      return timestamp if timestamp

      @logger.warn("Unrecognized @timestamp value, setting current time to @timestamp",
                   :value => ts.inspect)
      return nil
    rescue LogStash::TimestampParserError => e
      @logger.warn("Error parsing @timestamp string, setting current time to @timestamp",
                   :value => ts.inspect, :exception => e.message)
      return nil
    end
  end
end

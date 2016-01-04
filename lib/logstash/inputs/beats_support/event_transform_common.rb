# encoding: utf-8
module LogStash::Inputs::BeatsSupport
  # Base Transform class, expose the plugin decorate method,
  # apply the tags and make sure we copy the beat hostname into `host`
  # for backward compatibility.
  class EventTransformCommon
    def initialize(input)
      @input = input
      @logger = input.logger
    end

    # Copies the beat.hostname field into the host field unless
    # the host field is already defined
    def copy_beat_hostname(event)
      host = event["[beat][hostname]"]

      if host && event["host"].nil?
        event["host"] = host
      end
    end

    # This break the `#decorate` method visibility of the plugin base
    # class, the method is protected and we cannot access it, but well ruby
    # can let you do all the wrong thing.
    #
    # I think the correct behavior would be to allow the plugin to return a 
    # `Decorator` object that we can pass to other objects, since only the 
    # plugin know the data used to decorate. This would allow a more component
    # based workflow.
    def decorate(event)
      @input.send(:decorate, event)
    end

    def transform(event)
      copy_beat_hostname(event)
      decorate(event)
      event
    end
  end
end

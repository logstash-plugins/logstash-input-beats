# encoding: utf-8
require "logstash/event"
require "logstash/inputs/beats"
require_relative "../../support/shared_examples"
require "spec_helper"

describe LogStash::Inputs::Beats::EventTransformCommon do
  subject { described_class.new(input).transform(event) }

  include_examples "Common Event Transformation", :disabled, "host"
  include_examples "Common Event Transformation", :v1, "[@metadata][input][beats][host][name]"
end

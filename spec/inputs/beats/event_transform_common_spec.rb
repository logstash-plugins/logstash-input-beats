# encoding: utf-8
require "spec_helper"
require "logstash/event"
require "logstash/inputs/beats"
require_relative "../../support/shared_examples"

describe LogStash::Inputs::Beats::EventTransformCommon do
  subject { described_class.new(input).transform(event) }

  include_examples "Common Event Transformation"
end

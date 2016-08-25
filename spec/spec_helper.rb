# encoding: utf-8
require "rspec"
require "rspec/mocks"
require "rspec/wait"
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/plain"
require_relative "support/logstash_test"
require_relative "support/helpers"

$: << File.realpath(File.join(File.dirname(__FILE__), "..", "lib"))

RSpec.configure do |config|
  config.order = :rand
end


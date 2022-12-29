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

module SpecHelper

  def self.es_major_version
    es_version.split('.').first
  end

  def self.es_version
    [
      nilify(RSpec.configuration.filter[:es_version]),
      nilify(ENV['ES_VERSION']),
      nilify(ENV['ELASTIC_STACK_VERSION']),
    ].compact.first
  end

  private
  def self.nilify(str)
    if str.nil?
      return str
    end
    str.empty? ? nil : str
  end
end
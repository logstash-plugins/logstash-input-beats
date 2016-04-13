# encoding: utf-8
require "json"
module Lumberjack module Beats
  FILEBEAT_LOG_LINE_FIELD = "message".freeze
  LSF_LOG_LINE_FIELD = "line".freeze
  SEQUENCE_MAX = (2**32-1).freeze

  @@json = Class.new do
    def self.load(blob)
      JSON.parse(blob)
    end
    def self.dump(v)
      v.to_json
    end
  end

  def self.json
    @@json
  end

  def self.json=(j)
    @@json = j
  end
end; end

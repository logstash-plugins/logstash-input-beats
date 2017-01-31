# encoding: utf-8
require "sys/filesystem"
require "fileutils"

module LogStash module Inputs class Beats
  class StatsHelper
    # bitmask
    NO_EXEC_FLAG = 8

    # We want to be able to display a better error message, but I cannot call any public method
    # on netty classes to get the possible temporary directory so I have to port that logic here.
    #
    # But we only care about linux/unix
    #
    # https://netty.io/4.0/xref/io/netty/util/internal/NativeLibraryLoader.html
    #
    def self.netty_temporary_dir
      st = java.lang.System
      dir = st.getProperties["io.netty.native.workdir"] || st.getProperties["io.netty.tmpdir"] || st.getProperties["java.io.tmpdir"] || "/tmp"

      # Force creation, to make sure we can test with noexec
      FileUtils.mkdir_p(dir)
      dir
    end

    def self.noexec?(path)
      path = ::File.expand_path(path)

      parts = path.split(::File::SEPARATOR)
      number_of_iteration = parts.size

      results = {}

      number_of_iteration.times.collect do
        new_path = ::File.join(*parts)

        # we are interested all all the hierachy
        # but some directories could no exist yet?
        if Dir.exist?(new_path)
          stats = Sys::Filesystem.stat(new_path)
          results[new_path] = (stats.flags & NO_EXEC_FLAG) != 0
        end
        parts.pop
      end

      results.values.any?
    end
  end
end end end

# encoding: utf-8
require "jars/installer"
require "fileutils"

namespace :build do
  task :compile do
    system("./gradlew build")
    version = File.read("VERSION").strip
    FileUtils.mkdir_p("vendor/jar-dependencies")
    FileUtils.cp("build/libs/logstash-input-beats-#{version}.jar", "vendor/jar-dependencies")
  end

  desc "install vendored jars"
  task :install_jars => :compile do
    Jars::Installer.vendor_jars!("vendor/jar-dependencies")
  end

  desc "clean"
  task :clean do
    ["build", "vendor/jar-dependencies", "Gemfile.lock"].each do |p|
      FileUtils.rm_rf(p)
    end
  end
end

task :vendor => "build:install_jars"

# encoding: utf-8
require "jars/installer"
require "fileutils"

namespace :build do
  task :compile do
    system("gradle publishToMavenLocal")
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

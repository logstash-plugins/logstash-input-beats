# encoding: utf-8
VENDOR_PATH = File.expand_path(File.join(File.dirname(__FILE__), "..", "..", "vendor"))

#TODO: Figure out better means to keep this version in sync
FILEBEAT_VERSION = "8.19.14"
def filebeat_url
  @filebeat_url ||= begin
    host_os = RbConfig::CONFIG['host_os']
    host_cpu = RbConfig::CONFIG['host_cpu']

    fail("unsupported OS #{host_os}") unless %w(linux darwin).include?(host_os)
    fail("unsupported CPU #{host_cpu}") unless %w(aarch64 x86_64).include?(host_cpu)

    # for historic reasons aarch64 artifacts for linux are labeled arm64
    host_cpu = "arm64" if host_os == "linux" && host_cpu == "aarch64"

    "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-#{FILEBEAT_VERSION}-#{host_os}-#{host_cpu}.tar.gz"
  end
end

require "fileutils"
@files=[]

task :default do
  system("rake -T")
end

require "logstash/devutils/rake"

namespace :test do
  task :java do
    exit(1) unless system './gradlew test'
  end
  namespace :integration do
    task :setup do
      Rake::Task["test:integration:setup:filebeat"].invoke
    end

    namespace :setup do

      desc "Download nigthly filebeat for integration testing"
      task :filebeat do
        FileUtils.mkdir_p(VENDOR_PATH)
        download_destination = File.join(VENDOR_PATH, "filebeat.tar.gz")
        destination = File.join(VENDOR_PATH, "filebeat")
        FileUtils.rm_rf(download_destination)
        FileUtils.rm_rf(destination)
        FileUtils.rm_rf(File.join(VENDOR_PATH, "filebeat.tar"))

        puts "Filebeat: downloading from #{filebeat_url} to #{download_destination}"
        download(filebeat_url, download_destination)

        untar_all(download_destination, VENDOR_PATH) { |e| e }
      end
    end
  end
end

require 'zlib'
require 'minitar'

def untar_all(file, destination)
  Zlib::GzipReader.open(file) do |reader|
    Minitar.unpack(reader, destination)
  end
  filebeat_full_name = Dir.glob(destination + "/filebeat-*").first
  File.rename(filebeat_full_name, destination + "/filebeat")
end

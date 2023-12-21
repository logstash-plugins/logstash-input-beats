# encoding: utf-8
OS_PLATFORM = RbConfig::CONFIG["host_os"]
VENDOR_PATH = File.expand_path(File.join(File.dirname(__FILE__), "..", "..", "vendor"))

#TODO: Figure out better means to keep this version in sync
if OS_PLATFORM == "linux"
  FILEBEAT_URL = "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-7.6.0-linux-x86_64.tar.gz"
elsif OS_PLATFORM == "darwin"
  FILEBEAT_URL = "https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-7.6.0-darwin-x86_64.tar.gz"
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
        puts "Filebeat: downloading from #{FILEBEAT_URL} to #{download_destination}"
        download(FILEBEAT_URL, download_destination)

        untar_all(download_destination, File.join(VENDOR_PATH, "filebeat")) { |e| e }
      end
    end
  end
end

# Uncompress all the file from the archive this only work with 
# one level directory structure and filebeat packaging.
def untar_all(file, destination)
  untar(file) do |entry| 
    out = entry.full_name.split("/").last
    File.join(destination, out)
  end
end

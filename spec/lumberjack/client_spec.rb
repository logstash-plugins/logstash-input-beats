# encoding: utf-8
require 'spec_helper'
require 'lumberjack/client'
require 'lumberjack/server'
require "socket"
require "thread"
require "openssl"
require "zlib"

describe "Lumberjack::Client" do

  describe "Lumberjack::Socket" do

    let(:port)   { 5000 }

    subject(:socket) { Lumberjack::Socket.new(:port => port, :ssl_certificate => "" ) }

    before do
      allow_any_instance_of(Lumberjack::Socket).to receive(:connection_start).and_return(true)
      # mock any network call
      allow(socket).to receive(:send_window_size).with(kind_of(Integer)).and_return(true)
      allow(socket).to receive(:send_payload).with(kind_of(String)).and_return(true)
    end

    context "sequence" do
     let(:hash)   { {:a => 1, :b => 2}}
     let(:max_unsigned_int) { (2**32)-1 }

      before(:each) do
        allow(socket).to receive(:ack).and_return(true)
      end

      it "force sequence to be an unsigned 32 bits int" do
        socket.instance_variable_set(:@sequence, max_unsigned_int)
        socket.write_sync(hash)
        expect(socket.sequence).to eq(1)
      end
    end
  end

  describe Lumberjack::Encoder do
    it 'should creates frames without truncating accentued characters' do
      content = {
        "message" => "Le Canadien de Montréal est la meilleure équipe au monde!",
        "other" => "éléphant"
      }
      parser = Lumberjack::Parser.new
      parser.feed(Lumberjack::Encoder.to_frame(content, 0)) do |code, sequence, data|
        expect(data["message"].force_encoding('UTF-8')).to eq(content["message"])
        expect(data["other"].force_encoding('UTF-8')).to eq(content["other"])
      end
    end

    it 'should creates frames without dropping multibytes characters' do
      content = {
        "message" => "国際ホッケー連盟" # International Hockey Federation
      }
      parser = Lumberjack::Parser.new
      parser.feed(Lumberjack::Encoder.to_frame(content, 0)) do |code, sequence, data|
        expect(data["message"].force_encoding('UTF-8')).to eq(content["message"])
      end
    end
  end
end

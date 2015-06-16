require "spec_helper"
require "logstash/sized_queue_timeout"
require "flores/random"
require "stud/try"

describe "LogStash::SizedQueueTimeout" do
  let(:max_size) { Flores::Random.integer(2..100) }
  let(:element) { Flores::Random.text(0..100) }

  subject { LogStash::SizedQueueTimeout.new(max_size) }
  
  it "adds element to the queue" do
    subject << element
    expect(subject.size).to eq(1)
  end

  it "allow to pop element from the queue" do
    subject << element
    subject << "awesome"

    expect(subject.pop_no_timeout).to eq(element)
  end
  
  context "when the queue is full" do
    before do
      max_size.times { subject << element }
    end

    it "block with a timeout" do
      expect {
        subject << element
      }.to raise_error(LogStash::SizedQueueTimeout::TimeoutError)
    end

    it "unblock when we pop" do
      blocked = Thread.new do
        subject << element
      end
      sleep(0.1) until blocked.stop?

      expect(blocked.status).to eq("sleep")

      th = Thread.new do
        subject.pop_no_timeout
      end
      sleep(0.1) until th.stop?

      expect(blocked.status).to eq(false)

      blocked.join
      th.join
    end
  end

  context "when the queue is empty" do
    it "block on pop" do
      blocked = Thread.new do
        subject.pop_no_timeout
      end
      sleep(0.1) until blocked.stop?

      expect(blocked.status).to eq("sleep")

      th = Thread.new do
        subject << element
      end
      sleep(0.1) until th.stop?

      expect(blocked.status).to eq(false)
      th.join
      blocked.join
    end
  end

  context "when the queue is occupied but not full" do
    before :each do
      Flores::Random.iterations(0..max_size) { subject << "hurray" } 
    end

    it "doesnt block on pop" do
      th = Thread.new do
        subject.pop_no_timeout
      end
      sleep(0.1) until th.stop?

      expect(th.status).to eq(false)
      th.join
    end

    it "doesnt block on push" do
      th = Thread.new do
        subject << element
      end
      sleep(0.1) until th.stop?

      expect(th.status).to eq(false)
      th.join
    end
  end
end

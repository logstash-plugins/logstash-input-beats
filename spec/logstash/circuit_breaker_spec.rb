require_relative "../spec_helper"
require "logstash/circuit_breaker"

class DummyErrorTest < StandardError; end

describe LogStash::CircuitBreaker do
  let(:error_threshold) { 1 }
  let(:options) do 
    {
      :exceptions => [DummyErrorTest],
      :error_threshold => error_threshold 
    }
  end

  subject { LogStash::CircuitBreaker.new("testing", options) }


  it "closed by default" do
    expect(subject.closed?).to eq(true)
  end

  context "when having too many errors" do
    let(:future_time) { Time.now + 3600 }
    before do
      subject.execute do
        raise DummyErrorTest
      end
    end

    it "raised an exception if we have too many errors" do
      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::CircuitBreaker::OpenBreaker)
    end

    it "sets the breaker to open" do
      expect(subject.closed?).to eq(false)
    end

    it "resets the breaker after the time before retry" do
      expect(Time).to receive(:now).at_least(1).and_return(future_time)
      expect(subject.closed?).to eq(true)
    end

    it "doesnt run the command" do
      runned = false

      begin
        subject.execute do
          runned = true
        end
      rescue LogStash::CircuitBreaker::OpenBreaker
      end

      expect(runned).to eq(false)
    end
  end
end

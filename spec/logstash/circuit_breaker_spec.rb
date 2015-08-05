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

  context "when the breaker is closed" do
    it "closed by default" do
      expect(subject.closed?).to eq(true)
    end

    it "always raise an exception if an errors occur" do
      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::CircuitBreaker::HalfOpenBreaker)
    end

    it "open if we pass the errors threadshold" do
      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::CircuitBreaker::HalfOpenBreaker)

      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::CircuitBreaker::OpenBreaker)
    end
  end

  context "When the breaker is open" do
    let(:future_time) { Time.now + 3600 }

    before do
      # trip the breaker
      (error_threshold + 1).times do
        begin
          subject.execute do
            raise DummyErrorTest
          end
        rescue
        end
      end
    end

    it "#closed? should return false" do
      expect(subject.closed?).to eq(false)
    end

    it "resets the breaker after the time before retry" do
      expect(Time).to receive(:now).at_least(2).and_return(future_time)
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

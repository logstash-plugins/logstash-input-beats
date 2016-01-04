require_relative "../../spec_helper"
require "logstash/inputs/beats_support/circuit_breaker"

class DummyErrorTest < StandardError; end

describe LogStash::Inputs::BeatsSupport::CircuitBreaker do
  let(:error_threshold) { 1 }
  let(:options) do 
    {
      :exceptions => [DummyErrorTest],
      :error_threshold => error_threshold 
    }
  end

  subject { described_class.new("testing", options) }

  context "when the breaker is closed" do
    it "closed by default" do
      expect(subject.closed?).to eq(true)
    end

    it "always raise an exception if an errors occur" do
      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::Inputs::BeatsSupport::CircuitBreaker::HalfOpenBreaker)
    end

    it "open if we pass the errors threadshold" do
      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::Inputs::BeatsSupport::CircuitBreaker::HalfOpenBreaker)

      expect {
        subject.execute do
          raise DummyErrorTest
        end
      }.to raise_error(LogStash::Inputs::BeatsSupport::CircuitBreaker::OpenBreaker)
    end
  end

  context "When the breaker is open" do
    let(:retry_time) { 2 }
    let(:options) { super.merge(:time_before_retry => retry_time) }

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
      sleep(retry_time + 1)
      expect(subject.closed?).to eq(true)
    end

    it "doesnt run the command" do
      runned = false

      begin
        subject.execute do
          runned = true
        end
      rescue LogStash::Inputs::BeatsSupport::CircuitBreaker::OpenBreaker 
      end

      expect(runned).to eq(false)
    end
  end
end

# encoding: utf-8
require "flores/random"

shared_examples "send events" do
  it "successfully send the events" do
    try(25) { expect(queue.size).to eq(number_of_events), "Expected: #{number_of_events} got: #{queue.size}, execution output:\n #{@execution_output}" }
    expect(queue.collect { |e| e.get("message") }).to eq(events)
  end
end

shared_examples "doesn't send events" do
  it "doesn't send any events" do
    expect(queue.size).to eq(0), "Expected: #{number_of_events} got: #{queue.size}, execution output:\n #{@execution_output}"
  end
end

shared_context "beats configuration" do
  # common
  let(:port) { Flores::Random.port }
  let(:host) { "localhost" }

  let(:queue) { [] }
  let_tmp_file(:log_file) { events.join("\n") + "\n" } # make sure we end of line
  let(:number_of_events) { 5 }
  let(:event) { "Hello world" }
  let(:events) do
    events = []
    number_of_events.times { |n| events << "#{event} #{n}" }
    events
  end

  let(:input_config) do
    {
      "host" => host,
      "port" => port
    }
  end

  let(:beats) do
    LogStash::Inputs::Beats.new(input_config)
  end

  before :each do
    beats.register

    @abort_on_exception = Thread.abort_on_exception
    Thread.abort_on_exception = true

    @server = Thread.new do
      begin
        beats.run(queue)
      rescue => e
        retry unless beats.stop?
      end
    end
    @server.abort_on_exception = true

    sleep(1) while @server.status != "run"
  end

  after(:each) do
    beats.stop
    Thread.abort_on_exception = @abort_on_exception
  end
end

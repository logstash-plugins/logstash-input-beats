# encoding: utf-8
shared_examples "Common Event Transformation" do
  let(:tag) { "140-rpm-beats" }
  let(:config) do
    {
      "port" => 0,
      "type" => "example",
      "tags" => tag
    }
  end

  let(:input) do
    LogStash::Inputs::Beats.new(config).tap do |i|
      i.register
    end
  end
  let(:event) { LogStash::Event.new(event_map) }
  let(:event_map) do
    {
      "message" => "Hello world",
    }
  end

  it "adds configured tags to the event" do
    expect(subject.get("tags")).to include(tag)
  end

  context "when the `beats.hostname` doesnt exist on the event" do
    let(:already_exist) { "already_exist" }
    let(:event_map) { super.merge({ "host" => already_exist }) }

    it "doesnt change the value" do
      expect(subject.get("host")).to eq(already_exist)
    end
  end

  context "when the `beat.hostname` exist in the event" do
    let(:producer_host) { "newhost01" }
    let(:event_map) { super.merge({ "beat" => { "hostname" => producer_host }}) }

    context "when `host` key doesn't exist on the event" do
      it "copy the `beat.hostname` to `host` or backward compatibility" do
        expect(subject.get("host")).to eq(producer_host)
      end
    end

    context "when `host` key exists on the event" do
      let(:already_exist) { "already_exist" }
      let(:event_map) { super.merge({ "host" => already_exist }) }

      it "doesn't override it" do
        expect(subject.get("host")).to eq(already_exist)
      end
    end
  end
end

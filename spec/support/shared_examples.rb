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

  context 'when add_hostname is true' do
    let(:config) { super.merge({'add_hostname' => true})}

    context 'when a host is provided in beat.host.name' do
      let(:already_exist) { "already_exist" }
      let(:producer_host) { "newhost01" }
      let(:event_map) { super.merge({ "beat" => { "host" => {"name" => producer_host }}}) }

      context "when no `host` key already exists on the event" do
        it "does not set the host value" do
          expect(subject.get("host")).to be_nil
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

    context "when a host is set in `beat.hostname`" do
      let(:producer_host) { "newhost01" }
      let(:event_map) { super.merge({ "beat" => { "hostname" => producer_host }}) }

      context "when no `host` key already exists on the event" do
        it "copies the value in `beat.hostname` to `host`" do
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

    context "when no host is provided in beat" do
      context "when no `host` key already exists on the event" do
        it "does not set the host" do
          expect(subject.get("host")).to be_nil
        end
      end

      context "when `host` key already exists on the event" do
        let(:already_exist) { "already_exist" }
        let(:event_map) { super.merge({ "host" => already_exist }) }

        it "doesn't override it" do
          expect(subject.get("host")).to eq(already_exist)
        end
      end
    end
  end

  context 'when add hostname is false' do
    let(:config) { super.merge({'add_hostname' => false})}

    context 'when a host is provided in beat.host.name' do
      let(:already_exist) { "already_exist" }
      let(:producer_host) { "newhost01" }
      let(:event_map) { super.merge({ "beat" => { "host" => {"name" => producer_host }}}) }

      context "when no `host` key already exists on the event" do
        it "does not set the host" do
          expect(subject.get("host")).to be_nil
        end
      end

      context "when `host` key already exists on the event" do
        let(:already_exist) { "already_exist" }
        let(:event_map) { super.merge({ "host" => already_exist }) }

        it "doesn't override it" do
          expect(subject.get("host")).to eq(already_exist)
        end
      end
    end

    context "when a host is provided in `beat.hostname`" do
      let(:producer_host) { "newhost01" }
      let(:event_map) { super.merge({ "beat" => { "hostname" => producer_host }}) }

      context "when no `host` key already exists on the event" do
        it "does not set the host" do
          expect(subject.get("host")).to be_nil
        end
      end

      context "when `host` key already exists on the event" do
        let(:already_exist) { "already_exist" }
        let(:event_map) { super.merge({ "host" => already_exist }) }

        it "doesn't override it" do
          expect(subject.get("host")).to eq(already_exist)
        end
      end
    end

    context "when no host is provided in beat" do
      context "when no `host` key already exists on the event" do
        it "does not set the host" do
          expect(subject.get("host")).to be_nil
        end
      end

      context "when `host` key already exists on the event" do
        let(:already_exist) { "already_exist" }
        let(:event_map) { super.merge({ "host" => already_exist }) }

        it "doesn't override it" do
          expect(subject.get("host")).to eq(already_exist)
        end
      end
    end
  end
end

# encoding: utf-8
require "logstash/inputs/beats/tls"

describe LogStash::Inputs::Beats::TLS do
  subject { described_class }

  it "returns the minimum supported tls" do
    expect(subject.min.version).to eq(1)
    expect(subject.min.name).to eq("TLSv1")
  end

  it "returns the maximum supported tls" do
    expect(subject.max.version).to eq(1.2)
    expect(subject.max.name).to eq("TLSv1.2")
  end

  describe ".get_supported" do
    context "when a range is given" do
      it "returns the list of compatible TLS from a range" do
        expect(subject.get_supported((1.1)..(1.2)).map(&:version)).to match([1.1, 1.2])
      end

      it "it return an empty array when nothing match" do
        expect(subject.get_supported((3.1)..(8.2))).to be_empty
      end
    end

    context "when a scalar is given" do
      it "when a scalar is given we return the compatible value" do
        expect(subject.get_supported(1.1).map(&:version)).to match([1.1])
      end


      it "it return an empty array when nothing match" do
        expect(subject.get_supported(9)).to be_empty
      end
    end
  end
end

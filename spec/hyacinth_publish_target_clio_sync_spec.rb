require './hyacinth_publish_target_clio_sync'

describe HyacinthPublishTargetClioSync do
  let(:test_class) { Class.new.send :include, HyacinthPublishTargetClioSync }
  let(:marc_record) { MARC::Record.new_from_marc(File.read('spec/fixtures/13534401.marc')) }
  let(:expected) { JSON.load(File.read('spec/fixtures/13534401.json')) }
  let(:actual) { subject.marc_to_digital_object_data(marc_record) }
  subject { test_class.new }
  it "produces digital object data" do
    expect(JSON.load(actual.to_json)).to eql expected
  end
end
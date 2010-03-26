require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

CONFIG = {
  :database => "redmiso_test",
  :user => 'howard',
  :password => nil,
  :host => 'localhost'
}

db_name = CONFIG.delete(:database)
DB = Sequel.postgres(db_name, CONFIG)

def db_reset
  DB.tables.each do |table|
    DB.drop_table table
  end
end

describe "Redmiso::Dataset::Map" do
  before do
    # drop all tables
    db_reset
    @name = "redmiso_test"
    @map = Redmiso::Dataset::Map.new(@name,DB)
  end

  context "table definition" do
    context "ensure_table" do
      it "creates table if not created" do
        @map.ensure_table
        DB.tables.should include(@map.name)
      end

      it "is indempotent if table is created" do
        @map.ensure_table
        @map.ensure_table
        DB.tables.should include(@map.name)
      end
    end

    it "raises if name is already used" do
      @map.create_table
      lambda { @map.create_table }.should raise_error(Sequel::DatabaseError)
    end

    it "creates table" do
      @map.create_table
      schema = DB.schema(@map.name).inject({}) do |h,(name,info)|
        h[name] = info
        h
      end
      schema.should ==
        {:added_id => {
          :type => :integer,
          :allow_null => false,
          :default => "nextval('redmiso_test_added_id_seq'::regclass)",
          :ruby_default => nil,
          :primary_key => true,
          :db_type => "integer"},
        :created_at =>  {
          :type => :datetime,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "timestamp without time zone"},
        :updated_at => {
          :type => :datetime,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "timestamp without time zone"},
        :data => {
          :type => :blob,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "bytea"},
        :id => {
          :type => :blob,
          :allow_null => false,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "bytea"}}
      DB.indexes(@map.name).should == {
        :redmiso_test_id_key => {
          :unique => true, :columns => [:id]
        }}
    end
  end

  context "CRUD" do
    before do
      @map.create_table
    end

    def put(i)
      @map.put(i.to_s,i)
    end

    context "#put" do
      it "returns a record" do
        put(1)
        @map.table.count.should == 1
        result = @map.get("1")
        result[:id].should == "1"
        result[:data].should == 1
        result[:created_at].should be_a(Time)
      end

      it "raises if putting twice on same id" do
        put(1)
        lambda { @map.put("1",2) }.should raise_error(Redmiso::DuplicateID)
      end
    end

    context "#get" do
      it "has no record to return" do
        @map.get("1").should be_nil
      end

      it "has no record to return" do
        put(1)
        @map.get("1").should_not be_nil
      end
    end

    context "#delete" do
      it "deletes nothing" do
        @map.delete("1").should == 0
      end

      it "deletes" do
        put(1)
        @map.get("1").should_not be_nil
        @map.delete("1").should == 1
        @map.get("1").should be_nil
      end
    end

    context "#set" do
      it "raises if setting non-existing record" do
        lambda { @map.set("1",1) }.should raise_error(Redmiso::NotFound)
      end

      it "sets data with given value" do
        put(1)
        @map.set("1",2).should == 1
        result = @map.get("1")
        result[:updated_at].should be_a(Time)
        result[:data].should == 2
      end

      it "sets data by yielding old value to a block" do
        put(1)
        @map.set("1") { |n|
          n + 1
        }
        @map.get("1")[:data].should == 2
      end

      it "sets data with block with exclusive row lock" do
        put(0); put(1)

        q1 = Queue.new
        test = Queue.new
        t1 = Thread.new do
          @map.set("1") { |n|
            test << :ready
            q1.pop.should == :go
            2
          }
          test << :done
        end
        t1.abort_on_exception = true
        test.pop.should == :ready

        # selecting from unlocked row
        @map.get("0").should_not be_nil

        # selecting from locked row
        begin
          DB.fetch("SELECT * FROM #{@map.name} WHERE id = ?","1").first
        rescue Sequel::DatabaseError => e
          e.to_s.chomp.should ==
            'PGError: ERROR:  could not obtain lock on row in relation "test_set"'
        end
        q1 << :go
        test.pop.should == :done
        result = @map.get("1")
        result.should_not be_nil
        result[:data].should == 2
      end
    end
  end
end

describe "Redmiso::Dataset::Bag" do
  before do
    # drop all tables
    db_reset
    @name = "redmiso_test"
    @bag = Redmiso::Dataset::Bag.new(@name,DB)
    @bag.create_table
  end

  context "table definition" do
    it "uses the right schema" do
      schema = DB.schema(@bag.name).inject({}) do |h,(name,info)|
        h[name] = info
        h
      end
      schema.should ==
        {:added_id => {
          :type => :integer,
          :allow_null => false,
          :default => "nextval('redmiso_test_added_id_seq'::regclass)",
          :ruby_default => nil,
          :primary_key => true,
          :db_type => "integer"},
        :created_at =>  {
          :type => :datetime,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "timestamp without time zone"},
        :updated_at => {
          :type => :datetime,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "timestamp without time zone"},
        :data => {
          :type => :blob,
          :allow_null => true,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "bytea"},
        :id => {
          :type => :blob,
          :allow_null => false,
          :default => nil,
          :ruby_default => nil,
          :primary_key => false,
          :db_type => "bytea"}}
      DB.indexes(@bag.name).should == {
        :redmiso_test_id_index => {
          :unique => false, :columns => [:id]
        }}
    end
  end

  it "gets empty array if nothing had been associated with key" do
    @bag.get_all("1").should be_empty
  end

  it "puts multiple items under same key" do
    @bag.put("1",1)
    @bag.put("1",2)
    results = @bag.get_all("1")
    results.should have(2).results
    results.map{ |row| row[:data] }.should include(1,2)
    results.map{ |row| row[:id] }.should == ["1","1"]
  end

  it "deletes nothing if nothing had been associated with key" do
    @bag.delete("1").should == 0
  end

  it "deletes all items under a key" do
    @bag.put("1",1)
    @bag.put("1",2)
    @bag.delete("1").should == 2
  end

  it "sets all items under a key to the same value" do
    @bag.put("1",1)
    @bag.put("1",2)
    @bag.set("1",0).should == 2
    @bag.get_all("1").map { |row| row[:data] }.should == [0,0]
  end

  it "sets all items under a key to block's value" do
    @bag.put("1",1)
    @bag.put("1",2)
    @bag.set("1") { |n| n + 10 }.should == 2
    @bag.get_all("1").map { |row| row[:data] }.should include(11,12)
  end
end

context "Redmiso::Base" do
  before do
    db_reset
    @testmiso = Class.new(Redmiso::Base) do
      map = Redmiso::Dataset::Map.new("test_miso",DB)
      map.ensure_table
      use Redmiso::Storage::Basic.new(map)
    end
  end

  def data
    {"a" => 1}
  end
  it ".put" do
    miso = @testmiso.put("1",data)
    miso.should be_a(@testmiso)
    miso.attributes.should == data
  end

  it "#save" do
    miso = @testmiso.put("1",data)
    miso["b"] = 2
    miso["c"] = 3
    miso.attributes.delete "a"
    miso.save.should be_true
    miso.reload
    miso.attributes.should == {
      "b" => 2,
      "c" => 3
    }
    miso.updated_at.should be_a(Time)
  end

  it "#reload" do
    miso = @testmiso.put("1",data)
    old_attributes = miso.attributes.clone
    miso["b"] = 2
    miso.reload
    miso.attributes.should == old_attributes
  end
  
  it "#update" do
    miso = @testmiso.put("1",data)
    miso.update {
      miso["b"] = 2
      miso["c"] = 3
      miso.attributes.delete("a")
    }.should be_true
    miso.reload
    miso.attributes.should == {
      "b" => 2,
      "c" => 3,
    }
    miso.updated_at.should be_a(Time)
  end
end


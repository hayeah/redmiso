require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'redis/raketasks'
#require 'logger'


describe "Redmiso" do
  before(:all) do
    result = RedisRunner.start_detached
    raise("Could not start redis-server, aborting") unless result

    # yea, this sucks, but it seems like sometimes we try to connect too quickly w/o it
    sleep 1

    # use database 15 for testing so we dont accidentally step on you real data
    TestMiso.default = { :db => 15 }
    @r = TestMiso.redis
    raise("spec runs on database 15, but it's not empty") unless @r.dbsize == 0
    
  end

  before(:each) do
    @r['foo'] = 'bar'
  end

  after(:each) do
    @r.keys('*').each {|k|
      @r.del k
    }
  end

  after(:all) do
    begin
      @r.flushdb
      @r.save
      @r.quit
    ensure
      RedisRunner.stop
    end
  end

  context "Field" do
    before(:all) do
      @owner = nil
      @redis = @r
    end

    context "String" do
      before(:each) do
        @f = Redmiso::Field::String.new(@owner,@r,"string")
      end

      it "loads string into string" do
        s = "abc"
        @f.load("abc").should == s
      end

      it "returns nil if key not found" do
        @f.get.should == nil
      end

      it "dumps to string" do
        @f.dump(10).should == "10"
      end

      it "sets and gets string" do
        @f.set("abc")
        @f.get.should == "abc"
        @f.get.should == @f.value
      end

      it "getsets string" do
        @f.set('a')
        old_value = @f.getset("b")
        old_value.should == "a"
        @f.value.should == "b"
      end

      it "memoizes value" do
        @f.set("abc")
        @f.value.should == "abc"
        @redis["string"] = "cba"
        @f.value.should == "abc"
        @f.get == "cba"
      end

      it "resets memoized value" do
        @f.set("abc")
        @f.value.should == "abc"
        @redis["string"] = "cba"
        @f.value.should == "abc"
        @f.get.should == "cba"
        @f.reset.value.should == "cba"
      end
    end

    context "Integer" do
      before(:each) do
        @f = Redmiso::Field::Integer.new(@owner,@r,"integer")
      end

      it "loads integer" do
        @f.load(10).should == 10
        @f.load("10").should == 10
      end

      it "sets and gets integer" do
        @f.set("10")
        @f.get.should == 10
        @f.set(11)
        @f.get.should == 11
      end

      it "increments" do
        @f.incr.should == 1
        @f.incr.should == 2
        @f.value.should == 2
      end

      it "decrements" do
        @f.decr.should == -1
        @f.decr.should == -2
        @f.value.should == -2
      end
    end

    context "List" do
      before(:each) do
        @f = Redmiso::Field::List(Redmiso::Field::Type::Integer).new(@owner,@r,"list[integer]")
      end

      it "uses type" do
        @f.type == Redmiso::Field::Type::Integer
      end

      it "lpushes and lpops elements" do
        @f.lpush("1")
        @f.lpush(2)
        @f.lpop.should == 2
        @f.lpop.should == 1
      end

      it "rpushes and rpops elements" do
        @f.rpush("1")
        @f.rpush(2)
        @f.rpop.should == 2
        @f.rpop.should == 1
      end

      it "has length" do
        @f.push(1)
        @f.push(2)
        @f.length.should == 2
        @f.pop.should == 2
        @f.length.should == 1
      end

      it "accesses by index" do
        @f.push(0)
        @f.push(1)
        @f.push(2)
        @f.push(3)
        @f[0].should == 0
        @f[2].should == 2
        @f[-1].should == 3
        @f[0..1].should == [0,1]
        @f[0..-1].should == [0,1,2,3]
        @f[0..100].should == @f[0..-1]
      end

      it "iterates through elements" do
        @f.push(1)
        @f.push(2)
        @f.push(3)
        @f.map { |i| i+1 }.sort == [2,3,4]
      end
    end

    context "Set" do
      def set(type=Redmiso::Field::Type::Integer)
        Redmiso::Field::Set(type).new(@owner,@redis,"Set[integer]#{rand}")
      end

      before(:each) do
        @f = set
      end

      it "has type" do
        @f.type == Redmiso::Field::Type::Integer
      end

      it "adds element" do
        @f.add(1)
        @f.add(1)
        @f.add(1)
        @f.add(2)
        @f.add(2)
        @f.value.should == [1,2]
      end

      it "has size" do
        @f.add(1)
        @f.add(1)
        @f.add(2)
        @f.size.should == 2
      end

      it "pops random element" do
        @f.add(1)
        @f.pop.should == 1
        @f.should be_empty
      end

      it "returns a random element" do
        @f.add(1)
        @f.random.should == 1
        @f.size.should == 1
      end

      it "removes" do
        @f.add(1)
        @f.add(2)
        @f.delete(1)
        @f.value.should == [2]
      end

      it "checks membership" do
        @f.include?(1).should == false
        @f.add(1)
        @f.include?(1).should == true
      end

      it "moves element" do
        @f.add(1)
        @f.add(2)

        @f2 = set
        @f2.add(1)

        @f.move(@f2,1)
        @f.move(@f2,2)

        @f2.value.sort.should == [1,2]
        @f.should be_empty
      end

      it "moves element into itself does nothing" do
        @f.add(1)
        @f.move(@f,1)
        @f.value == [1]
      end

      it "raises when moving element between unequal types" do
        @f_incompatible = set(Redmiso::Field::Type::String)
        lambda { @f.move(@f_incompatible,1) }.should raise_error
      end

      it "iterates through elements" do
        @f.add(1)
        @f.add(2)
        @f.add(3)
        @f.map { |i| i+1 }.sort == [2,3,4]
      end

      it "unions" do
        @f1, @f2, @f3 = set, set, set
        @f1.add(1);@f1.add(2)
        @f2.add(2);@f2.add(3)
        @f3.add(3);@f3.add(4)
        @f1.union(@f2,@f3).sort.should == [1,2,3,4]
      end

      it "empty unions" do
        @f.union.should == []
        @f.union(@f,@f).should == []
      end

      it "unions then stores" do
        @f.add(0) # this will disappear
        @f1, @f2, @f3 = set, set, set
        @f1.add(1);@f1.add(2)
        @f2.add(2);@f2.add(3)
        @f3.add(3);@f3.add(4)
        @f.union_store(@f1,@f2,@f3)
        @f.value.sort.should == [1,2,3,4]
      end

      it "intersects" do
        @f1, @f2 = set, set
        @f1.add(1);@f1.add(2)
        @f2.add(2);@f2.add(3)
        @f1.intersect(@f2).sort.should == [2]
      end

      it "intersects then stores" do
        @f1, @f2 = set, set
        @f1.add(1);@f1.add(2)
        @f2.add(2);@f2.add(3)
        @f.intersect_store(@f1, @f2)
        @f.value.sort.should == [2]
      end

      it "diffs" do
        @f1, @f2 = set, set
        @f1.add(1);@f1.add(2);@f1.add(3)
        @f2.add(1);@f2.add(3)
        @f1.difference(@f2).should == [2]
      end

      it "diffs then stores" do
        @f1, @f2 = set, set
        @f1.add(1);@f1.add(2);@f1.add(3)
        @f2.add(1);@f2.add(3)
        @f.difference_store(@f1,@f2)
        @f.value.should == [2]
      end
    end
  end

  
  context "TestMiso" do
    class TestMiso < Redmiso
      c_string(:a_class_string)
      string(:a_string)
      string(:a_stripped_string) do
        def dump(str)
          str.strip
        end

        def load(str)
          str.strip
        end
      end

      list(:string,:list_of_strings)
      set(:string,:set_of_strings)
    end
    
    context "creation" do
      it "checks existence" do
        TestMiso.exist?(10).should == false
        o = TestMiso.create(10)
        TestMiso.exist?(10).should == true
        o.exist?.should == true
      end

      it "creates" do
        o = TestMiso.create(10)
        o.should be_a(TestMiso)
        TestMiso.exist?(10).should == true
      end

      it "creates with auto increment" do
        o = TestMiso.create
        o.id.should == 1
        o = TestMiso.create
        o.id.should == 2
      end

      it "raises when create with duplicate id" do
        TestMiso.create(10)
        lambda { TestMiso.create(10) }.should raise_error
      end

      it "counts the root set" do
        TestMiso.create
        TestMiso.create
        TestMiso.count.should == 2
      end

      it "gets all objects from root set" do
        o = TestMiso.create
        TestMiso.all.should == [o]
      end
      
      it "deletes from root set" do
        o = TestMiso.create
        o.exist?.should == true
        o.delete
        o.exist?.should == false
      end

      it "returns nil cannot find object" do
        TestMiso[10].should be_nil
      end

      it "finds object" do
        TestMiso.create(10)
        o = TestMiso[10].should be_a(TestMiso)
      end
      
    end

    before(:each) do
      @o = TestMiso.new(10)
    end
    
    it "defines a string field" do
      f = @o.a_string!
      f.should be_a(Redmiso::Field::String)
      f.key.should == "TestMiso[10].a_string"
    end

    it "unset field returns nil" do
      @o.a_string.should be_nil
    end

    it "set field returns nil" do
      @o.a_string = "abc"
      @o.a_string.should == "abc"
      @o.a_string = "cba"
      @o.a_string.should == "cba"
    end

    it "evaluates block in field" do
      @o.a_string { set("abc") }
      @o.a_string { [value,value*2] }.should == ["abc","abcabc"]
    end

    it "defines a specialized stripped string field" do
      f = @o.a_stripped_string!
      f.class.superclass.should == Redmiso::Field::String
      @o.a_stripped_string = "  abc  "
      @o.a_stripped_string.should == "abc"
      @o.a_stripped_string{get} == "abc"
    end

    it "defines a class string" do
      f = TestMiso.a_class_string!
      f.should be_a(Redmiso::Field::String)
      f.key.should == "TestMiso#a_class_string"
      TestMiso.a_class_string.should == nil
      TestMiso.a_class_string = "abcd"
      TestMiso.a_class_string.should == "abcd"
      TestMiso.redis[f.key].should == "abcd"
    end

    context "Collection" do
      context "List" do
        it "has a list" do
          f = @o.list_of_strings!
          f.class.superclass.should == Redmiso::Field::List
          f.type.should == Redmiso::Field::String
        end
      end

      context "Set" do
        it "has a set" do
          f = @o.set_of_strings!
          f.should be_a(Redmiso::Field::Set) 
          f.type.should == Redmiso::Field::String
        end
      end
      
    end
  end

  
#   context "ID Set" do
    

#     it "creates with auto increment"
    
#   end
  
  # it "fails" do
#     fail "hey buddy, you should probably rename this file and start specing for real"
#   end
  
end

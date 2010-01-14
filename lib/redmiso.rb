require 'redis'
class Redmiso
end

require 'redmiso/type'

class Redmiso
  class << self
    attr_accessor :default

    def redis
      # use Redis default by default
      @redis ||= Redis.new(default || {})
      @redis
    end

    def instance_fields
      @instance_fields ||= {}
      @instance_fields
    end

    def class_fields
      @class_fields ||= {}
      @class_fields
    end

    BASIC_TYPES = {
      :string => Redmiso::Field::String,
      :integer => Redmiso::Field::Integer,
    }
    BASIC_TYPES.each do |method_name,field_type|
      # declaration method for instance fields
      define_method(method_name) do |field_name,&block|
        define_instance_field(field_type,field_name,&block)
      end
      # declaration method for class fields
      define_method("c_#{method_name}") do |field_name,&block|
        define_class_field(field_type,field_name,&block)
      end
    end

    COLLECTION_TYPES = {
      :list => Redmiso::Field::List,
      :set  => Redmiso::Field::Set,
      :zset => Redmiso::Field::ZSet,
    }

    COLLECTION_TYPES.each do |method_name,collection_type|
      build_subtype = lambda { |collection_type,element_type,&block|
        # produce a parameterized subtype
        subtype = Class.new(collection_type)
        element_type = BASIC_TYPES[element_type] if element_type.is_a?(Symbol)
        raise "not a valid basic type: #{element_type}" unless element_type.ancestors.include?(Redmiso::Field::String)
        subtype.type(element_type) # parameterize the type
        subtype.class_eval(&block) if block
        subtype
      }
      define_method(method_name) do |element_type,field_name,&block|
        define_instance_field(build_subtype.call(collection_type,element_type),field_name,&block)
      end

      define_method("c_#{method_name}") do |element_type,field_name,&block|
        define_class_field(build_subtype.call(collection_type,element_type),field_name,&block)
      end
    end
    
    def exist?(id)
      r = redis.sismember(id_set,id)
      r
    end

    def create(id=nil)
      id ||= redis.incr(auto_increment)
      unless redis.sadd(id_set,id)
        raise "can't create with existing id: #{id}"
      end
      self.new(id)
    end

    def delete(id)
      if redis.srem(id_set,id) == 0
        raise "can't delete id: #{id}"
      end
    end

    def ids
      redis.smembers(id_set).map(&:to_i)
    end

    def all
      ids.map { |id| self.new(id) }
    end

    def count
      redis.scard(id_set)
    end

    def [](id)
      self.new(id) if exist?(id)
    end

    def vacuum
      # clean up everything not in root set
      raise "abstract"
      redis.keys("#{self}.*")
      # then extrat ids of fields, and check existence in rootset
    end

    def garbage
      # return attributes of objects not in rootset
    end

    def fields
      class_fields
    end

    private

    def get_field(name)
      fields[name].new(self,redis,"#{self}##{name}")
    end

    def define_class_field(type,name,&block)
      define_field(:class,type,name,&block)
    end
    
    def define_instance_field(type,name,&block)
      define_field(:instance,type,name,&block)
    end

    def define_field(on_what,type,name,&block)
      if block
        field = Class.new(type,&block)
      else
        field = type
      end
      case on_what
      when :class
        fs = class_fields
      when :instance
        fs = instance_fields
      end
      fs[name] = field
      # the following happens to work for both
      # class and instance. "get_field" is either
      # the class or the instance "get_field"
      # depending on who's calling it.
      src=<<-HERE
def #{name}!(&block)
  @#{name} ||= get_field(:#{name})
  if block
    @#{name}.instance_eval(&block)
  else
    @#{name}
  end
end

def #{name}(&block)
  if block
    #{name}!(&block)
  else
    #{name}!.value
  end
end

def #{name}=(v)
  #{name}!.set(v)
end
HERE
      case on_what
      when :class
        c = class << self; self; end
      when :instance
        c = self
      end
      c.class_eval(src,__FILE__,__LINE__)
    end
    
    # this is the ROOT for this class
    def id_set
      @id_set ||= "#{self}#ids"
      @id_set
    end

    def auto_increment
      @auto_increment ||= "#{self}#auto_increment"
      @auto_increment
    end
  end

  def redis
    self.class.redis
  end

  attr_reader :id
  def initialize(id)
    @id = id
  end

  def fields
    self.class.instance_fields
  end

  def delete
    self.class.delete(id)
  end

  def exist?
    self.class.exist?(id)
  end

  def ==(obj)
    obj.class == self.class && obj.id == self.id
  end

  private

  def get_field(name)
    fields[name].new(self,redis,"#{self.class}[#{self.id}].#{name}")
  end
end





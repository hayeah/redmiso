class Redmiso::Field
  module Type
    extend self
    def load(str) # => Object
      raise "abstract"
    end
    def dump(obj) # => String
      raise "abstract"
    end

    module String
      extend self
      def load(str)
        str.to_s
      end

      def dump(o)
        o.to_s
      end
    end

    module Integer
      extend self
      def load(str)
        # returns nil if is nil
        str && Integer(str)
      end

      def dump(i)
        i.to_i.to_s
      end
    end
  end
  
  class << self
    def type(t=nil)
      if t
        @type = t
      else
        @type
      end
    end

    def load(str)
      type.load(str)
    end

    def dump(o)
      type.dump(o)
    end
  end

  class << self
    attr_accessor :type
    def type(element_type=nil) # the type of the elements
      @type = element_type if element_type # set element type
      @type
    end
  end

  def type
    self.class.type
  end
  
  def load(str)
    self.class.load(str)
  end

  def dump(o)
    self.class.dump(o)
  end
  
  attr_reader :owner, :redis, :key
  def initialize(owner,redis,key)
    # owner is the object this attribute belongs to
    @owner = owner
    @redis = redis
    @key = key
  end

  def get_value
    raise "abstract"
  end

  def value
    return @value if defined?(@value)
    @value = get_value
  end

  def reset
    remove_instance_variable("@value") if defined?(@value)
    self
  end

  def refresh
    reset
    value
  end

  class Collection < self
    # abstract class for Set, List, ZSet
    # enumerable
    include Enumerable
    def each(&block)
      get_value.each(&block)
    end
  end
  
  class Set < Collection
    def sadd(obj)
      redis.sadd(key,dump(obj))
    end
    alias :add :sadd
    alias :<<  :sadd

    def scard
      redis.scard(key)
    end
    alias :size :scard
    alias :length :scard

    def empty?
      scard == 0
    end

    def spop
      load(redis.spop(key))
    end
    alias :pop :spop

    def srem(obj)
      redis.srem(key,dump(obj))
    end
    alias :delete :srem

    def sismember(obj)
      redis.sismember(key,dump(obj))
    end
    alias :include? :sismember
    alias :member? :sismember

    def smove(dest,obj)
      check_types(self,dest)
      redis.smove(key,dest.key,dump(obj))
    end
    alias :move :smove

    def srandmember
      load(redis.srandmember(key))
    end
    alias :random srandmember

    def sunion(*sets)
      return [] if sets.empty?
      check_types(self,*sets)
      redis.sunion([self,*sets].map(&:key)).map { |o| load(o) }
    end
    alias :union :sunion

    def sunionstore(*sets)
      check_types(self,*sets)
      redis.sunionstore(key,*sets.map(&:key))
    end
    alias :union_store :sunionstore

    def sinter(*sets)
      return [] if sets.empty?
      check_types(self,*sets)
      redis.sinter([self,*sets].map(&:key)).map { |o| load(o) }
    end
    alias :intersect :sinter

    def sinterstore(*sets)
      check_types(self,*sets)
      redis.sinterstore(key,*sets.map(&:key))
    end
    alias :intersect_store :sinterstore

    def sdiff(*sets)
      return [] if sets.empty?
      check_types(self,*sets)
      redis.sdiff([self,*sets].map(&:key)).map { |o| load(o) }
    end
    alias :difference :sdiff

    def sdiffstore(*sets)
      check_types(self,*sets)
      redis.sdiffstore(key,*sets.map(&:key))
    end
    alias :difference_store :sdiffstore

    def get_value
      redis.smembers(key).map { |o| load(o) }
    end

    protected
    def check_types(*sets)
      sets.each { |s|
        raise "incompatible types" unless s.type == sets.first.type
      }
      true
    end
  end

  def self.Set(type)
    c = Class.new(Set)
    c.type(type)
    c
  end

  class List < Collection
    
    def rpop
      load(redis.rpop(key))
    end
    alias :pop :rpop
    
    def lpop
      load(redis.lpop(key))
    end
    alias :shift :lpop
    
    def rpush(o)
      redis.rpush(key,dump(o))
    end
    alias :push :rpush

    def lpush(o)
      redis.lpush(key,dump(o))
    end
    alias :unshift :lpush

    def length
      redis.llen(key)
    end
    alias :size :length

    def [](idx)
      case idx
      when Integer, Fixnum
        load(redis.lindex(key,idx))
      when Range
        redis.lrange(key,idx.begin,idx.end).map { |o| load(o) }
      end
    end

    def get_value
      self[0..-1].map { |o| load(o) }
    end
  end

  def self.List(type)
    c = Class.new(List)
    c.type(type)
    c
  end

  class ZSet < Collection
    
  end

  SortedSet = ZSet

  class Boolean < self
    
  end

#   class Mutex
#   end

#   class ConditionVariable
#   end

  class String < self
    type self::Type::String

    # object should synchronize the latest @value
    def get
      if r=redis[key]
        @value = load(r)
      end
    end

    def set(obj)
      v = dump(obj)
      redis[key] = v
      reset
      @value = load(v)
    end

    def getset(obj)
      v = dump(obj)
      r = redis.getset(key,v)
      @value = load(v)
      return load(r)
    end

    def get_value
      get
    end
  end

  class Integer < String
    type self::Type::Integer
    def incr(n=1)
      if n==1
        redis.incr(key)
      else
        redis.incrby(key,n.abs)
      end
    end

    def decr(n=1)
      if n==1
        redis.decr(key)
      else
        redis.decrby(key,n.abs)
      end
    end
  end
end

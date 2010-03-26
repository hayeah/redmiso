
class Redmiso::Base
  include Redmiso
  class << self
    attr_reader :storage
    def use(storage)
      @storage = storage
    end

    def put(id,attributes={})
      raise TypeError, "Expects hash" unless Hash === attributes
      storage.put(id,attributes)
      get(id)
    end

    def get(id)
      self.new(id,storage.get(id))
    end
  end

  def storage
    self.class.storage
  end

  attr_reader :id, :attributes
  attr_reader :created_at, :updated_at
  def initialize(id,row)
    @id = id
    @attributes = row[:data]
    @created_at = row[:created_at]
    @updated_at = row[:updated_at]
  end

  def [](key)
    attributes[key]
  end

  def []=(key,value)
    attributes[key] = value
  end

  def reload
    row = storage.get(id)
    initialize(id,row)
    self
  end

  def update
    storage.set(id) do |attributes|
      @attributes = attributes
      yield
      @attributes
    end > 0
  end

  def save
    storage.set(id,attributes) > 0
  end
end

class Redmiso::Dataset
  require 'bert'
  
  class << self
    def default_schema
      raise "abstract"
    end
  end

  def ensure_table
    create_table unless database.table_exists?(name)
  end

  def create_table(&block)
    database.create_table(name,&(block || self.class.default_schema))
  end
  
  attr_reader :name, :database, :table
  def initialize(name,database)
    @name = name.to_sym
    @database = database
    @table = database.from(name)
  end
  
  def escape_bytea(bytes)
    database.synchronize do |pg|
      pg.escape_bytea(bytes)
    end
  end

  def unescape_bytea(bytes)
    database.synchronize do |pg|
      pg.unescape_bytea(bytes)
    end
  end

  def decode(bytes)
    BERT.decode(bytes)
  end

  def encode(object)
    escape_bytea(BERT.encode(object))
  end
  

  def transaction(&block)
    database.transaction(&block)
  end

  def delete(id)
    finder(id).delete
  end

  def put(id,data)
    begin
      table.insert([:id,:data,:created_at],
                   [id,encode(data),Time.now])
    rescue Sequel::DatabaseError 
      if $!.message =~ /^PGError: ERROR:  duplicate key value violates unique constraint/
        raise Redmiso::DuplicateID, id
      else
        raise $!
      end
    end
  end

  def get(id)
    row = finder(id).first
    return unless row
    row[:data] = decode(row[:data])
    row
  end

  def get_all(id)
    finder(id).all.map { |row|
      row[:data] = decode(row[:data])
      row
    }
  end

  def set(id,data=nil,&block)
    n_changed =
      if block
        update_with_block(id,&block)
      else
        update_with_data(id,data)
      end
    raise Redmiso::NotFound, id unless n_changed > 0
    n_changed
  end

  protected
  
  def update_with_data(id,data)
    finder(id).update(:data => encode(data),:updated_at => Time.now)
  end

  # WARNING: This locks the row being changed. So update should happen
  # as fast as possible.
  #
  # NOTE: There's no deadlock by design as long as we never acquire
  # more than one locked row per transaction. We can't have a
  # transaction over two ids anyway, because we might have sharded
  # storage.
  def update_with_block(id,&block)
    database.transaction do
      select = finder(id)
      count = 0
      select.for_update.each do |row|
        data2 = yield(decode(row[:data]))
        r = finder(id).where(:added_id => row[:added_id]).
          update(:data => encode(data2),:updated_at => Time.now)
        count += r
      end
      return count
    end
  end
  
  def finder(id)
    table.where(:id => id)
  end

  # No updating of elements in bag.
  class Bag < self
    def self.default_schema
      lambda {
        primary_key :added_id
        bytea   :id, :null => false
        bytea   :data
        Time :created_at
        Time :updated_at
        index :id
      }
    end
  end

  class Map < self
    def self.default_schema
      lambda {
        primary_key :added_id
        bytea   :id, :null => false
        bytea   :data
        DateTime :created_at
        DateTime :updated_at
        unique :id
      }
    end
  end
end


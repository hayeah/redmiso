module Redmiso::Storage
  
end

class Redmiso::Storage::Basic
  require 'forwardable'
  extend Forwardable

  def_delegators :@dataset, :put, :get, :set, :delete
  
  attr_reader :dataset
  def initialize(dataset)
    @dataset = dataset
  end
end

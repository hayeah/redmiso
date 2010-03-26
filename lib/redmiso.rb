module Redmiso
  require 'sequel'
  require 'redmiso/dataset'
  require 'redmiso/storage'
  require 'redmiso/base'

  Sequel.database_timezone = :utc
  Sequel.application_timezone = :local
  
  class Error < RuntimeError
  end

  class NotFound < Error
  end

  class DuplicateID < Error
  end
end

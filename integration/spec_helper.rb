$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems'
require 'redmiso'
require 'spec'
require 'spec/autorun'

require 'pp'
require 'thread'
require 'rr'
Spec::Runner.configure do |config|
  config.mock_with :rr
end

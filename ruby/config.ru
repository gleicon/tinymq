require 'rubygems'
require 'sinatra'
set :env,  :production
disable :run

require './tinymq'

run Sinatra::Application

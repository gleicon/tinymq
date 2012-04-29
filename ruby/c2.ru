require 'rubygems'
require 'sinatra'
set :env,  :production
disable :run

require './web'

run Sinatra::Application

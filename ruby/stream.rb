require 'sinatra'

set :server, :thin
connections = []
queue_presence = Hash.new { |h,k| h[k] = Array.new }

get '/:queue' do |queue|
  # keep stream open
  stream(:keep_open) do |out| 
      queue_presence[queue] << out 
  end
end

post '/:queue' do |queue|
  # write to all open streams
  queue_presence[queue].each do |out|
    out << params[:message] << "\n"
  end
  "message sent"
end


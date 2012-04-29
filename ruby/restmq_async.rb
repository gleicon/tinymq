# Sinatra minimalist RestMQ
# no COMET, just /q/ routes and queue logic
# the core of RestMQ is how it uses Redis' data types

require 'rubygems'
require "bundler/setup"

require 'eventmachine'
require 'sinatra/async'
require 'redis'
require 'json'

QUEUESET = 'QUEUESET'   # queue index
UUID_SUFFIX = ':UUID'   # queue unique id
QUEUE_SUFFIX = ':queue' # suffix to identify each queue's LIST

class CometProducer
    include EM::Deferrable
    def produce_forever(body, queue)
      body.call ['oie\n'] 
      puts queue
      puts body
      while true do
        body.call ['oy #{queue}']
        sleep 5
      end
    end
end


class RestMQ < Sinatra::Base
  register Sinatra::Async
  enable :show_exceptions  

  def initialize
    super
    @reds = Redis.new
    @message_hub = Hash.new { |h,k| h[k] = EM::Queue.new }
  end

  aget '/q' do
    b = @reds.smembers QUEUESET
    throw :halt, [404, 'Not found (empty queueset)'] if b == nil
    b.map! do |q| q = '/q/'+q end
    b.to_json
  end

  aget '/q/:queue' do |queue|
    soft = params['soft'] # soft = true doesn't rpop values
    throw :halt, [404, 'Not found'] if queue == nil
    queue = queue + QUEUE_SUFFIX
    if soft != nil
      b = @reds.lindex queue, -1
    else
      b = @reds.rpop queue 
    end
    throw :halt, [404, 'Not found (empty queue)'] if b == nil
    v = @reds.get b
    throw :halt, [200, "{'value':" + v + ", 'key':" + b + "}"] unless v == nil 
    'empty value'
  end

  apost '/q/*' do |queue|
    value = params['value'].to_s
    throw :halt, [404, "Not found"] if queue == nil
    q1 = queue + QUEUE_SUFFIX
    uuid = @reds.incr queue + UUID_SUFFIX 
    @reds.sadd QUEUESET, q1
    lkey = queue + ':' + uuid.to_s
    @reds.set lkey, value
    @reds.lpush q1, lkey
    body '{ok, ' + lkey + '}'
  end

  aget '/c/:queue' do |queue|
    cp = CometProducer.new
    Thread.new { cp.produce_forever body, queue }
  end

end

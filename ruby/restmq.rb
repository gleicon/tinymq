require 'rubygems'
require "bundler/setup"

require 'sinatra'
require 'redis'
require 'json'
require 'em-synchrony'
require 'sinatra/synchrony'

QUEUESET = 'QUEUESET'   # queue index
UUID_SUFFIX = ':UUID'   # queue unique id
QUEUE_SUFFIX = ':queue' # suffix to identify each queue's LIST

class RestMQ < Sinatra::Base
  register Sinatra::Synchrony
  enable :show_exceptions  

  def initialize
    super
    @reds = Redis.new
    $_queue_presence = Hash.new { |h,k| h[k] = Array.new }
    EM.add_periodic_timer(1) do dispatcher end
  end

  def dispatcher
        puts $_queue_presence
        queues = @reds.smembers QUEUESET
        queues.map! do |q| "#{q}#{QUEUE_SUFFIX}" end
        if queues.size > 0 then
            q, k= @reds.brpop *queues, 2
            return unless q != nil
            v = @reds.get k
            puts "--"
            puts k
            puts q
            puts v
            puts "--"
            $_queue_presence[q].each do |out|
               out << "#{v}\n" unless v == nil
            end
        end
  end

  get '/q' do
    b = @reds.smembers QUEUESET
    throw :halt, [404, 'Not found (empty queueset)'] if b == nil
    b.map! do |q| q = '/q/'+q end
    b.to_json
  end

  get '/q/:queue' do |queue|
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

  post '/q/*' do |queue|
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

  get '/c/*' do |queue|
    stream do |out| 
        out << "\n"
        $_queue_presence[queue] << out
        $_queue_presence[queue].delete out
    end
  end

end

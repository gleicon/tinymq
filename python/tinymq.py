from bottle import run, debug, abort, request, ServerAdapter, response, get, post
import redis
import json
import gevent
from gevent.queue import Queue
from Queue import Empty

from gevent import monkey
from collections import defaultdict
monkey.patch_all()

QUEUESET = 'QUEUESET'
UUID_SUFFIX = ':UUID'
QUEUE_SUFFIX = ':queue'

redis_cli = redis.Redis()
_queue_presence = defaultdict(lambda :Queue())

def queue_hub():
    while True:
        queues = list(redis_cli.smembers(QUEUESET))
        queues = map(lambda x : "%s%s" % (x, QUEUE_SUFFIX), queues)
        if len(queues) > 0:
            res = redis_cli.brpop(queues)
            if res is not None: 
                _queue_presence[res[0]].put(res[1])

gevent.spawn(queue_hub)

@get('/')
def all_queues():
    qs = redis_cli.smembers(QUEUESET) 
    if qs is None: abort(404, "No queues created")
    l = ["/q/%s" % q for q in qs]
    return json.dumps(l)

@get('/c/:q')
def queue(q = None):
    if q is None: abort(404, "No queue given")
    qn = q + QUEUE_SUFFIX
    response.set_header("Transfer-Encoding", "chunked")
    response.content_type = 'application/json; charset=UTF-8'
    redis_cli.sadd(QUEUESET, q)
    v = b = None
    yield " \n\n" # clean the pipes
    while(True):
        try:
            b = _queue_presence[qn].get(block=True, timeout=60)
            if b is not None: v = redis_cli.get(b)
        except Empty, e:
            pass

        if v is not None: 
            yield "%s\n" % json.dumps({"value": v, "key": b})
            v = None

@get('/q/:q')
def queue(q = None):
    if q is None: abort(404, "No queue given")
    redis_cli.sadd(QUEUESET, q)
    q = q + QUEUE_SUFFIX

    if "soft" in request.GET.keys(): b = redis_cli.lindex(q, -1)
    else: b = redis_cli.rpop(q)

    if b is None: abort(404, "Empty queue")
    v = redis_cli.get(b)

    if v is None: abort(404, "Empty value/no val")
    return json.dumps({"value": v, "key": b})

@post('/q/:q')
def queue(q = None):
    if q is None: abort(404, "No queue given")
    qkey = q + QUEUE_SUFFIX

    value = request.POST['value']
    if value is None: abort(401, "No value given")

    uuid = redis_cli.incr(q + UUID_SUFFIX)
    redis_cli.sadd(QUEUESET, q)

    lkey = "%s:%d" % (q, uuid)
    redis_cli.set(lkey, value)
    redis_cli.lpush(qkey, lkey)
    return json.dumps({"ok": lkey})

class GEventServerAdapter(ServerAdapter):
    def run(self, handler):
        from gevent import monkey
        monkey.patch_socket()
        from gevent.wsgi import WSGIServer
        WSGIServer((self.host, self.port), handler).serve_forever()

#debug(True)
run(host='localhost', port=8081, server=GEventServerAdapter)

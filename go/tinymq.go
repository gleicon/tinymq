package main

import (
	"encoding/json"
	"fmt"
	"github.com/fiorix/go-redis/redis"
	"github.com/fiorix/go-web/http"
	"github.com/fiorix/go-web/remux"
	"log"
	"strconv"
	"time"
)

var QUEUESET = "QUEUESET"
var UUID_SUFFIX = ":UUID"
var QUEUE_SUFFIX = ":queue"

var redis_client *redis.Client

type Response map[string]interface{}

func (r Response) String() (s string) {
	b, err := json.Marshal(r)
	if err != nil {
		s = ""
		return
	}
	s = string(b)
	return
}

func logger(w http.ResponseWriter, r *http.Request) {
	log.Printf("HTTP %d %s %s (%s) :: %s",
		w.Status(),
		r.Method,
		r.URL.Path,
		r.RemoteAddr,
		time.Since(r.Created))
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "tinymq")
}

func queueHandler(w http.ResponseWriter, r *http.Request) {
	queue_name := r.Vars[0]
	if queue_name == "" {
		http.Error(w, "Queue name not given", 404)
		return
	}
	qname := queue_name + QUEUE_SUFFIX
	quuid := queue_name + UUID_SUFFIX
	soft := r.FormValue("soft")
	callback := r.FormValue("callback")

	switch r.Method {
	case "GET":
		var (
			p   string
			err error
		)

		if soft != "" {
			p, err = redis_client.LIndex(qname, -1)
		} else {
			p, err = redis_client.RPop(qname)
		}

		if p == "" {
			http.Error(w, "Empty queue", 404)
			return
		}

		if err == nil {
			v, err := redis_client.Get(p)
			if err == nil {
				resp := Response{"key": p, "value": v}

				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					if callback == "" {
						fmt.Fprintf(w, "%s\n", resp)
					} else {
						fmt.Fprintf(w, "%s(%s);\n", callback, resp)
					}
					return
				}
				http.Error(w, "Value not found", 500)
				return
			}
		}
	case "POST":
		value := r.FormValue("value")
		if value == "" {
			http.Error(w, "Empty value", 401)
		}

		uuid, _ := redis_client.Incr(quuid)
		lkey := queue_name + ":" + strconv.Itoa(uuid)
		redis_client.Set(lkey, value)
		redis_client.LPush(qname, lkey)
		resp := Response{"key": lkey, "value": value}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s\n", resp)
	default:
		http.Error(w, "Method not accepted", 400)
		return

	}
}

func main() {
	redis_client = redis.New("127.0.0.1:6379")

	queue_name_re := "([a-zA-Z0-9]+)$"
	remux.HandleFunc("^/$", IndexHandler)
	remux.HandleFunc("^/q/"+queue_name_re, queueHandler)
	server := http.Server{
		Addr:    ":8080",
		Handler: remux.DefaultServeMux,
		Logger:  logger,
	}
	server.ListenAndServe()

}

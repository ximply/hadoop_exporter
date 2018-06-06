package main

import (
	"flag"
	"net"
	"os"
	"net/http"
	"io"
	"github.com/parnurzeal/gorequest"
	"github.com/robfig/cron"
	"time"
	"fmt"
	"sync"
	"encoding/json"
)

var (
	Name           = "hadoop_secondnamenode_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/hadoop_secondnamenode_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	nameNodeJmxUrl = flag.String("jmx.url", "http://localhost:50090/jmx", "Hadoop second namenode JMX URL.")
	role           = flag.String("role", "SecondaryNameNode", "Role type.")
)

var g_doing bool
var g_ret string
var g_lock sync.RWMutex

type HadoopNameNodeJmxInfo struct {
	MemoryInfo Memory
	JvmMetricsInfo JvmMetrics
}

type Memory struct {
	heapMemoryUsageCommitted float64
	heapMemoryUsageInit float64
	heapMemoryUsageMax float64
	heapMemoryUsageUsed float64
}

type JvmMetrics struct {
	GcTimeMillis float64
	GcTimeMillisParNew float64
	GcTimeMillisConcurrentMarkSweep float64
	GcCount float64
	GcCountParNew float64
	GcCountConcurrentMarkSweep float64
	ThreadsBlocked float64
	ThreadsWaiting float64
}


func metrics(w http.ResponseWriter, r *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func info() (HadoopNameNodeJmxInfo, bool) {
	ret := HadoopNameNodeJmxInfo {
	}
	// http://localhost:50090/jmx
	req := gorequest.New()
	_, body, errs := req.Retry(1, 5 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).Get(*nameNodeJmxUrl).End()
	if errs != nil {
		return ret, false
	}

	var f interface{}
	err := json.Unmarshal([]byte(body), &f)
	if err != nil {
		return ret, false
	}
	m := f.(map[string]interface{})
	var nameList = m["beans"].([]interface{})
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})

		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			ret.MemoryInfo.heapMemoryUsageCommitted = heapMemoryUsage["committed"].(float64)
			ret.MemoryInfo.heapMemoryUsageInit = heapMemoryUsage["init"].(float64)
			ret.MemoryInfo.heapMemoryUsageMax = heapMemoryUsage["max"].(float64)
			ret.MemoryInfo.heapMemoryUsageUsed = heapMemoryUsage["used"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=SecondaryNameNode,name=JvmMetrics" {
			ret.JvmMetricsInfo.GcTimeMillis = nameDataMap["GcTimeMillis"].(float64)
			ret.JvmMetricsInfo.GcTimeMillisParNew = nameDataMap["GcTimeMillisParNew"].(float64)
			ret.JvmMetricsInfo.GcTimeMillisConcurrentMarkSweep = nameDataMap["GcTimeMillisConcurrentMarkSweep"].(float64)
			ret.JvmMetricsInfo.GcCount = nameDataMap["GcCount"].(float64)
			ret.JvmMetricsInfo.GcCountParNew = nameDataMap["GcCountParNew"].(float64)
			ret.JvmMetricsInfo.GcCountConcurrentMarkSweep = nameDataMap["GcCountConcurrentMarkSweep"].(float64)
			ret.JvmMetricsInfo.ThreadsBlocked = nameDataMap["ThreadsBlocked"].(float64)
			ret.JvmMetricsInfo.ThreadsWaiting = nameDataMap["ThreadsWaiting"].(float64)
		}
	}

	return ret, true
}

func doWork() {
	if g_doing {
		return
	}
	g_doing = true

	s, ok := info()
	if !ok {
		g_doing = false
		return
	}

	ret := ""
	nameSpace := "hadoop_"

	// Memory
	ret += fmt.Sprintf("%s_heap_memory{type=\"committed\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageCommitted)
	ret += fmt.Sprintf("%s_heap_memory{type=\"init\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageInit)
	ret += fmt.Sprintf("%s_heap_memory{type=\"max\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageMax)
	ret += fmt.Sprintf("%s_heap_memory{type=\"used\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageUsed)


	// JvmMetrics
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_total_millis{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillis)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"par_new\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillisParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"concurrent_mark_sweep\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillisConcurrentMarkSweep)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count_total{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCount)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{role=\"%s\",type=\"par_new\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCountParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{role=\"%s\",type=\"concurrent_mark_sweep\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCountConcurrentMarkSweep)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_threads_blocked{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.ThreadsBlocked)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_threads_waiting{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.ThreadsWaiting)

	g_lock.Lock()
	g_ret = ret
	g_lock.Unlock()

	g_doing = false
}

func main() {
	flag.Parse()

	g_doing = false
	doWork()
	c := cron.New()
	c.AddFunc("0 */2 * * * ?", doWork)
	c.Start()

	mux := http.NewServeMux()
	mux.HandleFunc(*metricsPath, metrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Hadoop Second Name Node Exporter</title></head>
             <body>
             <h1>Hadoop Second Name Node Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	server := http.Server{
		Handler: mux, // http.DefaultServeMux,
	}
	os.Remove(*listenAddress)

	listener, err := net.Listen("unix", *listenAddress)
	if err != nil {
		panic(err)
	}
	server.Serve(listener)
}

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
	Name           = "hadoop_resourcemanager_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/hadoop_resourcemanager_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	rmUrl = flag.String("rm.url", "http://localhost:8088", "Hadoop resource manager URL.")
	jmxUrl = flag.String("jmx.url", "http://localhost:8088/jmx", "Hadoop resource manager JMX URL.")
	role           = flag.String("role", "ResourceManager", "Role type.")
)

var g_doing bool
var g_ret string
var g_lock sync.RWMutex

type RmInfo struct {
	AppsSubmitted float64
    AppsCompleted float64
    AppsPending float64
    AppsRunning float64
    AppsFailed float64
    AppsKilled float64
    ReservedMB float64
    AvailableMB float64
    AllocatedMB float64
	ContainersAllocated float64
	ContainersReserved float64
	ContainersPending float64
	TotalMB float64
	TotalNodes float64
	LostNodes float64
	UnhealthyNodes float64
	DecommissionedNodes float64
	RebootedNodes float64
	ActiveNodes float64
}

type DetailInfo struct {
	HeapMemoryUsageCommitted float64
	HeapMemoryUsageInit float64
	HeapMemoryUsageMax float64
	HeapMemoryUsageUsed float64

	GcTimeMillis float64
	GcCount float64
	ThreadsBlocked float64
	ThreadsWaiting float64
}

func metrics(w http.ResponseWriter, r *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func detailInfo() (DetailInfo, bool) {
	ret := DetailInfo{

	}

	// http://localhost:8088/jmx
	req := gorequest.New()
	_, body, errs := req.Retry(1, 5 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).Get(*jmxUrl).End()
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
			ret.HeapMemoryUsageCommitted = heapMemoryUsage["committed"].(float64)
			ret.HeapMemoryUsageInit = heapMemoryUsage["init"].(float64)
			ret.HeapMemoryUsageMax = heapMemoryUsage["max"].(float64)
			ret.HeapMemoryUsageUsed = heapMemoryUsage["used"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=ResourceManager,name=JvmMetrics" {
			ret.GcTimeMillis = nameDataMap["GcTimeMillis"].(float64)
			ret.GcCount = nameDataMap["GcCount"].(float64)
			ret.ThreadsBlocked = nameDataMap["ThreadsBlocked"].(float64)
			ret.ThreadsWaiting = nameDataMap["ThreadsWaiting"].(float64)
		}
	}

	return ret, true
}

func info() (RmInfo, bool) {
	ret := RmInfo {
	}
	// http://localhost:8088/ws/v1/cluster/metrics
	req := gorequest.New()
	_, body, errs := req.Retry(1, 5 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).Get(
			fmt.Sprintf("%s/ws/v1/cluster/metrics", *rmUrl)).End()
	if errs != nil {
		return ret, false
	}

	var f interface{}
	err := json.Unmarshal([]byte(body), &f)
	if err != nil {
		return ret, false
	}
	m := f.(map[string]interface{})
	cm := m["clusterMetrics"].(map[string]interface{})
	ret.AppsSubmitted = cm["appsSubmitted"].(float64)
	ret.AppsCompleted = cm["appsCompleted"].(float64)
	ret.AppsPending = cm["appsPending"].(float64)
	ret.AppsRunning = cm["appsRunning"].(float64)
	ret.AppsFailed = cm["appsFailed"].(float64)
	ret.AppsKilled = cm["appsKilled"].(float64)
	ret.ReservedMB = cm["reservedMB"].(float64)
	ret.AvailableMB = cm["availableMB"].(float64)
	ret.AllocatedMB = cm["allocatedMB"].(float64)
	ret.ContainersAllocated = cm["containersAllocated"].(float64)
	ret.ContainersReserved = cm["containersReserved"].(float64)
	ret.ContainersPending = cm["containersPending"].(float64)
	ret.TotalMB = cm["totalMB"].(float64)
	ret.TotalNodes = cm["totalNodes"].(float64)
	ret.LostNodes = cm["lostNodes"].(float64)
	ret.UnhealthyNodes = cm["unhealthyNodes"].(float64)
	ret.DecommissionedNodes = cm["decommissionedNodes"].(float64)
	ret.RebootedNodes = cm["rebootedNodes"].(float64)
	ret.ActiveNodes = cm["activeNodes"].(float64)

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

	s1, ok := detailInfo()
	if !ok {
		g_doing = false
		return
	}

	ret := ""
	nameSpace := "hadoop_"

	ret += fmt.Sprintf("%s_nodes{type=\"active\",role=\"%s\"} %g\n",
		nameSpace, *role, s.ActiveNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"rebooted\",role=\"%s\"} %g\n",
		nameSpace, *role, s.RebootedNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"decommissioned\",role=\"%s\"} %g\n",
		nameSpace, *role, s.DecommissionedNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"unhealthy\",role=\"%s\"} %g\n",
		nameSpace, *role, s.UnhealthyNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"lost\",role=\"%s\"} %g\n",
		nameSpace, *role, s.LostNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"total\",role=\"%s\"} %g\n",
		nameSpace, *role, s.TotalNodes)


	ret += fmt.Sprintf("%s_containers{type=\"allocated\",role=\"%s\"} %g\n",
		nameSpace, *role, s.ContainersAllocated)
	ret += fmt.Sprintf("%s_containers{type=\"reserved\",role=\"%s\"} %g\n",
		nameSpace, *role, s.ContainersReserved)
	ret += fmt.Sprintf("%s_containers{type=\"pending\",role=\"%s\"} %g\n",
		nameSpace, *role, s.ContainersPending)

	ret += fmt.Sprintf("%s_apps{type=\"killed\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AppsKilled)
	ret += fmt.Sprintf("%s_apps{type=\"failed\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AppsFailed)
	ret += fmt.Sprintf("%s_apps{type=\"running\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AppsRunning)
	ret += fmt.Sprintf("%s_apps{type=\"pending\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AppsPending)

	ret += fmt.Sprintf("%s_space{type=\"available\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AvailableMB)
	ret += fmt.Sprintf("%s_space{type=\"reserved\",role=\"%s\"} %g\n",
		nameSpace, *role, s.ReservedMB)
	ret += fmt.Sprintf("%s_space{type=\"allocated\",role=\"%s\"} %g\n",
		nameSpace, *role, s.AllocatedMB)
	ret += fmt.Sprintf("%s_space{type=\"total\",role=\"%s\"} %g\n",
		nameSpace, *role, s.TotalMB)

	ret += fmt.Sprintf("%s_heap_memory{type=\"committed\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.HeapMemoryUsageCommitted)
	ret += fmt.Sprintf("%s_heap_memory{type=\"init\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.HeapMemoryUsageInit)
	ret += fmt.Sprintf("%s_heap_memory{type=\"max\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.HeapMemoryUsageMax)
	ret += fmt.Sprintf("%s_heap_memory{type=\"used\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.HeapMemoryUsageUsed)

	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_total_millis{role=\"%s\"} %g\n",
		nameSpace, *role, s1.GcTimeMillis)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_threads_blocked{type=\"par_new\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.ThreadsBlocked)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count_total{role=\"%s\"} %g\n",
		nameSpace, *role, s1.GcCount)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_threads_waiting{type=\"par_new\",role=\"%s\"} %g\n",
		nameSpace, *role, s1.ThreadsWaiting)

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
             <head><title>Hadoop Resource Manager Exporter</title></head>
             <body>
             <h1>Hadoop Resource Manager Exporter</h1>
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

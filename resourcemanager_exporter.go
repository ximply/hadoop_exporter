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

func metrics(w http.ResponseWriter, r *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
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

	ret := ""
	nameSpace := "hadoop_resource_manager"

	ret += fmt.Sprintf("%s_nodes{type=\"active\"} %g\n",
		nameSpace, s.ActiveNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"rebooted\"} %g\n",
		nameSpace, s.RebootedNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"decommissioned\"} %g\n",
		nameSpace, s.DecommissionedNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"unhealthy\"} %g\n",
		nameSpace, s.UnhealthyNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"lost\"} %g\n",
		nameSpace, s.LostNodes)
	ret += fmt.Sprintf("%s_nodes{type=\"total\"} %g\n",
		nameSpace, s.TotalNodes)


	ret += fmt.Sprintf("%s_containers{type=\"allocated\"} %g\n",
		nameSpace, s.ContainersAllocated)
	ret += fmt.Sprintf("%s_containers{type=\"reserved\"} %g\n",
		nameSpace, s.ContainersReserved)
	ret += fmt.Sprintf("%s_containers{type=\"pending\"} %g\n",
		nameSpace, s.ContainersPending)

	ret += fmt.Sprintf("%s_apps{type=\"killed\"} %g\n",
		nameSpace, s.AppsKilled)
	ret += fmt.Sprintf("%s_apps{type=\"failed\"} %g\n",
		nameSpace, s.AppsFailed)
	ret += fmt.Sprintf("%s_apps{type=\"running\"} %g\n",
		nameSpace, s.AppsRunning)
	ret += fmt.Sprintf("%s_apps{type=\"pending\"} %g\n",
		nameSpace, s.AppsPending)

	ret += fmt.Sprintf("%s_space{type=\"available\"} %g\n",
		nameSpace, s.AvailableMB)
	ret += fmt.Sprintf("%s_space{type=\"reserved\"} %g\n",
		nameSpace, s.ReservedMB)
	ret += fmt.Sprintf("%s_space{type=\"allocated\"} %g\n",
		nameSpace, s.AllocatedMB)
	ret += fmt.Sprintf("%s_space{type=\"total\"} %g\n",
		nameSpace, s.TotalMB)


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

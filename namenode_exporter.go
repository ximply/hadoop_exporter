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
	Name           = "hadoop_namenode_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/hadoop_namenode_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	nameNodeJmxUrl = flag.String("jmx.url", "http://localhost:50070/jmx", "Hadoop namenode JMX URL.")
	role           = flag.String("role", "NameNode", "Role type.")
)

var g_doing bool
var g_ret string
var g_lock sync.RWMutex

type HadoopNameNodeJmxInfo struct {
	FSNamesystemInfo FSNamesystem
	MemoryInfo Memory
	FSNamesystemStateInfo FSNamesystemState
	NameNodeActivityInfo NameNodeActivity
	JvmMetricsInfo JvmMetrics
}

type FSNamesystem struct {
	MissingBlocks            float64
	CapacityTotalGB          float64
	CapacityUsedGB           float64
	CapacityRemainingGB      float64
	BlocksTotal              float64
	FilesTotal               float64
	CorruptBlocks            float64
	ExcessBlocks             float64
	TotalLoad                float64
	ScheduledReplicationBlocks float64
	PendingReplicationBlocks   float64
}

type Memory struct {
	heapMemoryUsageCommitted float64
	heapMemoryUsageInit float64
	heapMemoryUsageMax float64
	heapMemoryUsageUsed float64
}

type FSNamesystemState struct {
	CapacityTotal float64
	CapacityUsed float64
	CapacityRemaining float64
	TotalLoad float64
	BlocksTotal float64
	FilesTotal float64
	PendingReplicationBlocks float64
	UnderReplicatedBlocks float64
	ScheduledReplicationBlocks float64
	NumLiveDataNodes float64
	NumDeadDataNodes float64
}

type NameNodeActivity struct {
	CreateFileOps float64
	FilesCreated float64
	FilesAppended float64
	GetBlockLocations float64
	FilesRenamed float64
	GetListingOps float64
	DeleteFileOps float64
	FilesDeleted float64
	FileInfoOps float64

	AddBlockOps float64
	GetAdditionalDatanodeOps float64
	CreateSymlinkOps float64
	GetLinkTargetOps float64
	FilesInGetListingOps float64
	StorageBlockReportOps float64
	TransactionsNumOps float64
	TransactionsAvgTime float64

	SyncsNumOps float64
	SyncsAvgTime float64
	TransactionsBatchedInSync float64
	BlockReportNumOps float64
	BlockReportAvgTime float64
	SafeModeTime float64
	FsImageLoadTime float64

	GetEditNumOps float64
	GetEditAvgTime float64
	GetImageNumOps float64
	GetImageAvgTime float64
	PutImageNumOps float64
	PutImageAvgTime float64
}

type JvmMetrics struct {
	GcTimeMillis float64
	GcTimeMillisParNew float64
	GcTimeMillisConcurrentMarkSweep float64
	GcCount float64
	GcCountParNew float64
	GcCountConcurrentMarkSweep float64
	ThreadsBlocked float64
}


func metrics(w http.ResponseWriter, r *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func info() (HadoopNameNodeJmxInfo, bool) {
	ret := HadoopNameNodeJmxInfo {
	}
	// http://localhost:50070/jmx
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

		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystem" {
			ret.FSNamesystemInfo.MissingBlocks = nameDataMap["MissingBlocks"].(float64)
			ret.FSNamesystemInfo.CapacityTotalGB = nameDataMap["CapacityTotalGB"].(float64)
			ret.FSNamesystemInfo.CapacityUsedGB = nameDataMap["CapacityUsedGB"].(float64)
			ret.FSNamesystemInfo.CapacityRemainingGB = nameDataMap["CapacityRemainingGB"].(float64)
			ret.FSNamesystemInfo.BlocksTotal = nameDataMap["BlocksTotal"].(float64)
			ret.FSNamesystemInfo.FilesTotal = nameDataMap["FilesTotal"].(float64)
			ret.FSNamesystemInfo.CorruptBlocks = nameDataMap["CorruptBlocks"].(float64)
			ret.FSNamesystemInfo.ExcessBlocks = nameDataMap["ExcessBlocks"].(float64)
			ret.FSNamesystemInfo.TotalLoad = nameDataMap["TotalLoad"].(float64)
			ret.FSNamesystemInfo.ScheduledReplicationBlocks = nameDataMap["ScheduledReplicationBlocks"].(float64)
			ret.FSNamesystemInfo.PendingReplicationBlocks = nameDataMap["PendingReplicationBlocks"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystemState" {
			ret.FSNamesystemStateInfo.CapacityTotal = nameDataMap["CapacityTotal"].(float64)
			ret.FSNamesystemStateInfo.CapacityUsed = nameDataMap["CapacityUsed"].(float64)
			ret.FSNamesystemStateInfo.CapacityRemaining = nameDataMap["CapacityRemaining"].(float64)
			ret.FSNamesystemStateInfo.TotalLoad = nameDataMap["TotalLoad"].(float64)
			ret.FSNamesystemStateInfo.BlocksTotal = nameDataMap["BlocksTotal"].(float64)
			ret.FSNamesystemStateInfo.FilesTotal = nameDataMap["FilesTotal"].(float64)
			ret.FSNamesystemStateInfo.PendingReplicationBlocks = nameDataMap["PendingReplicationBlocks"].(float64)
			ret.FSNamesystemStateInfo.UnderReplicatedBlocks = nameDataMap["UnderReplicatedBlocks"].(float64)
			ret.FSNamesystemStateInfo.ScheduledReplicationBlocks = nameDataMap["ScheduledReplicationBlocks"].(float64)
			ret.FSNamesystemStateInfo.NumLiveDataNodes = nameDataMap["NumLiveDataNodes"].(float64)
			ret.FSNamesystemStateInfo.NumDeadDataNodes = nameDataMap["NumDeadDataNodes"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=NameNode,name=NameNodeActivity" {
			ret.NameNodeActivityInfo.CreateFileOps = nameDataMap["CreateFileOps"].(float64)
			ret.NameNodeActivityInfo.FilesCreated = nameDataMap["FilesCreated"].(float64)
			ret.NameNodeActivityInfo.FilesAppended = nameDataMap["FilesAppended"].(float64)
			ret.NameNodeActivityInfo.GetBlockLocations = nameDataMap["GetBlockLocations"].(float64)
			ret.NameNodeActivityInfo.FilesRenamed = nameDataMap["FilesRenamed"].(float64)
			ret.NameNodeActivityInfo.GetListingOps = nameDataMap["GetListingOps"].(float64)
			ret.NameNodeActivityInfo.DeleteFileOps = nameDataMap["DeleteFileOps"].(float64)
			ret.NameNodeActivityInfo.FilesDeleted = nameDataMap["FilesDeleted"].(float64)
			ret.NameNodeActivityInfo.FileInfoOps = nameDataMap["FileInfoOps"].(float64)

			ret.NameNodeActivityInfo.AddBlockOps = nameDataMap["AddBlockOps"].(float64)
			ret.NameNodeActivityInfo.GetAdditionalDatanodeOps = nameDataMap["GetAdditionalDatanodeOps"].(float64)
			ret.NameNodeActivityInfo.CreateSymlinkOps = nameDataMap["CreateSymlinkOps"].(float64)
			ret.NameNodeActivityInfo.GetLinkTargetOps = nameDataMap["GetLinkTargetOps"].(float64)
			ret.NameNodeActivityInfo.FilesInGetListingOps = nameDataMap["FilesInGetListingOps"].(float64)
			ret.NameNodeActivityInfo.StorageBlockReportOps = nameDataMap["StorageBlockReportOps"].(float64)
			ret.NameNodeActivityInfo.TransactionsNumOps = nameDataMap["TransactionsNumOps"].(float64)
			ret.NameNodeActivityInfo.TransactionsAvgTime = nameDataMap["TransactionsAvgTime"].(float64)

			ret.NameNodeActivityInfo.SyncsNumOps = nameDataMap["SyncsNumOps"].(float64)
			ret.NameNodeActivityInfo.SyncsAvgTime = nameDataMap["SyncsAvgTime"].(float64)
			ret.NameNodeActivityInfo.TransactionsBatchedInSync = nameDataMap["TransactionsBatchedInSync"].(float64)
			ret.NameNodeActivityInfo.BlockReportNumOps = nameDataMap["BlockReportNumOps"].(float64)
			ret.NameNodeActivityInfo.BlockReportAvgTime = nameDataMap["BlockReportAvgTime"].(float64)
			ret.NameNodeActivityInfo.SafeModeTime = nameDataMap["SafeModeTime"].(float64)
			ret.NameNodeActivityInfo.FsImageLoadTime = nameDataMap["FsImageLoadTime"].(float64)

			ret.NameNodeActivityInfo.GetEditNumOps = nameDataMap["GetEditNumOps"].(float64)
			ret.NameNodeActivityInfo.GetEditAvgTime = nameDataMap["GetEditAvgTime"].(float64)
			ret.NameNodeActivityInfo.GetImageNumOps = nameDataMap["GetImageNumOps"].(float64)
			ret.NameNodeActivityInfo.GetImageAvgTime = nameDataMap["GetImageAvgTime"].(float64)
			ret.NameNodeActivityInfo.PutImageNumOps = nameDataMap["PutImageNumOps"].(float64)
			ret.NameNodeActivityInfo.PutImageAvgTime = nameDataMap["PutImageAvgTime"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=NameNode,name=JvmMetrics" {
			ret.JvmMetricsInfo.GcTimeMillis = nameDataMap["GcTimeMillis"].(float64)
			ret.JvmMetricsInfo.GcTimeMillisParNew = nameDataMap["GcTimeMillisParNew"].(float64)
			ret.JvmMetricsInfo.GcTimeMillisConcurrentMarkSweep = nameDataMap["GcTimeMillisConcurrentMarkSweep"].(float64)
			ret.JvmMetricsInfo.GcCount = nameDataMap["GcCount"].(float64)
			ret.JvmMetricsInfo.GcCountParNew = nameDataMap["GcCountParNew"].(float64)
			ret.JvmMetricsInfo.GcCountConcurrentMarkSweep = nameDataMap["GcCountConcurrentMarkSweep"].(float64)
			ret.JvmMetricsInfo.ThreadsBlocked = nameDataMap["ThreadsBlocked"].(float64)
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
	nameSpace := "hadoop_namenode"

	// Memory
	ret += fmt.Sprintf("%s_heap_memory{type=\"committed\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageCommitted)
	ret += fmt.Sprintf("%s_heap_memory{type=\"init\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageInit)
	ret += fmt.Sprintf("%s_heap_memory{type=\"max\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageMax)
	ret += fmt.Sprintf("%s_heap_memory{type=\"used\",role=\"%s\"} %g\n",
		nameSpace, *role, s.MemoryInfo.heapMemoryUsageUsed)

	// FSNamesystem
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"missing\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.MissingBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"total\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.BlocksTotal)
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"corrupt\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.CorruptBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"excess\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.ExcessBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"pending_repl\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.PendingReplicationBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_blocks{type=\"scheduled_repl\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.ScheduledReplicationBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_capacity{type=\"total\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.CapacityTotalGB)
	ret += fmt.Sprintf("%s_fs_name_system_capacity{type=\"used\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.CapacityUsedGB)
	ret += fmt.Sprintf("%s_fs_name_system_capacity{type=\"remaining\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.CapacityRemainingGB)
	ret += fmt.Sprintf("%s_fs_name_system_files_total{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.FilesTotal)
	ret += fmt.Sprintf("%s_fs_name_system_total_load{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemInfo.TotalLoad)

	// FSNamesystemState
	ret += fmt.Sprintf("%s_fs_name_system_state_capacity{type=\"total\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.CapacityTotal)
	ret += fmt.Sprintf("%s_fs_name_system_state_capacity{type=\"used\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.CapacityUsed)
	ret += fmt.Sprintf("%s_fs_name_system_state_capacity{type=\"remaining\",role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.CapacityRemaining)
	ret += fmt.Sprintf("%s_fs_name_system_state_total_load{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.TotalLoad)
	ret += fmt.Sprintf("%s_fs_name_system_state_blocks_total{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.BlocksTotal)
	ret += fmt.Sprintf("%s_fs_name_system_state_files_total{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.FilesTotal)
	ret += fmt.Sprintf("%s_fs_name_system_state_pending_replication_blocks{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.PendingReplicationBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_state_under_replicated_blocks{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.UnderReplicatedBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_state_scheduled_replication_blocks{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.ScheduledReplicationBlocks)
	ret += fmt.Sprintf("%s_fs_name_system_state_num_live_datanodes{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.NumLiveDataNodes)
	ret += fmt.Sprintf("%s_fs_name_system_state_num_dead_datanodes{role=\"%s\"} %g\n",
		nameSpace, *role, s.FSNamesystemStateInfo.NumDeadDataNodes)

	// NameNodeActivity
	ret += fmt.Sprintf("%s_activity_create_file_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.CreateFileOps)
	ret += fmt.Sprintf("%s_activity_file_created{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FilesCreated)
	ret += fmt.Sprintf("%s_activity_create_file_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.CreateFileOps)
	ret += fmt.Sprintf("%s_activity_get_block_locations{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetBlockLocations)
	ret += fmt.Sprintf("%s_activity_files_renamed{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FilesRenamed)
	ret += fmt.Sprintf("%s_activity_get_listing_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetListingOps)
	ret += fmt.Sprintf("%s_activity_get_delete_file_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.DeleteFileOps)
	ret += fmt.Sprintf("%s_activity_get_files_deleted{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FilesDeleted)
	ret += fmt.Sprintf("%s_activity_file_info_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FileInfoOps)

	ret += fmt.Sprintf("%s_activity_block_add_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.AddBlockOps)
	ret += fmt.Sprintf("%s_activity_get_additional_datanode_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetAdditionalDatanodeOps)
	ret += fmt.Sprintf("%s_activity_create_symlink_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.CreateSymlinkOps)
	ret += fmt.Sprintf("%s_activity_get_link_target_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetLinkTargetOps)
	ret += fmt.Sprintf("%s_activity_files_in_get_listing_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FilesInGetListingOps)
	ret += fmt.Sprintf("%s_activity_storage_block_report_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.StorageBlockReportOps)
	ret += fmt.Sprintf("%s_activity_transactions_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.TransactionsNumOps)
	ret += fmt.Sprintf("%s_activity_transactions_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.TransactionsAvgTime)

	ret += fmt.Sprintf("%s_activity_syncs_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.SyncsNumOps)
	ret += fmt.Sprintf("%s_activity_syncs_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.SyncsAvgTime)
	ret += fmt.Sprintf("%s_activity_transactions_batched_in_sync{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.TransactionsBatchedInSync)
	ret += fmt.Sprintf("%s_activity_block_report_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.BlockReportNumOps)
	ret += fmt.Sprintf("%s_activity_block_report_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.BlockReportAvgTime)
	ret += fmt.Sprintf("%s_activity_safemode_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.SafeModeTime)
	ret += fmt.Sprintf("%s_activity_fs_image_load_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.FsImageLoadTime)

	ret += fmt.Sprintf("%s_activity_get_edit_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetEditNumOps)
	ret += fmt.Sprintf("%s_activity_get_edit_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetEditAvgTime)
	ret += fmt.Sprintf("%s_activity_get_image_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetImageNumOps)
	ret += fmt.Sprintf("%s_activity_get_image_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.GetImageAvgTime)
	ret += fmt.Sprintf("%s_activity_put_image_num_ops{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.PutImageNumOps)
	ret += fmt.Sprintf("%s_activity_put_image_avg_time{role=\"%s\"} %g\n",
		nameSpace, *role, s.NameNodeActivityInfo.PutImageAvgTime)

	// JvmMetrics
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_total_millis{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillis)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"par_new\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillisParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"concurrent_mark_sweep\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcTimeMillisConcurrentMarkSweep)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count_total{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCount)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{type=\"par_new\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCountParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{type=\"concurrent_mark_sweep\",role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.GcCountConcurrentMarkSweep)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_threads_blocked{role=\"%s\"} %g\n",
		nameSpace, *role, s.JvmMetricsInfo.ThreadsBlocked)

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
             <head><title>Hadoop Name Node Exporter</title></head>
             <body>
             <h1>Hadoop Name Node Exporter</h1>
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

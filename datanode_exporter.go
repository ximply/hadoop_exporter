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
	Name           = "hadoop_datanode_exporter"
	listenAddress  = flag.String("unix-sock", "/dev/shm/hadoop_datanode_exporter.sock", "Address to listen on for unix sock access and telemetry.")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	dataNodeJmxUrl = flag.String("jmx.url", "http://localhost:50075/jmx", "Hadoop namenode JMX URL.")
)

var g_doing bool
var g_ret string
var g_lock sync.RWMutex

type DataNodeInfo struct {
	HeapMemoryUsageCommitted float64
	HeapMemoryUsageInit float64
	HeapMemoryUsageMax float64
	HeapMemoryUsageUsed float64

	BytesWritten float64
	BytesRead float64

	BlocksWritten float64
	BlocksRead float64
	BlocksReplicated float64
	BlocksRemoved float64
	BlocksVerified float64
	BlockVerificationFailures float64

	ReadsFromLocalClient float64
	ReadsFromRemoteClient float64
	WritesFromLocalClient float64
	WritesFromRemoteClient float64

	BlocksGetLocalPathInfo float64
	FsyncCount float64
	VolumeFailures float64

	ReadBlockOpNumOps float64
	ReadBlockOpAvgTime float64
	WriteBlockOpNumOps float64
	WriteBlockOpAvgTime float64
	BlockChecksumOpNumOps float64
	BlockChecksumOpAvgTime float64
	CopyBlockOpNumOps float64
	CopyBlockOpAvgTime float64
	ReplaceBlockOpNumOps float64
	ReplaceBlockOpAvgTime float64

	HeartbeatsNumOps float64
	HeartbeatsAvgTime float64

	BlockReportsNumOps float64
	BlockReportsAvgTime float64

	PacketAckRoundTripTimeNanosNumOps float64
	PacketAckRoundTripTimeNanosAvgTime float64

	FlushNanosNumOps float64
	FlushNanosAvgTime float64
	FsyncNanosNumOps float64
	FsyncNanosAvgTime float64

	SendDataPacketBlockedOnNetworkNanosNumOps float64
	SendDataPacketBlockedOnNetworkNanosAvgTime float64
	SendDataPacketTransferNanosNumOps float64
	SendDataPacketTransferNanosAvgTime float64

	GcTimeMillis float64
	GcTimeMillisParNew float64
	GcTimeMillisConcurrentMarkSweep float64
	GcCount float64
	GcCountParNew float64
	GcCountConcurrentMarkSweep float64
}


func metrics(w http.ResponseWriter, r *http.Request) {
	g_lock.RLock()
	io.WriteString(w, g_ret)
	g_lock.RUnlock()
}

func info() (DataNodeInfo, bool) {
	ret := DataNodeInfo {
	}

	hostName, osErr := os.Hostname()
	if osErr != nil {
		return ret, false
	}

	// http://localhost:50075/jmx
	req := gorequest.New()
	_, body, errs := req.Retry(1, 5 * time.Second,
		http.StatusBadRequest, http.StatusInternalServerError).Get(*dataNodeJmxUrl).End()
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

		if nameDataMap["name"] == "Hadoop:service=DataNode,name=DataNodeActivity-" + hostName + "-50010" {
			ret.BytesWritten = nameDataMap["BytesWritten"].(float64)
			ret.BytesRead = nameDataMap["BytesRead"].(float64)

			ret.BlocksWritten = nameDataMap["BlocksWritten"].(float64)
			ret.BlocksRead = nameDataMap["BlocksRead"].(float64)
			ret.BlocksReplicated = nameDataMap["BlocksReplicated"].(float64)
			ret.BlocksRemoved = nameDataMap["BlocksRemoved"].(float64)
			ret.BlocksVerified = nameDataMap["BlocksVerified"].(float64)
			ret.BlockVerificationFailures = nameDataMap["BlockVerificationFailures"].(float64)

			ret.ReadsFromLocalClient = nameDataMap["ReadsFromLocalClient"].(float64)
			ret.ReadsFromRemoteClient = nameDataMap["ReadsFromRemoteClient"].(float64)
			ret.WritesFromLocalClient = nameDataMap["WritesFromLocalClient"].(float64)
			ret.WritesFromRemoteClient = nameDataMap["WritesFromRemoteClient"].(float64)

			ret.BlocksGetLocalPathInfo = nameDataMap["BlocksGetLocalPathInfo"].(float64)
			ret.FsyncCount = nameDataMap["FsyncCount"].(float64)
			ret.VolumeFailures = nameDataMap["VolumeFailures"].(float64)

			ret.ReadBlockOpNumOps = nameDataMap["ReadBlockOpNumOps"].(float64)
			ret.ReadBlockOpAvgTime = nameDataMap["ReadBlockOpAvgTime"].(float64)
			ret.WriteBlockOpNumOps = nameDataMap["WriteBlockOpNumOps"].(float64)
			ret.WriteBlockOpAvgTime = nameDataMap["WriteBlockOpAvgTime"].(float64)
			ret.BlockChecksumOpNumOps = nameDataMap["BlockChecksumOpNumOps"].(float64)
			ret.BlockChecksumOpAvgTime = nameDataMap["BlockChecksumOpAvgTime"].(float64)
			ret.CopyBlockOpNumOps = nameDataMap["CopyBlockOpNumOps"].(float64)
			ret.CopyBlockOpAvgTime = nameDataMap["CopyBlockOpAvgTime"].(float64)
			ret.ReplaceBlockOpNumOps = nameDataMap["ReplaceBlockOpNumOps"].(float64)
			ret.ReplaceBlockOpAvgTime = nameDataMap["ReplaceBlockOpAvgTime"].(float64)

			ret.HeartbeatsNumOps = nameDataMap["HeartbeatsNumOps"].(float64)
			ret.HeartbeatsAvgTime = nameDataMap["HeartbeatsAvgTime"].(float64)

			ret.BlockReportsNumOps = nameDataMap["BlockReportsNumOps"].(float64)
			ret.BlockReportsAvgTime = nameDataMap["BlockReportsAvgTime"].(float64)

			ret.PacketAckRoundTripTimeNanosNumOps = nameDataMap["PacketAckRoundTripTimeNanosNumOps"].(float64)
			ret.PacketAckRoundTripTimeNanosAvgTime = nameDataMap["PacketAckRoundTripTimeNanosAvgTime"].(float64)

			ret.FlushNanosNumOps = nameDataMap["FlushNanosNumOps"].(float64)
			ret.FlushNanosAvgTime = nameDataMap["FlushNanosAvgTime"].(float64)
			ret.FsyncNanosNumOps = nameDataMap["FsyncNanosNumOps"].(float64)
			ret.FsyncNanosAvgTime = nameDataMap["FsyncNanosAvgTime"].(float64)

			ret.SendDataPacketBlockedOnNetworkNanosNumOps = nameDataMap["SendDataPacketBlockedOnNetworkNanosNumOps"].(float64)
			ret.SendDataPacketBlockedOnNetworkNanosAvgTime = nameDataMap["SendDataPacketBlockedOnNetworkNanosAvgTime"].(float64)
			ret.SendDataPacketTransferNanosNumOps = nameDataMap["SendDataPacketTransferNanosNumOps"].(float64)
			ret.SendDataPacketTransferNanosAvgTime = nameDataMap["SendDataPacketTransferNanosAvgTime"].(float64)
		}

		if nameDataMap["name"] == "Hadoop:service=DataNode,name=JvmMetrics" {
			ret.GcTimeMillis = nameDataMap["GcTimeMillis"].(float64)
			ret.GcTimeMillisParNew = nameDataMap["GcTimeMillisParNew"].(float64)
			ret.GcTimeMillisConcurrentMarkSweep = nameDataMap["GcTimeMillisConcurrentMarkSweep"].(float64)
			ret.GcCount = nameDataMap["GcCount"].(float64)
			ret.GcCountParNew = nameDataMap["GcCountParNew"].(float64)
			ret.GcCountConcurrentMarkSweep = nameDataMap["GcCountConcurrentMarkSweep"].(float64)
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
	nameSpace := "hadoop_datanode"

	ret += fmt.Sprintf("%s_heap_memory{type=\"committed\"} %g\n",
		nameSpace, s.HeapMemoryUsageCommitted)
	ret += fmt.Sprintf("%s_heap_memory{type=\"init\"} %g\n",
		nameSpace, s.HeapMemoryUsageInit)
	ret += fmt.Sprintf("%s_heap_memory{type=\"max\"} %g\n",
		nameSpace, s.HeapMemoryUsageMax)
	ret += fmt.Sprintf("%s_heap_memory{type=\"used\"} %g\n",
		nameSpace, s.HeapMemoryUsageUsed)


	ret += fmt.Sprintf("%s_bytes_written %g\n",
		nameSpace, s.BytesWritten)
	ret += fmt.Sprintf("%s_bytes_read %g\n",
		nameSpace, s.BytesRead)

	ret += fmt.Sprintf("%s_blocks_written %g\n",
		nameSpace, s.BlocksWritten)
	ret += fmt.Sprintf("%s_blocks_read %g\n",
		nameSpace, s.BlocksRead)
	ret += fmt.Sprintf("%s_blocks_replicated %g\n",
		nameSpace, s.BlocksReplicated)
	ret += fmt.Sprintf("%s_blocks_removed %g\n",
		nameSpace, s.BlocksRemoved)
	ret += fmt.Sprintf("%s_blocks_verified %g\n",
		nameSpace, s.BlocksVerified)
	ret += fmt.Sprintf("%s_block_verification_failures %g\n",
		nameSpace, s.BlockVerificationFailures)

	ret += fmt.Sprintf("%s_reads_from_local_client %g\n",
		nameSpace, s.ReadsFromLocalClient)
	ret += fmt.Sprintf("%s_reads_from_remote_client %g\n",
		nameSpace, s.ReadsFromRemoteClient)
	ret += fmt.Sprintf("%s_writes_from_local_client %g\n",
		nameSpace, s.WritesFromLocalClient)
	ret += fmt.Sprintf("%s_writes_from_remote_client %g\n",
		nameSpace, s.WritesFromRemoteClient)

	ret += fmt.Sprintf("%s_blocks_get_local_path_info %g\n",
		nameSpace, s.BlocksGetLocalPathInfo)
	ret += fmt.Sprintf("%s_fsync_count %g\n",
		nameSpace, s.FsyncCount)
	ret += fmt.Sprintf("%s_volume_failures %g\n",
		nameSpace, s.VolumeFailures)

	ret += fmt.Sprintf("%s_read_block_op_uum_ops %g\n",
		nameSpace, s.ReadBlockOpNumOps)
	ret += fmt.Sprintf("%s_read_block_op_avg_time %g\n",
		nameSpace, s.ReadBlockOpAvgTime)
	ret += fmt.Sprintf("%s_write_block_op_uum_ops %g\n",
		nameSpace, s.WriteBlockOpNumOps)
	ret += fmt.Sprintf("%s_write_block_op_avg_time %g\n",
		nameSpace, s.WriteBlockOpAvgTime)
	ret += fmt.Sprintf("%s_block_checksum_op_num_ops %g\n",
		nameSpace, s.BlockChecksumOpNumOps)
	ret += fmt.Sprintf("%s_block_checksum_op_vvg_time %g\n",
		nameSpace, s.BlockChecksumOpAvgTime)
	ret += fmt.Sprintf("%s_copy_block_op_num_ops %g\n",
		nameSpace, s.CopyBlockOpNumOps)
	ret += fmt.Sprintf("%s_copy_block_op_avg_time %g\n",
		nameSpace, s.CopyBlockOpAvgTime)
	ret += fmt.Sprintf("%s_replace_block_op_num_ops %g\n",
		nameSpace, s.ReplaceBlockOpNumOps)
	ret += fmt.Sprintf("%s_replace_block_op_avg_time %g\n",
		nameSpace, s.ReplaceBlockOpAvgTime)

	ret += fmt.Sprintf("%s_heartbeats_num_ops %g\n",
		nameSpace, s.HeartbeatsNumOps)
	ret += fmt.Sprintf("%s_heartbeats_avg_time %g\n",
		nameSpace, s.HeartbeatsAvgTime)

	ret += fmt.Sprintf("%s_block_reports_num_ops %g\n",
		nameSpace, s.BlockReportsNumOps)
	ret += fmt.Sprintf("%s_block_reports_avg_time %g\n",
		nameSpace, s.BlockReportsAvgTime)

	ret += fmt.Sprintf("%s_packet_ack_roundtrip_time_nanos_num_ops %g\n",
		nameSpace, s.PacketAckRoundTripTimeNanosNumOps)
	ret += fmt.Sprintf("%s_packet_ack_roundtrip_time_nanos_avg_time %g\n",
		nameSpace, s.PacketAckRoundTripTimeNanosAvgTime)

	ret += fmt.Sprintf("%s_flush_nanos_num_ops %g\n",
		nameSpace, s.FlushNanosNumOps)
	ret += fmt.Sprintf("%s_flush_nanos_avg_time %g\n",
		nameSpace, s.FlushNanosAvgTime)
	ret += fmt.Sprintf("%s_fsync_nanos_num_ops %g\n",
		nameSpace, s.FsyncNanosNumOps)
	ret += fmt.Sprintf("%s_fsync_nanos_avg_time %g\n",
		nameSpace, s.FsyncNanosAvgTime)

	ret += fmt.Sprintf("%s_senddata_packet_blocked_on_network_nanos_num_ops %g\n",
		nameSpace, s.SendDataPacketBlockedOnNetworkNanosNumOps)
	ret += fmt.Sprintf("%s_senddata_packet_blocked_on_network_nanos_avg_time %g\n",
		nameSpace, s.SendDataPacketBlockedOnNetworkNanosAvgTime)
	ret += fmt.Sprintf("%s_senddata_packet_transfer_nanos_num_ops %g\n",
		nameSpace, s.SendDataPacketTransferNanosNumOps)
	ret += fmt.Sprintf("%s_senddata_packet_transfer_nanos_avg_time %g\n",
		nameSpace, s.SendDataPacketTransferNanosAvgTime)



	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_total_millis %g\n",
		nameSpace, s.GcTimeMillis)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"par_new\"} %g\n",
		nameSpace, s.GcTimeMillisParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_time_millis{type=\"concurrent_mark_sweep\"} %g\n",
		nameSpace, s.GcTimeMillisConcurrentMarkSweep)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count_total %g\n",
		nameSpace, s.GcCount)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{type=\"par_new\"} %g\n",
		nameSpace, s.GcCountParNew)
	ret += fmt.Sprintf("%s_jvm_metrics_gc_count{type=\"concurrent_mark_sweep\"} %g\n",
		nameSpace, s.GcCountConcurrentMarkSweep)


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
             <head><title>Hadoop Data Node Exporter</title></head>
             <body>
             <h1>Hadoop Data Node Exporter</h1>
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

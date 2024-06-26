package aws

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/coroot/logparser"
	"k8s.io/klog"
)

const (
	logsRefreshInterval = 30 * time.Second
)

type LogReader struct {
	discoverer *Discoverer
	instanceId *string
	logs       map[string]*logFileMeta
	ch         chan<- logparser.LogEntry
	stop       chan bool
}

func NewLogReader(discoverer *Discoverer, instanceId *string, ch chan<- logparser.LogEntry) *LogReader {
	r := &LogReader{
		discoverer: discoverer,
		instanceId: instanceId,
		logs:       map[string]*logFileMeta{},
		ch:         ch,
		stop:       make(chan bool),
	}
	initialized := r.refresh(true)
	go func() {
		t := time.NewTicker(logsRefreshInterval)
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				if ok := r.refresh(!initialized); ok {
					initialized = true
				}
			}
		}
	}()
	return r
}

func (r *LogReader) Stop() {
	r.stop <- true
}

func (r *LogReader) refresh(init bool) bool {
	t := time.Now()
	defer func() {
		klog.Infoln("refreshed in", time.Since(t).Truncate(time.Millisecond))
	}()
	res, err := rds.New(r.discoverer).DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{DBInstanceIdentifier: r.instanceId})
	if err != nil {
		klog.Warning("failed to describe log files:", err)
		r.discoverer.registerError(err)
		return false
	}
	seenLogs := map[string]bool{}
	for _, f := range res.DescribeDBLogFiles {
		fileName := aws.StringValue(f.LogFileName)
		seenLogs[fileName] = true
		meta := r.logs[fileName]
		if meta == nil {
			klog.Info("new log file detected:", fileName)
			meta = &logFileMeta{}
			r.logs[fileName] = meta
		}

		if init {
			var n int64 = 1 // read last line to obtain the marker
			response, err := r.download(fileName, nil, &n)
			if err != nil {
				klog.Warning(err)
				continue
			}
			meta.lastWritten = aws.Int64Value(f.LastWritten)
			meta.marker = aws.StringValue(response.Marker)
			continue
		}

		if meta.lastWritten >= aws.Int64Value(f.LastWritten) {
			continue
		}
		response, err := r.download(fileName, &meta.marker, nil)
		if err != nil {
			klog.Warning(err)
			continue
		}
		meta.lastWritten = aws.Int64Value(f.LastWritten)
		meta.marker = aws.StringValue(response.Marker)
		r.write(response.LogFileData)
	}

	for name := range r.logs {
		if !seenLogs[name] {
			delete(r.logs, name)
		}
	}
	return true
}

func (r *LogReader) download(logFileName string, marker *string, numberOfLines *int64) (*rds.DownloadDBLogFilePortionOutput, error) {
	request := rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: r.instanceId,
		LogFileName:          &logFileName,
		Marker:               marker,
		NumberOfLines:        numberOfLines,
	}
	response, err := rds.New(r.discoverer).DownloadDBLogFilePortion(&request)
	if err != nil {
		return nil, fmt.Errorf(`failed to download file %s: %s`, logFileName, err)
	}
	return response, nil
}

func (r *LogReader) write(data *string) {
	reader := bufio.NewReader(strings.NewReader(aws.StringValue(data)))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		r.ch <- logparser.LogEntry{Content: strings.TrimSuffix(line, "\n"), Level: logparser.LevelUnknown}
	}
}

type logFileMeta struct {
	lastWritten int64
	marker      string
}

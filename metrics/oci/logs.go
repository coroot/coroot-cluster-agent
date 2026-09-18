package oci

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logparser"
	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/oracle/oci-go-sdk/v65/loggingsearch"
	"k8s.io/klog"
)

const logsRefreshInterval = 30 * time.Second

type LogReader struct {
	discoverer *Discoverer
	resource   string
	subject    string
	since      time.Time
	seen       map[string]bool
	parser     *logparser.Parser
	emitter    *common.LogEmitter
	ch         chan logparser.LogEntry
	stop       chan struct{}
}

func NewLogReader(discoverer *Discoverer, resource, subject, serviceName, hostName string, forward bool) *LogReader {
	r := &LogReader{
		discoverer: discoverer,
		resource:   resource,
		subject:    subject,
		since:      time.Now(),
		seen:       map[string]bool{},
		ch:         make(chan logparser.LogEntry),
		stop:       make(chan struct{}),
	}
	var onMsg logparser.OnMsgCallbackF
	if forward {
		emitter, err := common.NewLogEmitter(serviceName, hostName)
		if err != nil {
			klog.Errorln("failed to create the log emitter, logs won't be forwarded:", err)
		} else {
			r.emitter = emitter
			onMsg = emitter.Callback()
		}
	}
	r.parser = logparser.NewParser(r.ch, nil, onMsg, common.MultilineCollectorTimeout, common.LogPatternsPerLevel, false, nil)
	go func() {
		t := time.NewTicker(logsRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				r.refresh()
			}
		}
	}()
	return r
}

func (r *LogReader) Stop() {
	close(r.stop)
	r.parser.Stop()
	if r.emitter != nil {
		r.emitter.Stop()
	}
}

func (r *LogReader) Counters() []logparser.LogCounter {
	return r.parser.GetCounters()
}

type logEntry struct {
	Data struct {
		LogContent struct {
			Id   string    `json:"id"`
			Time time.Time `json:"time"`
			Data struct {
				Level   string `json:"level"`
				Msg     string `json:"msg"`     // PostgreSQL
				Message string `json:"message"` // OCI Cache
			} `json:"data"`
		} `json:"logContent"`
	} `json:"data"`
}

func (r *LogReader) refresh() {
	d := r.discoverer
	log := d.serviceLog(r.resource)
	if log == "" { // no service log enabled for the resource
		return
	}
	query := fmt.Sprintf(`search "%s" | sort by datetime asc`, log)
	if r.subject != "" {
		query = fmt.Sprintf(`search "%s" | where subject = '%s' | sort by datetime asc`, log, r.subject)
	}
	req := loggingsearch.SearchLogsRequest{
		SearchLogsDetails: loggingsearch.SearchLogsDetails{
			SearchQuery: &query,
			TimeStart:   &ocicommon.SDKTime{Time: r.since},
			TimeEnd:     &ocicommon.SDKTime{Time: time.Now()},
		},
		Limit:           ocicommon.Int(1000),
		RequestMetadata: retry(),
	}
	since, seen := r.since, r.seen
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.logSearchClient.SearchLogs(ctx, req)
		cancel()
		if err != nil {
			d.registerError(err)
			return
		}
		var entries []logEntry
		if data, err := json.Marshal(resp.Results); err == nil {
			_ = json.Unmarshal(data, &entries)
		}
		for _, e := range entries {
			c := e.Data.LogContent
			if c.Time.Equal(since) && seen[c.Id] {
				continue // fetched last time
			}
			switch {
			case c.Time.After(r.since):
				r.since, r.seen = c.Time, map[string]bool{c.Id: true}
			case c.Time.Equal(r.since):
				r.seen[c.Id] = true
			}
			msg := c.Data.Msg
			if msg == "" {
				msg = c.Data.Message
			}
			if msg == "" {
				continue
			}
			select {
			case r.ch <- logparser.LogEntry{Timestamp: c.Time, Content: msg, Level: levelFromString(c.Data.Level)}:
			case <-r.stop:
				return
			}
		}
		if resp.OpcNextPage == nil {
			return
		}
		req.Page = resp.OpcNextPage
	}
}

func (d *Discoverer) discoverServiceLogs() {
	logs := map[string]string{}
	for _, compartment := range d.compartments {
		if !d.listServiceLogs(compartment, logs) {
			return
		}
	}
	d.serviceLogsLock.Lock()
	d.serviceLogs = logs
	d.serviceLogsLock.Unlock()
}

func (d *Discoverer) listServiceLogs(compartment string, logs map[string]string) bool {
	groups := logging.ListLogGroupsRequest{CompartmentId: &compartment, RequestMetadata: retry()}
	for {
		ctx, cancel := d.apiContext()
		resp, err := d.loggingClient.ListLogGroups(ctx, groups)
		cancel()
		if err != nil {
			d.registerError(err)
			return false
		}
		for _, g := range resp.Items {
			req := logging.ListLogsRequest{LogGroupId: g.Id, LogType: logging.ListLogsLogTypeService, LifecycleState: logging.ListLogsLifecycleStateActive, RequestMetadata: retry()}
			for {
				ctx, cancel := d.apiContext()
				resp, err := d.loggingClient.ListLogs(ctx, req)
				cancel()
				if err != nil {
					d.registerError(err)
					return false
				}
				for _, l := range resp.Items {
					if l.Configuration == nil || l.IsEnabled != nil && !*l.IsEnabled {
						continue
					}
					if src, ok := l.Configuration.Source.(logging.OciService); ok {
						logs[str(src.Resource)] = compartment + "/" + str(g.Id) + "/" + str(l.Id)
					}
				}
				if resp.OpcNextPage == nil {
					break
				}
				req.Page = resp.OpcNextPage
			}
		}
		if resp.OpcNextPage == nil {
			return true
		}
		groups.Page = resp.OpcNextPage
	}
}

func (d *Discoverer) serviceLog(resource string) string {
	d.serviceLogsLock.RLock()
	defer d.serviceLogsLock.RUnlock()
	return d.serviceLogs[resource]
}

func levelFromString(level string) logparser.Level {
	switch level {
	case "DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5", "debug":
		return logparser.LevelDebug
	case "LOG", "INFO", "NOTICE", "info", "notice":
		return logparser.LevelInfo
	case "WARNING", "warning":
		return logparser.LevelWarning
	case "ERROR", "error":
		return logparser.LevelError
	case "FATAL", "PANIC", "fatal", "panic":
		return logparser.LevelCritical
	}
	return logparser.LevelUnknown
}

package gcp

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logparser"
	logging "google.golang.org/api/logging/v2"
	"k8s.io/klog"
)

const logsRefreshInterval = 30 * time.Second

type LogReader struct {
	discoverer *Discoverer
	instance   string
	since      time.Time       // receiveTimestamp of the newest entry fetched so far
	seen       map[string]bool // insertIds of the entries received exactly at `since`, not to fetch them again
	parser     *logparser.Parser
	emitter    *common.LogEmitter
	ch         chan logparser.LogEntry
	stop       chan struct{}
}

func NewLogReader(discoverer *Discoverer, instance string, forward bool) *LogReader {
	r := &LogReader{
		discoverer: discoverer,
		instance:   instance,
		since:      time.Now(),
		seen:       map[string]bool{},
		ch:         make(chan logparser.LogEntry),
		stop:       make(chan struct{}),
	}
	var onMsg logparser.OnMsgCallbackF
	if forward {
		emitter, err := common.NewLogEmitter("/gcp/cloudsql/"+discoverer.project+"/"+instance, "cloudsql:"+instance)
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

func (r *LogReader) refresh() {
	d := r.discoverer
	filter := fmt.Sprintf(`resource.type="cloudsql_database" AND resource.labels.database_id="%s:%s" AND receiveTimestamp>="%s"`,
		d.project, r.instance, r.since.UTC().Format(time.RFC3339Nano))
	req := &logging.ListLogEntriesRequest{
		ResourceNames: []string{"projects/" + d.project},
		Filter:        filter,
		OrderBy:       "timestamp asc",
		PageSize:      1000,
	}
	since, seen := r.since, r.seen
	ctx, cancel := d.apiContext()
	defer cancel()
	err := d.loggingClient.Entries.List(req).Pages(ctx, func(page *logging.ListLogEntriesResponse) error {
		for _, e := range page.Entries {
			received := parseTime(e.ReceiveTimestamp)
			if received.Equal(since) && seen[e.InsertId] {
				continue // fetched last time
			}
			switch {
			case received.After(r.since):
				r.since, r.seen = received, map[string]bool{e.InsertId: true}
			case received.Equal(r.since):
				r.seen[e.InsertId] = true
			}
			ts := parseTime(e.Timestamp)
			msg := e.TextPayload
			if msg == "" && len(e.JsonPayload) > 0 {
				var payload struct {
					Message string `json:"message"`
				}
				if json.Unmarshal(e.JsonPayload, &payload) == nil {
					msg = payload.Message
				}
			}
			if msg == "" {
				continue
			}
			select {
			case r.ch <- logparser.LogEntry{Timestamp: ts, Content: msg, Level: severityToLevel(e.Severity)}:
			case <-r.stop:
				return nil
			}
		}
		return nil
	})
	if err != nil {
		d.registerError(err)
	}
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Now()
	}
	return t
}

func severityToLevel(severity string) logparser.Level {
	switch severity {
	case "DEBUG":
		return logparser.LevelDebug
	case "INFO", "NOTICE", "DEFAULT":
		return logparser.LevelInfo
	case "WARNING":
		return logparser.LevelWarning
	case "ERROR":
		return logparser.LevelError
	case "CRITICAL", "ALERT", "EMERGENCY":
		return logparser.LevelCritical
	}
	return logparser.LevelUnknown
}

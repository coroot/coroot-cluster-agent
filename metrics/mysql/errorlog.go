package mysql

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/coroot/logparser"
	"github.com/go-sql-driver/mysql"
)

const errorLogRefreshInterval = 30 * time.Second

type ErrorLogReader struct {
	db      *sql.DB
	since   string
	parser  *logparser.Parser
	emitter *common.LogEmitter
	ch      chan logparser.LogEntry
	stop    chan struct{}
	logger  logger.Logger
}

func (c *Collector) StartErrorLog(serviceName, hostName string) {
	if c.errorLog != nil { // already started
		return
	}
	emitter, err := common.NewLogEmitter(serviceName, hostName)
	if err != nil {
		c.logger.Warning("failed to create the log emitter, the error log won't be forwarded:", err)
		return
	}
	r := &ErrorLogReader{
		db:      c.db,
		emitter: emitter,
		ch:      make(chan logparser.LogEntry),
		stop:    make(chan struct{}),
		logger:  c.logger,
	}
	r.parser = logparser.NewParser(r.ch, nil, emitter.Callback(), common.MultilineCollectorTimeout, common.LogPatternsPerLevel, false, nil)
	c.errorLog = r
	go func() {
		t := time.NewTicker(errorLogRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				if !r.refresh(c.ctx, c.collectTimeout) {
					return
				}
			}
		}
	}()
}

func (c *Collector) ErrorLogCounters() []logparser.LogCounter {
	if c.errorLog == nil {
		return nil
	}
	return c.errorLog.parser.GetCounters()
}

func (r *ErrorLogReader) Stop() {
	close(r.stop)
	r.parser.Stop()
	r.emitter.Stop()
}

func (r *ErrorLogReader) refresh(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if r.since == "" {
		if err := r.db.QueryRowContext(ctx, `SELECT DATE_FORMAT(NOW(6), '%Y-%m-%d %H:%i:%s.%f')`).Scan(&r.since); err != nil {
			r.logger.Warning("failed to read the server time:", err)
			return true
		}
	}
	rows, err := r.db.QueryContext(ctx, `SELECT LOGGED, UNIX_TIMESTAMP(LOGGED), PRIO, DATA FROM performance_schema.error_log WHERE LOGGED > ? ORDER BY LOGGED LIMIT 1000`, r.since)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1146 || mysqlErr.Number == 1142) { // no such table, command denied
			r.logger.Warning("the error log won't be read:", err)
			return false
		}
		r.logger.Warning("failed to read performance_schema.error_log:", err)
		return true
	}
	defer rows.Close()
	for rows.Next() {
		var logged, prio, data string
		var unix float64
		if err := rows.Scan(&logged, &unix, &prio, &data); err != nil { // the columns are NOT NULL
			r.logger.Warning("failed to scan performance_schema.error_log row:", err)
			continue
		}
		if logged > r.since {
			r.since = logged
		}
		select {
		case r.ch <- logparser.LogEntry{Timestamp: time.Unix(0, int64(unix*1e9)), Content: data, Level: errorLogLevel(prio)}:
		case <-r.stop:
			return false
		}
	}
	return true
}

func errorLogLevel(prio string) logparser.Level {
	switch prio {
	case "Error":
		return logparser.LevelError
	case "Warning":
		return logparser.LevelWarning
	case "Note", "System":
		return logparser.LevelInfo
	}
	return logparser.LevelUnknown
}

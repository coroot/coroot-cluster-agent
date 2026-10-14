package mysql

import (
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dUp          = common.Desc("mysql_up", "")
	dScrapeError = common.Desc("mysql_scrape_error", "", "error", "warning")
	dInfo        = common.Desc("mysql_info", "", "server_version", "server_id", "server_uuid")
	dPerfSchema  = common.Desc("mysql_performance_schema_enabled", "Whether performance_schema is enabled (1 = ON). Per-query statistics, table I/O waits and lock waits are collected from it")

	dQueryCalls        = common.Desc("mysql_top_query_calls_per_second", "", "schema", "query")
	dQueryTotalTime    = common.Desc("mysql_top_query_time_per_second", "", "schema", "query")
	dQueryLockTime     = common.Desc("mysql_top_query_lock_time_per_second", "", "schema", "query")
	dQueryRowsExamined = common.Desc("mysql_top_query_rows_examined_per_second", "Rows examined per second by the query (vs rows returned: a high ratio means a missing index)", "schema", "query")
	dQueryRowsSent     = common.Desc("mysql_top_query_rows_sent_per_second", "Rows returned per second by the query", "schema", "query")

	dTmpDiskTables = common.Desc("mysql_created_tmp_disk_tables_total", "Number of internal on-disk temporary tables created (sorts/joins spilling to disk)")

	dLockedQueries       = common.Desc("mysql_locked_queries", "Number of queries currently waiting for a lock", "schema", "query")
	dLockAwaitingQueries = common.Desc("mysql_lock_awaiting_queries", "Number of queries currently awaiting a lock, by the blocking query", "schema", "blocking_query")

	dInnodbTransactionSeconds = common.Desc("mysql_innodb_transaction_seconds", "Age of the longest-running active InnoDB transactions, by query shape", "query")

	dReplicationIORunning  = common.Desc("mysql_replication_io_status", "", "source_server_id", "source_server_uuid", "state", "last_error")
	dReplicationSQLRunning = common.Desc("mysql_replication_sql_status", "", "source_server_id", "source_server_uuid", "state", "last_error")
	dReplicationLag        = common.Desc("mysql_replication_lag_seconds", "", "source_server_id", "source_server_uuid")

	dConnectionsMax                 = common.Desc("mysql_connections_max", "")
	dConnectionsCurrent             = common.Desc("mysql_connections_current", "")
	dConnectionsTotal               = common.Desc("mysql_connections_total", "")
	dConnectionsAborted             = common.Desc("mysql_connections_aborted_total", "")
	dConnectionErrorsMaxConnections = common.Desc("mysql_connection_errors_max_connections_total", "Number of connections refused because max_connections was reached")

	dThreadsRunning = common.Desc("mysql_threads_running", "Number of threads that are not sleeping")

	dBytesReceived = common.Desc("mysql_traffic_received_bytes_total", "")
	dBytesSent     = common.Desc("mysql_traffic_sent_bytes_total", "")

	dQueries     = common.Desc("mysql_queries_total", "")
	dSlowQueries = common.Desc("mysql_slow_queries_total", "")

	dIOTime = common.Desc("mysql_top_table_io_wait_time_per_second", "", "schema", "table", "operation")

	dDbSize          = common.Desc("mysql_database_size_bytes", "Total size of the database in bytes", "db")
	dTableSize       = common.Desc("mysql_table_size_bytes", "Total size of the table in bytes", "db", "table")
	dTableSizeGrowth = common.Desc("mysql_table_size_growth_bytes_per_second", "Table size growth rate in bytes per second", "db", "table")

	dWsrepClusterSize   = common.Desc("mysql_wsrep_cluster_size", "Number of nodes in the Galera cluster component")
	dWsrepClusterStatus = common.Desc("mysql_wsrep_cluster_status", "Status of the cluster component this node is in (Primary means it has quorum)", "status")
	dWsrepLocalState    = common.Desc("mysql_wsrep_local_state", "Current Galera node state (1), with the state name in the label", "state")
	dWsrepReady         = common.Desc("mysql_wsrep_ready", "Whether the node can accept queries (1 = ON)")
	dWsrepConnected     = common.Desc("mysql_wsrep_connected", "Whether the node is connected to the cluster (1 = ON)")

	dWsrepFlowControlPaused = common.Desc("mysql_wsrep_flow_control_paused_seconds_total", "Total time writes were paused by flow control")
	dWsrepLocalRecvQueue    = common.Desc("mysql_wsrep_local_recv_queue", "Current number of write-sets waiting to be applied on this node")
	dWsrepLocalSendQueue    = common.Desc("mysql_wsrep_local_send_queue", "Current number of write-sets waiting to be sent from this node")
	dWsrepCertFailures      = common.Desc("mysql_wsrep_local_cert_failures_total", "Number of transactions that failed certification (write conflicts)")
	dWsrepBfAborts          = common.Desc("mysql_wsrep_local_bf_aborts_total", "Number of local transactions aborted by replication (brute-force aborts)")

	dGrMemberState                      = common.Desc("mysql_group_replication_member_state", "Group Replication state of this member (1), with the state name in the label", "state")
	dGrClusterSize                      = common.Desc("mysql_group_replication_cluster_size", "Number of members in the Group Replication group")
	dGrMembersOnline                    = common.Desc("mysql_group_replication_members_online", "Number of members currently ONLINE in the group")
	dGrTransactionsInQueue              = common.Desc("mysql_group_replication_transactions_in_queue", "Transactions waiting in the certification queue on this member")
	dGrTransactionsRemoteInApplierQueue = common.Desc("mysql_group_replication_transactions_remote_in_applier_queue", "Remote transactions waiting to be applied on this member")
	dGrConflictsDetected                = common.Desc("mysql_group_replication_conflicts_detected_total", "Number of transactions that failed certification (conflicts) on this member")

	dInnodbBufferPoolReadRequests  = common.Desc("mysql_innodb_buffer_pool_read_requests_total", "Logical read requests to the InnoDB buffer pool")
	dInnodbBufferPoolReads         = common.Desc("mysql_innodb_buffer_pool_reads_total", "Read requests that could not be satisfied from the buffer pool and went to disk")
	dInnodbBufferPoolWriteRequests = common.Desc("mysql_innodb_buffer_pool_write_requests_total", "Writes done to the InnoDB buffer pool")
	dInnodbBufferPoolPagesTotal    = common.Desc("mysql_innodb_buffer_pool_pages_total", "Total pages in the InnoDB buffer pool")
	dInnodbBufferPoolPagesFree     = common.Desc("mysql_innodb_buffer_pool_pages_free", "Free pages in the InnoDB buffer pool")
	dInnodbBufferPoolPagesDirty    = common.Desc("mysql_innodb_buffer_pool_pages_dirty", "Dirty (modified, not yet flushed) pages in the buffer pool")
	dInnodbBufferPoolPagesData     = common.Desc("mysql_innodb_buffer_pool_pages_data", "Pages holding data (clean + dirty) in the buffer pool")
	dInnodbBufferPoolWaitFree      = common.Desc("mysql_innodb_buffer_pool_wait_free_total", "Times a write had to wait for a free buffer-pool page (buffer pool pressure)")
	dInnodbBufferPoolPagesFlushed  = common.Desc("mysql_innodb_buffer_pool_pages_flushed_total", "Buffer-pool pages flushed to disk")
	dInnodbPageSize                = common.Desc("mysql_innodb_page_size_bytes", "InnoDB page size in bytes (converts buffer-pool page counts to bytes)")

	dInnodbRowsRead     = common.Desc("mysql_innodb_rows_read_total", "Rows read by InnoDB")
	dInnodbRowsInserted = common.Desc("mysql_innodb_rows_inserted_total", "Rows inserted by InnoDB")
	dInnodbRowsUpdated  = common.Desc("mysql_innodb_rows_updated_total", "Rows updated by InnoDB")
	dInnodbRowsDeleted  = common.Desc("mysql_innodb_rows_deleted_total", "Rows deleted by InnoDB")

	dInnodbRowLockWaits        = common.Desc("mysql_innodb_row_lock_waits_total", "Number of times a row lock had to be waited for")
	dInnodbRowLockTime         = common.Desc("mysql_innodb_row_lock_time_seconds_total", "Total time spent waiting for InnoDB row locks, seconds")
	dInnodbRowLockCurrentWaits = common.Desc("mysql_innodb_row_lock_current_waits", "Row locks currently being waited for")

	dInnodbDataReads   = common.Desc("mysql_innodb_data_reads_total", "InnoDB data read operations (OS reads)")
	dInnodbDataWrites  = common.Desc("mysql_innodb_data_writes_total", "InnoDB data write operations (OS writes)")
	dInnodbDataRead    = common.Desc("mysql_innodb_data_read_bytes_total", "Bytes read by InnoDB")
	dInnodbDataWritten = common.Desc("mysql_innodb_data_written_bytes_total", "Bytes written by InnoDB")
	dInnodbDataFsyncs  = common.Desc("mysql_innodb_data_fsyncs_total", "InnoDB fsync() operations")

	dInnodbLogWaits     = common.Desc("mysql_innodb_log_waits_total", "Times a write had to wait for the redo log buffer to be flushed")
	dInnodbOsLogWritten = common.Desc("mysql_innodb_os_log_written_bytes_total", "Bytes written to the InnoDB redo log")

	dSortMergePasses = common.Desc("mysql_sort_merge_passes_total", "Merge passes for sorts that spilled to disk (sort_buffer_size too small)")

	dComCommit   = common.Desc("mysql_commit_total", "COMMIT statements executed")
	dComRollback = common.Desc("mysql_rollback_total", "ROLLBACK statements executed")

	dInnodbDeadlocks         = common.Desc("mysql_innodb_deadlocks_total", "Transactions rolled back by InnoDB deadlocks")
	dInnodbLockWaitTimeouts  = common.Desc("mysql_innodb_lock_wait_timeouts_total", "Transactions rolled back after waiting innodb_lock_wait_timeout for a row lock")
	dInnodbHistoryListLength = common.Desc("mysql_innodb_history_list_length", "Undo records not yet purged (history list length); grows while a long transaction holds them")

	dBinlogSize          = common.Desc("mysql_binlog_size_bytes", "Total size of the binary logs on disk")
	dBinlogFiles         = common.Desc("mysql_binlog_files", "Number of binary log files on disk")
	dBinlogExpireSeconds = common.Desc("mysql_binlog_expire_seconds", "binlog_expire_logs_seconds; 0 means binary logs are never purged automatically")
	dUndoSize            = common.Desc("mysql_undo_size_bytes", "Total size of the InnoDB undo tablespaces on disk")

	dTableLocksWaited    = common.Desc("mysql_table_locks_waited_total", "Table-lock requests that had to wait (LOCK TABLES, MyISAM, DDL metadata locks)")
	dTableLocksImmediate = common.Desc("mysql_table_locks_immediate_total", "Table-lock requests granted immediately")
)

var galeraDescs = []*prometheus.Desc{
	dWsrepClusterSize, dWsrepClusterStatus, dWsrepLocalState, dWsrepReady, dWsrepConnected,
	dWsrepFlowControlPaused, dWsrepLocalRecvQueue, dWsrepLocalSendQueue,
	dWsrepCertFailures, dWsrepBfAborts,
}

var groupReplicationDescs = []*prometheus.Desc{
	dGrMemberState, dGrClusterSize, dGrMembersOnline,
	dGrTransactionsInQueue, dGrTransactionsRemoteInApplierQueue, dGrConflictsDetected,
}

var innodbDescs = []*prometheus.Desc{
	dInnodbBufferPoolReadRequests, dInnodbBufferPoolReads, dInnodbBufferPoolWriteRequests,
	dInnodbBufferPoolPagesTotal, dInnodbBufferPoolPagesFree, dInnodbBufferPoolPagesDirty, dInnodbBufferPoolPagesData,
	dInnodbBufferPoolWaitFree, dInnodbBufferPoolPagesFlushed, dInnodbPageSize,
	dInnodbRowsRead, dInnodbRowsInserted, dInnodbRowsUpdated, dInnodbRowsDeleted,
	dInnodbRowLockWaits, dInnodbRowLockTime, dInnodbRowLockCurrentWaits,
	dInnodbDataReads, dInnodbDataWrites, dInnodbDataRead, dInnodbDataWritten, dInnodbDataFsyncs,
	dInnodbLogWaits, dInnodbOsLogWritten,
	dSortMergePasses,
	dComCommit, dComRollback,
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dScrapeError
	ch <- dInfo
	ch <- dQueryCalls
	ch <- dQueryTotalTime
	ch <- dQueryLockTime
	ch <- dQueryRowsExamined
	ch <- dQueryRowsSent
	ch <- dTmpDiskTables
	ch <- dLockedQueries
	ch <- dLockAwaitingQueries
	ch <- dInnodbTransactionSeconds
	ch <- dReplicationIORunning
	ch <- dReplicationSQLRunning
	ch <- dReplicationLag
	ch <- dConnectionsMax
	ch <- dConnectionsCurrent
	ch <- dConnectionsTotal
	ch <- dConnectionsAborted
	ch <- dConnectionErrorsMaxConnections
	ch <- dThreadsRunning
	ch <- dBinlogSize
	ch <- dBinlogFiles
	ch <- dBinlogExpireSeconds
	ch <- dUndoSize
	ch <- dTableLocksWaited
	ch <- dTableLocksImmediate
	ch <- dInnodbDeadlocks
	ch <- dInnodbLockWaitTimeouts
	ch <- dInnodbHistoryListLength
	ch <- dBytesReceived
	ch <- dBytesSent
	ch <- dQueries
	ch <- dSlowQueries
	ch <- dIOTime
	ch <- dDbSize
	ch <- dTableSize
	ch <- dTableSizeGrowth
	for _, d := range galeraDescs {
		ch <- d
	}
	for _, d := range groupReplicationDescs {
		ch <- d
	}
	for _, d := range innodbDescs {
		ch <- d
	}
}

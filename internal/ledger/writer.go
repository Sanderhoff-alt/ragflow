//
//  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package ledger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ragflow/internal/common"
	"ragflow/internal/entity"
)

const (
	DefaultBufferSize         = 10000
	DefaultBatchSize          = 100
	DefaultFlushPeriod        = 500 * time.Millisecond
	DefaultBatchWorkers       = 2
	DefaultMaxInFlightBatches = 4
	DefaultBatchTimeout       = 5 * time.Second
	DefaultShutdownTimeout    = 5 * time.Second
	MaxRawDetailsBytes        = 32 * 1024 // 32 KB limit on serialized raw provider telemetry
	MaxErrorMessageBytes      = 512
)

var (
	defaultWriter *AsyncLedgerWriter
	writerMu      sync.RWMutex
	defaultDB     *gorm.DB
)

// StatsSnapshot captures point-in-time telemetry counters for the ledger writer.
type StatsSnapshot struct {
	Enqueued  uint64 `json:"enqueued"`
	Written   uint64 `json:"written"`
	Failed    uint64 `json:"failed"`
	Dropped   uint64 `json:"dropped"`
	Unwritten uint64 `json:"unwritten"`
}

// WriterOption configures AsyncLedgerWriter parameters.
type WriterOption func(*AsyncLedgerWriter)

// WithBufferSize sets the capacity of the in-memory record channel.
func WithBufferSize(size int) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if size > 0 {
			w.bufferSize = size
		}
	}
}

// WithBatchSize sets the maximum number of records per batch.
func WithBatchSize(size int) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if size > 0 {
			w.batchSize = size
		}
	}
}

// WithFlushPeriod sets the interval between periodic batch flushes.
func WithFlushPeriod(period time.Duration) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if period > 0 {
			w.flushPeriod = period
		}
	}
}

// WithBatchWorkers sets the number of concurrent batch-writing worker goroutines.
func WithBatchWorkers(workers int) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if workers > 0 {
			w.batchWorkers = workers
		}
	}
}

// WithMaxInFlightBatches sets the capacity of the batch channel feeding workers.
func WithMaxInFlightBatches(max int) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if max > 0 {
			w.maxInFlightBatches = max
		}
	}
}

// WithBatchTimeout sets the unified deadline for an entire batch (including item fallback).
func WithBatchTimeout(timeout time.Duration) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if timeout > 0 {
			w.batchTimeout = timeout
		}
	}
}

// WithShutdownTimeout sets the maximum time allowed for graceful drain on shutdown.
func WithShutdownTimeout(timeout time.Duration) WriterOption {
	return func(w *AsyncLedgerWriter) {
		if timeout > 0 {
			w.shutdownTimeout = timeout
		}
	}
}

// withBeforeWrite is an internal test hook invoked by a worker right before executing writeBatch.
func withBeforeWrite(fn func(batch []*entity.TenantTokenLedger)) WriterOption {
	return func(w *AsyncLedgerWriter) {
		w.beforeWrite = fn
	}
}

// AsyncLedgerWriter decouples batch cutting from DB writes with bounded concurrency.
// Callers enqueue snapshots non-blockingly; a collector cuts batches by count/time,
// and a fixed worker pool persists batches to the shared GORM database connection pool.
type AsyncLedgerWriter struct {
	db                 *gorm.DB
	queue              chan *entity.TenantTokenLedger
	batchCh            chan []*entity.TenantTokenLedger
	flushCh            chan chan struct{}
	bufferSize         int
	batchSize          int
	flushPeriod        time.Duration
	batchWorkers       int
	maxInFlightBatches int
	batchTimeout       time.Duration
	shutdownTimeout    time.Duration

	shutdownCtx    context.Context
	cancelShutdown context.CancelFunc

	mu        sync.RWMutex
	closed    atomic.Bool
	closeOnce sync.Once

	collectorWg sync.WaitGroup
	workersWg   sync.WaitGroup

	inFlightBatches atomic.Int64

	// Observability metrics
	enqueuedRecords  atomic.Uint64
	writtenRecords   atomic.Uint64
	failedRecords    atomic.Uint64
	droppedRecords   atomic.Uint64
	unwrittenRecords atomic.Uint64

	beforeWrite func(batch []*entity.TenantTokenLedger)
}

// NewAsyncLedgerWriter initializes and starts the collector and worker pool.
func NewAsyncLedgerWriter(db *gorm.DB, opts ...WriterOption) *AsyncLedgerWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &AsyncLedgerWriter{
		db:                 db,
		bufferSize:         DefaultBufferSize,
		batchSize:          DefaultBatchSize,
		flushPeriod:        DefaultFlushPeriod,
		batchWorkers:       DefaultBatchWorkers,
		maxInFlightBatches: DefaultMaxInFlightBatches,
		batchTimeout:       DefaultBatchTimeout,
		shutdownTimeout:    DefaultShutdownTimeout,
		shutdownCtx:        ctx,
		cancelShutdown:     cancel,
		flushCh:            make(chan chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}

	w.queue = make(chan *entity.TenantTokenLedger, w.bufferSize)
	w.batchCh = make(chan []*entity.TenantTokenLedger, w.maxInFlightBatches)

	w.collectorWg.Add(1)
	go w.runCollector()

	for i := 0; i < w.batchWorkers; i++ {
		w.workersWg.Add(1)
		go w.runWorker()
	}

	return w
}

// Push adds a snapshot record to the buffer without blocking callers.
// If the buffer is full or the writer is closed, it drops the record and increments droppedRecords.
func (w *AsyncLedgerWriter) Push(record *entity.TenantTokenLedger) bool {
	if record == nil {
		return false
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed.Load() {
		w.droppedRecords.Add(1)
		return false
	}

	select {
	case w.queue <- record:
		w.enqueuedRecords.Add(1)
		return true
	default:
		dropped := w.droppedRecords.Add(1)
		if dropped == 1 || dropped%1000 == 0 {
			common.Warn("ledger: queue full, dropping record",
				zap.Uint64("dropped_total", dropped),
				zap.String("record_id", record.ID),
				zap.String("tenant_id", record.TenantID),
			)
		}
		return false
	}
}

// Flush synchronously drains any currently queued records to the database.
func (w *AsyncLedgerWriter) Flush() {
	if w == nil || w.closed.Load() {
		return
	}
	done := make(chan struct{})
	select {
	case w.flushCh <- done:
		select {
		case <-done:
		case <-time.After(w.batchTimeout):
			return
		}
	case <-time.After(w.batchTimeout):
		return
	}

	// Wait for any in-flight batches to finish writing
	deadline := time.Now().Add(w.batchTimeout)
	for w.inFlightBatches.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

// Close gracefully stops the writer: closes input, drains queue and in-flight batches,
// and cancels active database operations if the total shutdown timeout is exceeded.
func (w *AsyncLedgerWriter) Close() {
	if w == nil {
		return
	}
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed.Store(true)
		close(w.queue)
		w.mu.Unlock()

		waitCh := make(chan struct{})
		go func() {
			w.collectorWg.Wait()
			w.workersWg.Wait()
			close(waitCh)
		}()

		select {
		case <-waitCh:
			// Normal clean exit
		case <-time.After(w.shutdownTimeout):
			// Timeout reached: abort any in-progress database requests immediately
			w.cancelShutdown()

			// Count remaining unwritten items
			var unwritten uint64
			for rec := range w.queue {
				if rec != nil {
					unwritten++
				}
			}
			for len(w.batchCh) > 0 {
				select {
				case b := <-w.batchCh:
					unwritten += uint64(len(b))
				default:
					break
				}
			}
			inFlight := w.inFlightBatches.Load()
			if inFlight > 0 {
				unwritten += uint64(inFlight) * uint64(w.batchSize)
			}
			w.unwrittenRecords.Add(unwritten)

			common.Warn("ledger: shutdown timed out, unwritten records may be lost",
				zap.Uint64("unwritten_count", unwritten),
				zap.Duration("timeout", w.shutdownTimeout),
			)
		}
	})
}

// Stats returns a snapshot of writer telemetry counters.
func (w *AsyncLedgerWriter) Stats() StatsSnapshot {
	if w == nil {
		return StatsSnapshot{}
	}
	return StatsSnapshot{
		Enqueued:  w.enqueuedRecords.Load(),
		Written:   w.writtenRecords.Load(),
		Failed:    w.failedRecords.Load(),
		Dropped:   w.droppedRecords.Load(),
		Unwritten: w.unwrittenRecords.Load(),
	}
}

func (w *AsyncLedgerWriter) runCollector() {
	defer w.collectorWg.Done()
	batch := make([]*entity.TenantTokenLedger, 0, w.batchSize)
	ticker := time.NewTicker(w.flushPeriod)
	defer ticker.Stop()

	sendBatch := func() {
		if len(batch) == 0 {
			return
		}
		b := batch
		batch = make([]*entity.TenantTokenLedger, 0, w.batchSize)
		w.inFlightBatches.Add(1)
		select {
		case w.batchCh <- b:
		case <-w.shutdownCtx.Done():
			w.inFlightBatches.Add(-1)
			w.unwrittenRecords.Add(uint64(len(b)))
		}
	}

	for {
		select {
		case rec, ok := <-w.queue:
			if !ok {
				// Queue closed: send remaining and close batchCh to notify workers
				sendBatch()
				close(w.batchCh)
				return
			}
			batch = append(batch, rec)
			if len(batch) >= w.batchSize {
				sendBatch()
			}
		case <-ticker.C:
			sendBatch()
		case done := <-w.flushCh:
			for len(w.queue) > 0 {
				rec, ok := <-w.queue
				if !ok {
					break
				}
				batch = append(batch, rec)
				if len(batch) >= w.batchSize {
					sendBatch()
				}
			}
			sendBatch()
			close(done)
		}
	}
}

func (w *AsyncLedgerWriter) runWorker() {
	defer w.workersWg.Done()
	for batch := range w.batchCh {
		if w.beforeWrite != nil {
			w.beforeWrite(batch)
		}
		w.writeBatch(batch)
		w.inFlightBatches.Add(-1)
	}
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "driver: bad connection") ||
		strings.Contains(msg, "database is closed") ||
		strings.Contains(msg, "sql: database is closed")
}

func (w *AsyncLedgerWriter) writeBatch(batch []*entity.TenantTokenLedger) {
	if len(batch) == 0 || w.db == nil {
		return
	}

	// Unified total deadline for this entire batch, including any fallback retries
	ctx, cancel := context.WithTimeout(w.shutdownCtx, w.batchTimeout)
	defer cancel()

	// Fast path: attempt batch insert with OnConflict DoNothing
	err := w.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(batch, len(batch)).Error
	if err == nil {
		w.writtenRecords.Add(uint64(len(batch)))
		return
	}

	// If failure is due to timeout, cancellation, or DB connectivity, do NOT repeat item by item
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || isConnectionError(err) {
		w.failedRecords.Add(uint64(len(batch)))
		common.Warn("ledger: batch insert failed due to database connectivity/timeout, skipping item fallback",
			zap.Int("batch_size", len(batch)),
			zap.Error(err),
		)
		return
	}

	// Slow path: batch insert failed (likely due to a bad record/data constraint).
	// Fall back to item-by-item insertion under the SAME ctx and remaining deadline.
	common.Warn("ledger: batch insert failed, falling back to item-by-item persistence",
		zap.Int("batch_size", len(batch)),
		zap.Error(err),
	)

	for i, rec := range batch {
		if ctx.Err() != nil {
			remaining := len(batch) - i
			w.failedRecords.Add(uint64(remaining))
			common.Warn("ledger: batch item fallback deadline expired, remaining items aborted",
				zap.Int("unprocessed_count", remaining),
				zap.Error(ctx.Err()),
			)
			return
		}

		itemErr := w.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(rec).Error
		if itemErr != nil {
			w.failedRecords.Add(1)
			if errors.Is(itemErr, context.DeadlineExceeded) || errors.Is(itemErr, context.Canceled) || isConnectionError(itemErr) {
				remaining := len(batch) - i - 1
				if remaining > 0 {
					w.failedRecords.Add(uint64(remaining))
					common.Warn("ledger: item fallback encountered connectivity/timeout error, aborting remaining items",
						zap.Int("unprocessed_count", remaining),
						zap.Error(itemErr),
					)
				}
				return
			}

			common.Error("ledger: failed to persist individual record, isolating failure", itemErr,
				zap.String("record_id", rec.ID),
				zap.String("tenant_id", rec.TenantID),
				zap.String("user_id", rec.UserID),
				zap.String("model_name", rec.ModelName),
				zap.String("provider_name", rec.ProviderName),
				zap.String("instance_id", rec.InstanceID),
				zap.Int("total_tokens", rec.TotalTokens),
			)
		} else {
			w.writtenRecords.Add(1)
		}
	}
}

// truncateUTF8 safely truncates s so its byte length is <= maxBytes without cutting across rune boundaries.
func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	if maxBytes <= 0 {
		return ""
	}
	b := []byte(s)
	for maxBytes > 0 && !utf8.RuneStart(b[maxBytes]) {
		maxBytes--
	}
	return string(b[:maxBytes])
}

// validateIdentity ensures tenant and user IDs conform to database constraints without silent truncation.
func validateIdentity(tenantID, userID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if len(tenantID) > 255 {
		return fmt.Errorf("tenant_id length %d exceeds max limit of 255 characters", len(tenantID))
	}
	if len(userID) > 255 {
		return fmt.Errorf("user_id length %d exceeds max limit of 255 characters", len(userID))
	}
	return nil
}

// UsageToLedgerRecord maps a ModelUsage structure to a TenantTokenLedger entity snapshot.
// It verifies field integrity, avoids broken UTF-8 byte cuts, and enforces size bounds on telemetry.
func UsageToLedgerRecord(usage *common.ModelUsage) (*entity.TenantTokenLedger, error) {
	if usage == nil {
		return nil, nil
	}
	if usage.ModelName == "" && usage.TotalTokens <= 0 && usage.ErrorMessage == "" {
		return nil, nil
	}

	if err := validateIdentity(usage.TenantID, usage.UserID); err != nil {
		return nil, err
	}

	createdAt := usage.StartAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	record := &entity.TenantTokenLedger{
		ID:                  common.GenerateUUID(),
		TenantID:            usage.TenantID,
		UserID:              usage.UserID,
		ProviderName:        truncateUTF8(usage.ProviderName, 128),
		InstanceID:          truncateUTF8(usage.InstanceID, 64),
		ModelName:           truncateUTF8(usage.ModelName, 255),
		ModelType:           truncateUTF8(usage.Type, 128),
		PromptTokens:        usage.InputTokens,
		CompletionTokens:    usage.OutputTokens,
		TotalTokens:         usage.TotalTokens,
		CacheReadTokens:     usage.CacheReadTokens,
		CacheCreationTokens: usage.CacheCreationTokens,
		ReasoningTokens:     usage.ReasoningTokens,
		RequestID:           truncateUTF8(usage.RequestID, 255),
		ResponseTimeMS:      usage.ResponseTimeMS,
		CreatedAt:           createdAt,
	}

	if usage.RawDetails != "" {
		if len(usage.RawDetails) > MaxRawDetailsBytes {
			record.RawDetails = entity.JSONMap{
				"warning": "raw_details_exceeded_size_limit",
				"snippet": truncateUTF8(usage.RawDetails, 1024),
			}
		} else {
			var details entity.JSONMap
			if err := json.Unmarshal([]byte(usage.RawDetails), &details); err == nil {
				record.RawDetails = details
			} else {
				record.RawDetails = entity.JSONMap{"raw": usage.RawDetails}
			}
		}
	}

	if usage.ErrorMessage != "" {
		if record.RawDetails == nil {
			record.RawDetails = make(entity.JSONMap)
		}
		record.RawDetails["error"] = truncateUTF8(usage.ErrorMessage, MaxErrorMessageBytes)
	} else if usage.TotalTokens <= 0 {
		if record.RawDetails == nil {
			record.RawDetails = make(entity.JSONMap)
		}
		if _, exists := record.RawDetails["anomaly"]; !exists && record.RawDetails["error"] == nil {
			record.RawDetails["anomaly"] = "zero_tokens"
		}
	}

	return record, nil
}

// Init sets the active database connection and initializes the global default writer.
func Init(db *gorm.DB, opts ...WriterOption) {
	writerMu.Lock()
	defer writerMu.Unlock()
	defaultDB = db
	if defaultWriter != nil && defaultWriter.db == db && len(opts) == 0 && !defaultWriter.closed.Load() {
		return
	}
	if defaultWriter != nil {
		defaultWriter.Close()
	}
	defaultWriter = NewAsyncLedgerWriter(db, opts...)
}

// SetDB updates the global database handle.
func SetDB(db *gorm.DB) {
	writerMu.Lock()
	defer writerMu.Unlock()
	defaultDB = db
	if defaultWriter != nil {
		defaultWriter.db = db
	} else if db != nil {
		defaultWriter = NewAsyncLedgerWriter(db)
	}
}

// GetDB returns the configured database handle.
func GetDB() *gorm.DB {
	writerMu.RLock()
	defer writerMu.RUnlock()
	return defaultDB
}

// SetDefaultWriter sets the global default writer instance (primarily for testing).
func SetDefaultWriter(w *AsyncLedgerWriter) {
	writerMu.Lock()
	defer writerMu.Unlock()
	if defaultWriter != nil && defaultWriter != w {
		defaultWriter.Close()
	}
	defaultWriter = w
}

// GetDefaultWriter returns the global default writer instance.
func GetDefaultWriter() *AsyncLedgerWriter {
	writerMu.RLock()
	defer writerMu.RUnlock()
	return defaultWriter
}

// Push enqueues a record into the global default writer.
// If no writer is active, it drops the record and returns false (no synchronous direct DB write).
func Push(record *entity.TenantTokenLedger) bool {
	if record == nil {
		return false
	}
	w := GetDefaultWriter()
	if w == nil {
		return false
	}
	return w.Push(record)
}

// PushUsage maps a ModelUsage to a TenantTokenLedger snapshot and enqueues it.
func PushUsage(usage *common.ModelUsage) {
	if usage == nil {
		return
	}
	record, err := UsageToLedgerRecord(usage)
	if err != nil {
		common.Warn("ledger: record rejected due to constraint violation", zap.Error(err))
		if w := GetDefaultWriter(); w != nil {
			w.failedRecords.Add(1)
		}
		return
	}
	if record != nil {
		Push(record)
	}
}

// Flush forces an immediate synchronous flush of any buffered records to the database.
func Flush() {
	w := GetDefaultWriter()
	if w != nil {
		w.Flush()
	}
}

// Close gracefully closes the active global writer.
func Close() {
	writerMu.Lock()
	defer writerMu.Unlock()
	if defaultWriter != nil {
		defaultWriter.Close()
		defaultWriter = nil
	}
}

// Stats returns the global default writer statistics.
func Stats() StatsSnapshot {
	w := GetDefaultWriter()
	if w == nil {
		return StatsSnapshot{}
	}
	return w.Stats()
}

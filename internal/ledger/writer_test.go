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
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"ragflow/internal/common"
	"ragflow/internal/entity"
)

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", url.QueryEscape(t.Name()))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite in-memory db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("failed to get underlying sql.DB: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&entity.TenantTokenLedger{}); err != nil {
		t.Fatalf("failed to automigrate tenant_token_ledger: %v", err)
	}
	return db
}

func TestAsyncLedgerWriter_CountAndTimerTrigger(t *testing.T) {
	db := setupTestDB(t)

	// Batch size 5, flush period 100ms
	writer := NewAsyncLedgerWriter(db, WithBatchSize(5), WithFlushPeriod(100*time.Millisecond))
	SetDefaultWriter(writer)
	defer Close()

	// 1. Count trigger: Push exactly 5 records (should trigger batch write immediately)
	for i := 0; i < 5; i++ {
		PushUsage(&common.ModelUsage{
			TenantID:     "t-count",
			UserID:       "u-count",
			ProviderName: "openai",
			ModelName:    "gpt-4o",
			Type:         "chat",
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		})
	}

	// Wait briefly for count-triggered batch to write
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var count int64
		db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-count").Count(&count)
		if count == 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var count1 int64
	db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-count").Count(&count1)
	if count1 != 5 {
		t.Fatalf("expected 5 records written via count trigger, got %d", count1)
	}

	// 2. Timer trigger: Push 2 records (less than batchSize 5, should trigger after 100ms)
	for i := 0; i < 2; i++ {
		PushUsage(&common.ModelUsage{
			TenantID:    "t-timer",
			ModelName:   "claude-3-5-sonnet",
			TotalTokens: 50,
		})
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var count int64
		db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-timer").Count(&count)
		if count == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	var count2 int64
	db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-timer").Count(&count2)
	if count2 != 2 {
		t.Fatalf("expected 2 records written via timer trigger, got %d", count2)
	}
}

func TestAsyncLedgerWriter_ConcurrentBatches(t *testing.T) {
	db := setupTestDB(t)

	// Controlled synchronization point to coordinate concurrency without scheduler races
	enteredCh := make(chan struct{}, 2)
	releaseCh := make(chan struct{})

	writer := NewAsyncLedgerWriter(db,
		WithBatchWorkers(2),
		WithBatchSize(2),
		WithFlushPeriod(10*time.Second),
		withBeforeWrite(func(batch []*entity.TenantTokenLedger) {
			enteredCh <- struct{}{}
			<-releaseCh
		}),
	)
	SetDefaultWriter(writer)
	defer Close()

	// Push 4 records -> cuts 2 batches of size 2
	for i := 0; i < 4; i++ {
		PushUsage(&common.ModelUsage{
			TenantID:    "t-concurrent",
			ModelName:   fmt.Sprintf("model-%d", i),
			TotalTokens: 10 + i,
		})
	}

	// Verify BOTH workers enter beforeWrite concurrently
	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not enter batch write")
	}

	select {
	case <-enteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("second worker did not enter batch write concurrently")
	}

	// Release both workers so they can complete persistence
	close(releaseCh)

	// Flush and verify all 4 records are persisted
	Flush()

	var count int64
	db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-concurrent").Count(&count)
	if count != 4 {
		t.Fatalf("expected 4 records written, got %d", count)
	}
}

func TestAsyncLedgerWriter_BoundedBufferAndDrop(t *testing.T) {
	db := setupTestDB(t)

	// Tiny buffer capacity of 2, large batch size of 100, long flush period
	writer := NewAsyncLedgerWriter(db, WithBufferSize(2), WithBatchSize(100), WithFlushPeriod(10*time.Second))
	defer writer.Close()

	r1 := &entity.TenantTokenLedger{ID: "1", TenantID: "t-drop", ModelName: "m", TotalTokens: 1}
	r2 := &entity.TenantTokenLedger{ID: "2", TenantID: "t-drop", ModelName: "m", TotalTokens: 2}
	r3 := &entity.TenantTokenLedger{ID: "3", TenantID: "t-drop", ModelName: "m", TotalTokens: 3}
	r4 := &entity.TenantTokenLedger{ID: "4", TenantID: "t-drop", ModelName: "m", TotalTokens: 4}

	ok1 := writer.Push(r1)
	ok2 := writer.Push(r2)
	ok3 := writer.Push(r3)
	ok4 := writer.Push(r4)

	if !ok1 || !ok2 {
		t.Fatalf("expected first two records to be enqueued successfully, got ok1=%v, ok2=%v", ok1, ok2)
	}
	if ok3 || ok4 {
		t.Fatalf("expected third and fourth records to be dropped due to bounded buffer, got ok3=%v, ok4=%v", ok3, ok4)
	}

	stats := writer.Stats()
	if stats.Enqueued != 2 {
		t.Errorf("expected enqueued=2, got %d", stats.Enqueued)
	}
	if stats.Dropped != 2 {
		t.Errorf("expected dropped=2, got %d", stats.Dropped)
	}
}

func TestAsyncLedgerWriter_ItemFallbackIsolatesBadRecords(t *testing.T) {
	db := setupTestDB(t)

	// SQLite trigger rejecting poison-pill model
	err := db.Exec(`CREATE TRIGGER reject_poison BEFORE INSERT ON tenant_token_ledger
		WHEN NEW.model_name = 'poison-pill'
		BEGIN
			SELECT RAISE(ABORT, 'poison pill rejected');
		END;`).Error
	if err != nil {
		t.Fatalf("failed to create sqlite trigger: %v", err)
	}

	writer := NewAsyncLedgerWriter(db, WithFlushPeriod(50*time.Millisecond), WithBatchSize(10))
	SetDefaultWriter(writer)
	defer Close()

	good1 := &common.ModelUsage{
		TenantID:    "t-good-1",
		ModelName:   "good-model-1",
		TotalTokens: 100,
	}
	poison := &common.ModelUsage{
		TenantID:    "t-bad",
		ModelName:   "poison-pill",
		TotalTokens: 666,
	}
	good2 := &common.ModelUsage{
		TenantID:    "t-good-2",
		ModelName:   "good-model-2",
		TotalTokens: 200,
	}

	PushUsage(good1)
	PushUsage(poison)
	PushUsage(good2)

	Flush()

	var rows []entity.TenantTokenLedger
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows = nil
		_ = db.Where("tenant_id IN (?, ?)", "t-good-1", "t-good-2").Order("tenant_id asc").Find(&rows).Error
		if len(rows) == 2 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 good rows to be persisted despite the bad record, got %d", len(rows))
	}

	// Verify poison record was rejected and never inserted
	var poisonCount int64
	db.Model(&entity.TenantTokenLedger{}).Where("model_name = ?", "poison-pill").Count(&poisonCount)
	if poisonCount != 0 {
		t.Errorf("poison pill should not have been saved in db, got %d", poisonCount)
	}

	stats := writer.Stats()
	if stats.Written != 2 || stats.Failed != 1 {
		t.Errorf("stats mismatch: written=%d (want 2), failed=%d (want 1)", stats.Written, stats.Failed)
	}
}

func TestAsyncLedgerWriter_BatchTimeoutCap(t *testing.T) {
	db := setupTestDB(t)

	// SQLite trigger rejecting all records for tenant 't-slow'
	err := db.Exec(`CREATE TRIGGER reject_all BEFORE INSERT ON tenant_token_ledger
		WHEN NEW.tenant_id = 't-slow'
		BEGIN
			SELECT RAISE(ABORT, 'all rejected');
		END;`).Error
	if err != nil {
		t.Fatalf("failed to create sqlite trigger: %v", err)
	}

	// Set a tight batch timeout of 50ms
	writer := NewAsyncLedgerWriter(db,
		WithBatchSize(10),
		WithFlushPeriod(10*time.Second),
		WithBatchTimeout(50*time.Millisecond),
	)
	SetDefaultWriter(writer)
	defer Close()

	for i := 0; i < 10; i++ {
		PushUsage(&common.ModelUsage{
			TenantID:    "t-slow",
			ModelName:   fmt.Sprintf("m-%d", i),
			TotalTokens: 10,
		})
	}

	start := time.Now()
	Flush()
	elapsed := time.Since(start)

	// Total fallback duration must be capped by batchTimeout, never 10 * 5s = 50s!
	if elapsed > 1*time.Second {
		t.Fatalf("batch write took %v, expected to be bounded by batchTimeout", elapsed)
	}

	stats := writer.Stats()
	if stats.Failed != 10 {
		t.Errorf("expected 10 failed records, got %d", stats.Failed)
	}
}

func TestAsyncLedgerWriter_CloseGracefulDrainAndTimeout(t *testing.T) {
	db := setupTestDB(t)

	// 1. Graceful drain on normal close
	writer := NewAsyncLedgerWriter(db, WithFlushPeriod(10*time.Second), WithBatchSize(100))
	SetDefaultWriter(writer)

	for i := 0; i < 5; i++ {
		PushUsage(&common.ModelUsage{
			TenantID:    "t-drain",
			ModelName:   "gpt-4o",
			TotalTokens: 10 + i,
		})
	}

	Close()

	var count int64
	db.Model(&entity.TenantTokenLedger{}).Where("tenant_id = ?", "t-drain").Count(&count)
	if count != 5 {
		t.Fatalf("expected 5 rows drained into ledger table, got %d", count)
	}

	// 2. Push after close must be rejected without hanging or panicking
	pushed := writer.Push(&entity.TenantTokenLedger{
		ID:          common.GenerateUUID(),
		TenantID:    "t-after",
		ModelName:   "gpt-4o",
		TotalTokens: 10,
	})
	if pushed {
		t.Fatalf("expected push after close to return false")
	}

	stats := writer.Stats()
	if stats.Dropped != 1 {
		t.Errorf("expected dropped=1 for push after close, got %d", stats.Dropped)
	}
}

func TestAsyncLedgerWriter_UTF8AndFieldValidation(t *testing.T) {
	// 1. Safe multi-byte UTF-8 truncation:
	// "中文测试模型" is 6 runes (18 bytes).
	// Byte truncation at 10 bytes would cut "测" (bytes 6..9) in half!
	chineseName := "中文测试模型"
	truncated := truncateUTF8(chineseName, 10)
	if !utf8.ValidString(truncated) {
		t.Fatalf("truncated string %q is not valid UTF-8", truncated)
	}
	if len(truncated) > 10 {
		t.Fatalf("truncated string len=%d exceeds max 10", len(truncated))
	}
	// 9 bytes = "中文测"
	if truncated != "中文测" {
		t.Errorf("expected %q, got %q", "中文测", truncated)
	}

	// 2. Strict identity constraint validation (no silent truncation):
	// Empty TenantID rejected
	emptyTenant := &common.ModelUsage{TenantID: "", ModelName: "gpt-4o", TotalTokens: 10}
	if _, err := UsageToLedgerRecord(emptyTenant); err == nil {
		t.Errorf("expected empty TenantID to return error, got nil")
	}

	// TenantID > 255 characters rejected
	longTenant := &common.ModelUsage{
		TenantID:    strings.Repeat("t", 256),
		ModelName:   "gpt-4o",
		TotalTokens: 10,
	}
	if _, err := UsageToLedgerRecord(longTenant); err == nil {
		t.Errorf("expected TenantID > 255 to return error, got nil")
	}

	// UserID > 255 characters rejected
	longUser := &common.ModelUsage{
		TenantID:    "valid-tenant",
		UserID:      strings.Repeat("u", 256),
		ModelName:   "gpt-4o",
		TotalTokens: 10,
	}
	if _, err := UsageToLedgerRecord(longUser); err == nil {
		t.Errorf("expected UserID > 255 to return error, got nil")
	}

	// Long enterprise tenant key (e.g. 100 chars) and long user ID accepted
	longEnterpriseTenant := &common.ModelUsage{
		TenantID:    "urn:ragflow:enterprise:division:subsidiary:department:production:tenant-alpha-001234567890",
		UserID:      "external_user_canvas_session_long_identifier_12345@enterprise-organization.com",
		ModelName:   "gpt-4o",
		TotalTokens: 10,
	}
	if rec, err := UsageToLedgerRecord(longEnterpriseTenant); err != nil || rec == nil {
		t.Errorf("expected valid enterprise tenant and long user ID to succeed, got rec=%v, err=%v", rec, err)
	}

	// 3. Size control on RawDetails and ErrorMessage
	hugeRaw := strings.Repeat("x", 40*1024) // 40 KB
	hugeErr := strings.Repeat("e", 1000)

	usage := &common.ModelUsage{
		TenantID:     "valid-tenant",
		UserID:       "valid-user",
		ModelName:    chineseName,
		TotalTokens:  10,
		RawDetails:   hugeRaw,
		ErrorMessage: hugeErr,
	}

	rec, err := UsageToLedgerRecord(usage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// RawDetails bounded
	if rec.RawDetails["warning"] != "raw_details_exceeded_size_limit" {
		t.Errorf("expected raw_details_exceeded_size_limit warning")
	}
	// ErrorMessage truncated to max 512 bytes
	errStr, ok := rec.RawDetails["error"].(string)
	if !ok || len(errStr) > MaxErrorMessageBytes {
		t.Errorf("expected error string <= 512 bytes, got len=%d", len(errStr))
	}

	// 4. ModelType (128 bytes) headroom
	namespacedUsage := &common.ModelUsage{
		TenantID:    "valid-tenant",
		ModelName:   "gpt-4o",
		Type:        "chat,image2text,audio_transcription",
		TotalTokens: 10,
	}
	nsRec, err := UsageToLedgerRecord(namespacedUsage)
	if err != nil {
		t.Fatalf("unexpected error on namespaced usage: %v", err)
	}
	if nsRec.ModelType != "chat,image2text,audio_transcription" {
		t.Errorf("expected full ModelType preserved, got %q", nsRec.ModelType)
	}
}

func TestAsyncLedgerWriter_ZeroTokenAndErrorCapture(t *testing.T) {
	db := setupTestDB(t)

	Init(db, WithFlushPeriod(50*time.Millisecond))
	defer Close()

	zeroUsageWithError := &common.ModelUsage{
		TenantID:       "tenant-err",
		ModelName:      "gpt-4o",
		Type:           "chat",
		TotalTokens:    0,
		ErrorMessage:   "upstream 429 rate limit exceeded",
		ResponseTimeMS: 150,
	}

	zeroUsageAnomaly := &common.ModelUsage{
		TenantID:       "tenant-anomaly",
		ModelName:      "claude-3-5-sonnet",
		Type:           "chat",
		TotalTokens:    0,
		ResponseTimeMS: 500,
	}

	emptyUsage := &common.ModelUsage{
		TenantID:    "tenant-empty",
		TotalTokens: 0,
	}

	PushUsage(zeroUsageWithError)
	PushUsage(zeroUsageAnomaly)
	PushUsage(emptyUsage)
	PushUsage(nil)
	Push(nil)

	Flush()

	var rows []entity.TenantTokenLedger
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows = nil
		if err := db.Order("tenant_id asc").Find(&rows).Error; err == nil && len(rows) == 2 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows in ledger table for anomaly/error tokens, got %d", len(rows))
	}

	// First row: tenant-anomaly
	r1 := rows[0]
	if r1.TenantID != "tenant-anomaly" || r1.TotalTokens != 0 {
		t.Errorf("r1 mismatch: tenant=%s, total=%d", r1.TenantID, r1.TotalTokens)
	}
	if r1.RawDetails["anomaly"] != "zero_tokens" {
		t.Errorf("r1 anomaly mismatch: got %v, want zero_tokens", r1.RawDetails["anomaly"])
	}

	// Second row: tenant-err
	r2 := rows[1]
	if r2.TenantID != "tenant-err" || r2.TotalTokens != 0 {
		t.Errorf("r2 mismatch: tenant=%s, total=%d", r2.TenantID, r2.TotalTokens)
	}
	if r2.RawDetails["error"] != "upstream 429 rate limit exceeded" {
		t.Errorf("r2 error mismatch: got %v, want 'upstream 429 rate limit exceeded'", r2.RawDetails["error"])
	}
}

func TestTenantAttribution_FiveScenarios(t *testing.T) {
	// Test that LLM, VLM, Embedding, Rerank, and Decision calls produce valid tenant ledger records
	// with complete provider snapshot and instance configuration attribution
	scenarios := []struct {
		name         string
		usage        *common.ModelUsage
		wantModel    string
		wantType     string
		wantProvider string
		wantInstance string
	}{
		{
			name: "LLM",
			usage: &common.ModelUsage{
				TenantID:     "t-llm",
				UserID:       "u-llm",
				ProviderName: "openai",
				InstanceID:   "inst-openai-prod",
				ModelName:    "gpt-4o",
				Type:         "chat",
				TotalTokens:  150,
			},
			wantModel:    "gpt-4o",
			wantType:     "chat",
			wantProvider: "openai",
			wantInstance: "inst-openai-prod",
		},
		{
			name: "VLM",
			usage: &common.ModelUsage{
				TenantID:     "t-vlm",
				UserID:       "u-vlm",
				ProviderName: "openai",
				InstanceID:   "inst-openai-vlm",
				ModelName:    "gpt-4o-mini",
				Type:         "image2text",
				TotalTokens:  200,
			},
			wantModel:    "gpt-4o-mini",
			wantType:     "image2text",
			wantProvider: "openai",
			wantInstance: "inst-openai-vlm",
		},
		{
			name: "Embedding",
			usage: &common.ModelUsage{
				TenantID:     "t-embed",
				UserID:       "u-embed",
				ProviderName: "baai",
				InstanceID:   "inst-baai-embed",
				ModelName:    "bge-m3",
				Type:         "embedding",
				TotalTokens:  512,
			},
			wantModel:    "bge-m3",
			wantType:     "embedding",
			wantProvider: "baai",
			wantInstance: "inst-baai-embed",
		},
		{
			name: "Rerank",
			usage: &common.ModelUsage{
				TenantID:     "t-rerank",
				UserID:       "u-rerank",
				ProviderName: "bce",
				InstanceID:   "inst-bce-rerank",
				ModelName:    "bce-reranker-base_v1",
				Type:         "rerank",
				TotalTokens:  80,
			},
			wantModel:    "bce-reranker-base_v1",
			wantType:     "rerank",
			wantProvider: "bce",
			wantInstance: "inst-bce-rerank",
		},
		{
			name: "Decision",
			usage: &common.ModelUsage{
				TenantID:     "t-decide",
				UserID:       "u-decide",
				ProviderName: "systemone",
				InstanceID:   "", // Builtin system decision model has no tenant instance ID
				ModelName:    "decision-v1",
				Type:         "decision",
				TotalTokens:  95,
			},
			wantModel:    "decision-v1",
			wantType:     "decision",
			wantProvider: "systemone",
			wantInstance: "",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			rec, err := UsageToLedgerRecord(sc.usage)
			if err != nil {
				t.Fatalf("UsageToLedgerRecord failed: %v", err)
			}
			if rec.TenantID != sc.usage.TenantID {
				t.Errorf("tenant mismatch: got %s, want %s", rec.TenantID, sc.usage.TenantID)
			}
			if rec.ModelName != sc.wantModel {
				t.Errorf("model mismatch: got %s, want %s", rec.ModelName, sc.wantModel)
			}
			if rec.ModelType != sc.wantType {
				t.Errorf("type mismatch: got %s, want %s", rec.ModelType, sc.wantType)
			}
			if rec.ProviderName != sc.wantProvider {
				t.Errorf("provider mismatch: got %s, want %s", rec.ProviderName, sc.wantProvider)
			}
			if rec.InstanceID != sc.wantInstance {
				t.Errorf("instance mismatch: got %s, want %s", rec.InstanceID, sc.wantInstance)
			}
			if rec.TotalTokens != sc.usage.TotalTokens {
				t.Errorf("tokens mismatch: got %d, want %d", rec.TotalTokens, sc.usage.TotalTokens)
			}
		})
	}
}

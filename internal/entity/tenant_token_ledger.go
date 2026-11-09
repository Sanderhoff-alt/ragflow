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

package entity

import (
	"time"
)

// TenantTokenLedger records individual model usage accounting transactions (pure financial telemetry, zero noise).
type TenantTokenLedger struct {
	// 1. Primary key & tenant identity
	ID       string `gorm:"column:id;primaryKey;size:32" json:"id"`
	TenantID string `gorm:"column:tenant_id;size:255;not null;index:idx_tenant_created,priority:1" json:"tenant_id"`
	UserID   string `gorm:"column:user_id;size:255;index:idx_user_id" json:"user_id"`

	// 2. Model & provider configuration attributes
	ProviderName string `gorm:"column:provider_name;size:128;not null" json:"provider_name"`
	InstanceID   string `gorm:"column:instance_id;size:64;index:idx_instance_id" json:"instance_id,omitempty"`
	ModelName    string `gorm:"column:model_name;size:255;not null;index:idx_model_created,priority:1" json:"model_name"`
	ModelType    string `gorm:"column:model_type;size:128;not null" json:"model_type"`

	// 3. Token metrics (normalized)
	PromptTokens        int `gorm:"column:prompt_tokens;not null;default:0" json:"prompt_tokens"`
	CompletionTokens    int `gorm:"column:completion_tokens;not null;default:0" json:"completion_tokens"`
	TotalTokens         int `gorm:"column:total_tokens;not null;default:0" json:"total_tokens"`
	CacheReadTokens     int `gorm:"column:cache_read_tokens;not null;default:0" json:"cache_read_tokens"`
	CacheCreationTokens int `gorm:"column:cache_creation_tokens;not null;default:0" json:"cache_creation_tokens"`
	ReasoningTokens     int `gorm:"column:reasoning_tokens;not null;default:0" json:"reasoning_tokens"`

	// 4. Request context & latency
	RequestID      string `gorm:"column:request_id;size:255" json:"request_id"`
	ResponseTimeMS int64  `gorm:"column:response_time_ms;not null;default:0" json:"response_time_ms"`

	// 5. Raw vendor details snapshot & timestamp
	RawDetails JSONMap   `gorm:"column:raw_details;type:longtext" json:"raw_details,omitempty"`
	CreatedAt  time.Time `gorm:"column:created_at;not null;index:idx_tenant_created,priority:2;index:idx_model_created,priority:2" json:"created_at"`
}

func (TenantTokenLedger) TableName() string {
	return "tenant_token_ledger"
}

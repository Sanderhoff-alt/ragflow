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

package models

import "encoding/json"

// extractOpenAIUsage extracts token usage from an OpenAI-compatible
// non-streaming response body. Returns (nil, false) when the response
// carries no usable usage block.
func extractOpenAIUsage(body map[string]any) (*TokenUsage, bool) {
	rawUsage, ok := body["usage"].(map[string]any)
	if !ok {
		return nil, false
	}

	return tokenUsageFromRaw(rawUsage), true
}

// extractOpenAIStreamUsage extracts token usage from one OpenAI-compatible
// streaming event. A missing or null usage field is not an error.
func extractOpenAIStreamUsage(event map[string]any) (*TokenUsage, bool) {
	rawUsage, ok := event["usage"].(map[string]any)
	if !ok || rawUsage == nil {
		return nil, false
	}

	return tokenUsageFromRaw(rawUsage), true
}

func tokenUsageFromRaw(rawUsage map[string]any) *TokenUsage {
	usage := &TokenUsage{}
	usage.PromptTokens = extractToken(rawUsage, "prompt_tokens", "input_tokens")
	usage.CompletionTokens = extractToken(rawUsage, "completion_tokens", "output_tokens")
	usage.TotalTokens = extractToken(rawUsage, "total_tokens")

	// 1. Cache Read / Hit
	usage.CacheReadTokens = extractNestedToken(rawUsage, "prompt_tokens_details", "cached_tokens", "cache_read_tokens")
	if usage.CacheReadTokens == 0 {
		usage.CacheReadTokens = extractToken(rawUsage, "prompt_cache_hit_tokens", "cache_read_input_tokens", "cacheReadInputTokens")
	}

	// 2. Cache Write / Creation
	usage.CacheCreationTokens = extractNestedToken(rawUsage, "prompt_tokens_details", "cache_write_tokens", "cache_creation_tokens")
	if usage.CacheCreationTokens == 0 {
		usage.CacheCreationTokens = extractToken(rawUsage, "cache_creation_input_tokens", "cacheWriteInputTokens")
	}

	// 3. Reasoning / Thinking
	usage.ReasoningTokens = extractNestedToken(rawUsage, "completion_tokens_details", "reasoning_tokens")
	if usage.ReasoningTokens == 0 {
		usage.ReasoningTokens = extractToken(rawUsage, "reasoning_tokens")
	}

	if usage.TotalTokens == 0 {
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	}

	if rawBytes, err := json.Marshal(rawUsage); err == nil {
		usage.RawDetails = string(rawBytes)
	}

	return usage
}

// extractNestedToken reads a numeric field from a nested map object.
func extractNestedToken(m map[string]any, parentKey string, keys ...string) int {
	if parent, ok := m[parentKey].(map[string]any); ok && parent != nil {
		return extractToken(parent, keys...)
	}
	return 0
}

// extractToken reads a numeric field from a map, trying each key in order.
// Returns 0 when no key is present or the value is not a number.
func extractToken(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case float64:
				return int(val)
			case int:
				return val
			case int64:
				return int(val)
			case json.Number:
				if n, err := val.Int64(); err == nil {
					return int(n)
				}
			}
		}
	}
	return 0
}

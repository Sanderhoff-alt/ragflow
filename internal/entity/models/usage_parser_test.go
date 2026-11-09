package models

import (
	"ragflow/internal/common"
	"testing"
)

func TestExtractOpenAIUsage(t *testing.T) {
	cases := []struct {
		name   string
		body   map[string]any
		want   *TokenUsage
		wantOK bool
	}{
		{
			name: "standard openai",
			body: map[string]any{
				"usage": map[string]any{
					"prompt_tokens":     float64(10),
					"completion_tokens": float64(20),
					"total_tokens":      float64(30),
				},
			},
			want:   &TokenUsage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
			wantOK: true,
		},
		{
			name: "total missing falls back to sum",
			body: map[string]any{
				"usage": map[string]any{
					"prompt_tokens":     float64(10),
					"completion_tokens": float64(20),
				},
			},
			want:   &TokenUsage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
			wantOK: true,
		},
		{
			name: "no usage key",
			body: map[string]any{
				"choices": []any{map[string]any{"message": map[string]any{"content": "hi"}}},
			},
			want:   nil,
			wantOK: false,
		},
		{
			name: "cohere style input_tokens/output_tokens",
			body: map[string]any{
				"usage": map[string]any{
					"input_tokens":  float64(100),
					"output_tokens": float64(50),
				},
			},
			want:   &TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractOpenAIUsage(tc.body)
			if ok != tc.wantOK {
				t.Fatalf("extractOpenAIUsage() ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got.PromptTokens != tc.want.PromptTokens ||
				got.CompletionTokens != tc.want.CompletionTokens ||
				got.TotalTokens != tc.want.TotalTokens {
				t.Fatalf("extractOpenAIUsage() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestExtractOpenAIStreamUsage(t *testing.T) {
	cases := []struct {
		name   string
		event  map[string]any
		want   *TokenUsage
		wantOK bool
	}{
		{
			name: "standard streaming event",
			event: map[string]any{
				"usage": map[string]any{
					"prompt_tokens":     float64(100),
					"completion_tokens": float64(50),
					"total_tokens":      float64(150),
				},
			},
			want:   &TokenUsage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
			wantOK: true,
		},
		{
			name: "null usage",
			event: map[string]any{
				"usage": nil,
			},
			want:   nil,
			wantOK: false,
		},
		{
			name: "no usage key",
			event: map[string]any{
				"choices": []any{map[string]any{"delta": map[string]any{"content": "hi"}}},
			},
			want:   nil,
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractOpenAIStreamUsage(tc.event)
			if ok != tc.wantOK {
				t.Fatalf("extractOpenAIStreamUsage() ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if got.PromptTokens != tc.want.PromptTokens ||
				got.CompletionTokens != tc.want.CompletionTokens ||
				got.TotalTokens != tc.want.TotalTokens {
				t.Fatalf("extractOpenAIStreamUsage() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestExtractToken(t *testing.T) {
	m := map[string]any{
		"a": float64(1),
		"b": 2,
		"c": int64(3),
	}

	if got := extractToken(m, "a"); got != 1 {
		t.Fatalf("extractToken(a) = %d, want 1", got)
	}
	if got := extractToken(m, "b"); got != 2 {
		t.Fatalf("extractToken(b) = %d, want 2", got)
	}
	if got := extractToken(m, "c"); got != 3 {
		t.Fatalf("extractToken(c) = %d, want 3", got)
	}
	if got := extractToken(m, "missing"); got != 0 {
		t.Fatalf("extractToken(missing) = %d, want 0", got)
	}
	if got := extractToken(m, "missing", "a"); got != 1 {
		t.Fatalf("extractToken(missing, a) = %d, want 1", got)
	}
}

func TestTokenUsageFromRaw_DetailedMetrics(t *testing.T) {
	t.Run("openai with details", func(t *testing.T) {
		raw := map[string]any{
			"prompt_tokens":     float64(100),
			"completion_tokens": float64(50),
			"total_tokens":      float64(150),
			"prompt_tokens_details": map[string]any{
				"cached_tokens":      float64(30),
				"cache_write_tokens": float64(15),
			},
			"completion_tokens_details": map[string]any{
				"reasoning_tokens": float64(20),
			},
		}

		usage := tokenUsageFromRaw(raw)
		if usage.PromptTokens != 100 || usage.CompletionTokens != 50 || usage.TotalTokens != 150 {
			t.Errorf("base tokens mismatch: %+v", usage)
		}
		if usage.CacheReadTokens != 30 {
			t.Errorf("CacheReadTokens = %d, want 30", usage.CacheReadTokens)
		}
		if usage.CacheCreationTokens != 15 {
			t.Errorf("CacheCreationTokens = %d, want 15", usage.CacheCreationTokens)
		}
		if usage.ReasoningTokens != 20 {
			t.Errorf("ReasoningTokens = %d, want 20", usage.ReasoningTokens)
		}
		if usage.RawDetails == "" {
			t.Errorf("RawDetails is empty")
		}
	})

	t.Run("deepseek and anthropic fallback keys", func(t *testing.T) {
		raw := map[string]any{
			"input_tokens":                float64(80),
			"output_tokens":               float64(40),
			"prompt_cache_hit_tokens":     float64(25),
			"cache_creation_input_tokens": float64(10),
			"reasoning_tokens":            float64(15),
		}

		usage := tokenUsageFromRaw(raw)
		if usage.PromptTokens != 80 || usage.CompletionTokens != 40 || usage.TotalTokens != 120 {
			t.Errorf("base tokens mismatch: %+v", usage)
		}
		if usage.CacheReadTokens != 25 {
			t.Errorf("CacheReadTokens = %d, want 25", usage.CacheReadTokens)
		}
		if usage.CacheCreationTokens != 10 {
			t.Errorf("CacheCreationTokens = %d, want 10", usage.CacheCreationTokens)
		}
		if usage.ReasoningTokens != 15 {
			t.Errorf("ReasoningTokens = %d, want 15", usage.ReasoningTokens)
		}
	})
}

func TestCollectModelUsageUpdatesRawDetailsWithStandardTokens(t *testing.T) {
	t.Run("overwrites stale raw details when new raw details present", func(t *testing.T) {
		modelUsage := &common.ModelUsage{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
			RawDetails:   `{"stale": true, "turn": 1}`,
		}
		newUsage := &TokenUsage{
			PromptTokens:        50,
			CompletionTokens:    60,
			TotalTokens:         110,
			CacheReadTokens:     15,
			CacheCreationTokens: 5,
			ReasoningTokens:     25,
			RawDetails:          `{"fresh": true, "turn": 2}`,
		}

		if err := collectModelUsage(modelUsage, newUsage); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if modelUsage.InputTokens != 50 || modelUsage.OutputTokens != 60 || modelUsage.TotalTokens != 110 {
			t.Errorf("token fields mismatch: %+v", modelUsage)
		}
		if modelUsage.CacheReadTokens != 15 || modelUsage.CacheCreationTokens != 5 || modelUsage.ReasoningTokens != 25 {
			t.Errorf("detailed tokens mismatch: %+v", modelUsage)
		}
		if modelUsage.RawDetails != `{"fresh": true, "turn": 2}` {
			t.Errorf("RawDetails was not updated, got %q, want fresh raw details", modelUsage.RawDetails)
		}
	})

	t.Run("clears stale raw details when new token usage has empty raw details", func(t *testing.T) {
		modelUsage := &common.ModelUsage{
			InputTokens: 10,
			RawDetails:  `{"stale": true}`,
		}
		newUsage := &TokenUsage{
			PromptTokens:     35,
			CompletionTokens: 45,
			TotalTokens:      80,
			RawDetails:       "",
		}

		if err := collectModelUsage(modelUsage, newUsage); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if modelUsage.InputTokens != 35 || modelUsage.OutputTokens != 45 {
			t.Errorf("token fields mismatch: %+v", modelUsage)
		}
		if modelUsage.RawDetails != "" {
			t.Errorf("expected RawDetails to be updated to empty, got %q", modelUsage.RawDetails)
		}
	})
}

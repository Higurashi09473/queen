package base

import (
	"testing"
	"time"
)

func TestPlaceholderDollar(t *testing.T) {
	tests := []struct {
		n        int
		expected string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
		{100, "$100"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := PlaceholderDollar(tt.n)
			if result != tt.expected {
				t.Errorf("PlaceholderDollar(%d) = %q; want %q", tt.n, result, tt.expected)
			}
		})
	}
}

func TestPlaceholderQuestion(t *testing.T) {
	for i := 1; i <= 10; i++ {
		t.Run("placeholder", func(t *testing.T) {
			result := PlaceholderQuestion(i)
			if result != "?" {
				t.Errorf("PlaceholderQuestion(%d) = %q; want ?", i, result)
			}
		})
	}
}

func TestQuoteDoubleQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "users", `"users"`},
		{"with quotes", `my"table`, `"my""table"`},
		{"multiple quotes", `my"ta"ble`, `"my""ta""ble"`},
		{"empty", "", `""`},
		{"special chars", "table_123", `"table_123"`},
		{"spaces", "my table", `"my table"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteDoubleQuotes(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteDoubleQuotes(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQuoteBackticks(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "users", "`users`"},
		{"with backtick", "my`table", "`my``table`"},
		{"multiple backticks", "my`ta`ble", "`my``ta``ble`"},
		{"empty", "", "``"},
		{"special chars", "table_123", "`table_123`"},
		{"spaces", "my table", "`my table`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteBackticks(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteBackticks(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseTimeISO8601(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "valid timestamp",
			input:    "2024-01-28 12:34:56",
			expected: time.Date(2024, 1, 28, 12, 34, 56, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "another valid timestamp",
			input:    "2023-12-31 23:59:59",
			expected: time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:    "invalid format",
			input:   "28-01-2024 12:34:56",
			wantErr: true,
		},
		{
			name:    "not a string",
			input:   12345,
			wantErr: true,
		},
		{
			name:    "nil",
			input:   nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseTimeISO8601(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !result.Equal(tt.expected) {
				t.Errorf("ParseTimeISO8601(%v) = %v; want %v", tt.input, result, tt.expected)
			}
		})
	}
}

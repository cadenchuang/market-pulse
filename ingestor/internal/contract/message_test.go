package contract

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validItem() NewsRaw {
	return NewsRaw{
		SchemaVersion: SchemaVersion,
		ID:            "replay-000001",
		Source:        SourceReplay,
		Title:         "Synthetic headline",
		Body:          "Synthetic body text.",
		PublishedAt:   time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		IngestedAt:    time.Date(2026, 7, 20, 12, 0, 1, 0, time.UTC),
	}
}

func TestValidate_OK(t *testing.T) {
	if err := validItem().Validate(); err != nil {
		t.Fatalf("expected valid item, got %v", err)
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := map[string]func(*NewsRaw){
		"bad schema_version": func(n *NewsRaw) { n.SchemaVersion = "9.9.9" },
		"missing id":         func(n *NewsRaw) { n.ID = "" },
		"bad source":         func(n *NewsRaw) { n.Source = "twitter" },
		"missing title":      func(n *NewsRaw) { n.Title = "" },
		"missing body":       func(n *NewsRaw) { n.Body = "" },
		"missing published":  func(n *NewsRaw) { n.PublishedAt = time.Time{} },
		"missing ingested":   func(n *NewsRaw) { n.IngestedAt = time.Time{} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			item := validItem()
			mutate(&item)
			if err := item.Validate(); err == nil {
				t.Fatalf("expected validation error for %q", name)
			}
		})
	}
}

func TestJSON_RoundTrip_And_WireTags(t *testing.T) {
	item := validItem()
	item.Feed = "synthetic-markets"
	item.Language = "en"
	item.Tickers = []string{"NMBS"}

	b, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := string(b)

	// Wire field names must match schemas/news.raw.schema.json.
	for _, key := range []string{
		`"schema_version":"1.0.0"`, `"id":`, `"source":"replay"`,
		`"published_at":`, `"ingested_at":`,
	} {
		if !strings.Contains(raw, key) {
			t.Errorf("marshaled JSON missing %s\n got: %s", key, raw)
		}
	}

	var back NewsRaw
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !back.PublishedAt.Equal(item.PublishedAt) || back.ID != item.ID {
		t.Errorf("round trip mismatch: %+v vs %+v", back, item)
	}
}

func TestKey_IsID(t *testing.T) {
	item := validItem()
	if string(item.Key()) != item.ID {
		t.Errorf("Key() = %q, want %q", item.Key(), item.ID)
	}
}

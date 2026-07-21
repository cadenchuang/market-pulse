package process

import (
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

func TestNormalize(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"collapses whitespace": {"  Hello   \t World \n", "hello world"},
		"lowercases":           {"Apple INC", "apple inc"},
		"strips controls":      {"a\x00b", "ab"},
		"empty":                {"   ", ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := Normalize(c.in); got != c.want {
				t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestContentHash_FormatAndStability(t *testing.T) {
	h1 := ContentHash("a title", "a body")
	h2 := ContentHash("a title", "a body")
	if h1 != h2 {
		t.Errorf("hash not stable: %q vs %q", h1, h2)
	}
	if !contractHashOK(h1) {
		t.Errorf("hash %q does not match sha256:<64 hex>", h1)
	}
	if ContentHash("a title", "a body") == ContentHash("a title", "different") {
		t.Error("different content produced identical hash")
	}
}

func contractHashOK(h string) bool {
	p := contract.NewsProcessed{
		SchemaVersion: contract.SchemaVersion, ID: "x", ContentHash: h,
		Source: contract.SourceReplay, Title: "t", Body: "b",
		PublishedAt: time.Now(), IngestedAt: time.Now(), ProcessedAt: time.Now(),
	}
	return p.Validate() == nil
}

func TestDeduper_EvictsOldest(t *testing.T) {
	d := NewDeduper(2)
	if d.Seen("a") {
		t.Fatal("first insert should be new")
	}
	if !d.Seen("a") {
		t.Fatal("second insert of a should be duplicate")
	}
	d.Seen("b") // now {a,b}
	d.Seen("c") // evicts a -> {b,c}
	if d.Seen("b") == false {
		t.Error("b should still be present")
	}
	if d.Seen("a") {
		t.Error("a should have been evicted and thus treated as new")
	}
}

func TestFilter(t *testing.T) {
	cfg := DefaultFilterConfig() // MinBodyLen 20, Language en
	long := "this is a sufficiently long synthetic body text"
	cases := []struct {
		name              string
		title, body, lang string
		keep              bool
		reason            string
	}{
		{"keep", "t", long, "en", true, ""},
		{"empty", "", "", "en", false, "empty"},
		{"too short", "t", "short", "en", false, "too_short"},
		{"wrong lang", "t", long, "fr", false, "language"},
		{"no lang kept", "t", long, "", true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keep, reason := Filter(c.title, c.body, c.lang, cfg)
			if keep != c.keep || reason != c.reason {
				t.Errorf("Filter = (%v,%q), want (%v,%q)", keep, reason, c.keep, c.reason)
			}
		})
	}
}

func rawItem(id, title, body string) contract.NewsRaw {
	return contract.NewsRaw{
		SchemaVersion: contract.SchemaVersion, ID: id, Source: contract.SourceReplay,
		Title: title, Body: body, Language: "en",
		PublishedAt: time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC),
		IngestedAt:  time.Date(2026, 7, 20, 12, 0, 1, 0, time.UTC),
	}
}

func TestProcessor_Decisions(t *testing.T) {
	p := NewProcessor(1000, DefaultFilterConfig())
	now := time.Date(2026, 7, 20, 12, 0, 2, 0, time.UTC)
	longBody := "this is a sufficiently long synthetic body for testing"

	out, dec, _ := p.Process(rawItem("1", "Title One", longBody), now)
	if dec != DecisionEmit {
		t.Fatalf("expected Emit, got %v", dec)
	}
	if err := out.Validate(); err != nil {
		t.Fatalf("emitted item fails contract validation: %v", err)
	}
	if !out.ProcessedAt.Equal(now) || out.Title != "title one" {
		t.Errorf("unexpected output: %+v", out)
	}

	// Same content, different id -> duplicate.
	_, dec, _ = p.Process(rawItem("2", "Title One", longBody), now)
	if dec != DecisionDuplicate {
		t.Errorf("expected Duplicate, got %v", dec)
	}

	// Too-short body -> filtered.
	_, dec, reason := p.Process(rawItem("3", "t", "short"), now)
	if dec != DecisionFiltered || reason != "too_short" {
		t.Errorf("expected Filtered/too_short, got %v/%q", dec, reason)
	}
}

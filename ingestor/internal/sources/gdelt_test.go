package sources

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

const gdeltBody = `{"articles":[
  {"url":"https://ex.com/a","title":"Alpha Corp beats earnings","seendate":"20260720T120000Z","domain":"ex.com","language":"English"},
  {"url":"https://ex.com/b","title":"Beta Inc slumps on weak demand","seendate":"20260720T120500Z","domain":"ex.com","language":"English"},
  {"url":"","title":"missing url dropped","seendate":"20260720T120500Z"}
]}`

// drainN collects up to n items from a live source, cancelling once it has them.
func drainN(t *testing.T, s Source, n int) []contract.NewsRaw {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan contract.NewsRaw, 64)
	go func() { _ = s.Run(ctx, out) }()

	var got []contract.NewsRaw
	timeout := time.After(2 * time.Second)
	for len(got) < n {
		select {
		case item := <-out:
			got = append(got, item)
		case <-timeout:
			t.Fatalf("timed out collecting items: got %d, want %d", len(got), n)
		}
	}
	return got
}

func TestGDELT_ParsesArticles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "stocks" {
			t.Errorf("unexpected query: %q", r.URL.Query().Get("query"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(gdeltBody))
	}))
	defer srv.Close()

	g := &GDELT{Query: "stocks", BaseURL: srv.URL, PollInterval: time.Hour}
	got := drainN(t, g, 2)

	if got[0].Source != contract.SourceGDELT {
		t.Errorf("source = %q, want gdelt", got[0].Source)
	}
	if got[0].Title != "Alpha Corp beats earnings" {
		t.Errorf("title = %q", got[0].Title)
	}
	if got[0].Body != got[0].Title {
		t.Errorf("gdelt body should fall back to title, got %q", got[0].Body)
	}
	if got[0].Language != "en" {
		t.Errorf("language = %q, want en", got[0].Language)
	}
	if got[0].URL != "https://ex.com/a" {
		t.Errorf("url = %q", got[0].URL)
	}
	want := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	if !got[0].PublishedAt.Equal(want) {
		t.Errorf("published_at = %v, want %v", got[0].PublishedAt, want)
	}
	for _, it := range got {
		if err := it.Validate(); err != nil {
			t.Errorf("gdelt item fails contract validation: %v", err)
		}
	}
}

func TestGDELT_DedupsAcrossPolls(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte(gdeltBody))
	}))
	defer srv.Close()

	// Short interval so multiple polls happen; the seen-set must suppress repeats.
	g := &GDELT{Query: "stocks", BaseURL: srv.URL, PollInterval: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	out := make(chan contract.NewsRaw, 128)
	done := make(chan struct{})
	go func() { _ = g.Run(ctx, out); close(done) }()
	<-done
	close(out)

	var count int
	for range out {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 unique items across %d polls, got %d", hits, count)
	}
}

func TestGDELT_TransientErrorDoesNotCrash(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(gdeltBody))
	}))
	defer srv.Close()

	// First poll 500s (logged + skipped), second succeeds.
	g := &GDELT{Query: "stocks", BaseURL: srv.URL, PollInterval: 10 * time.Millisecond}
	got := drainN(t, g, 2)
	if len(got) < 2 {
		t.Fatalf("expected recovery after transient error, got %d", len(got))
	}
}

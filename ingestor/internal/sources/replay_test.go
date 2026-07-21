package sources

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

func writeJSONL(t *testing.T, lines ...string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.jsonl")
	content := ""
	for _, l := range lines {
		content += l + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp jsonl: %v", err)
	}
	return path
}

const rawLine1 = `{"schema_version":"1.0.0","id":"replay-1","source":"replay","title":"A","body":"body a","published_at":"2026-07-20T12:00:00Z","ingested_at":"2026-07-20T12:00:00Z"}`
const rawLine2 = `{"schema_version":"1.0.0","id":"replay-2","source":"replay","title":"B","body":"body b","published_at":"2026-07-20T12:01:00Z","ingested_at":"2026-07-20T12:01:00Z"}`

func collect(t *testing.T, r *Replay, ctx context.Context) []contract.NewsRaw {
	t.Helper()
	out := make(chan contract.NewsRaw, 16)
	errCh := make(chan error, 1)
	go func() {
		err := r.Run(ctx, out)
		close(out)
		errCh <- err
	}()
	var got []contract.NewsRaw
	for item := range out {
		got = append(got, item)
	}
	if err := <-errCh; err != nil && ctx.Err() == nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	return got
}

func TestReplay_StreamsAllItems(t *testing.T) {
	path := writeJSONL(t, rawLine1, "", rawLine2) // blank line should be skipped
	r := &Replay{Path: path, RatePerSec: 0}
	got := collect(t, r, context.Background())
	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
	if got[0].ID != "replay-1" || got[1].ID != "replay-2" {
		t.Errorf("unexpected ids: %s, %s", got[0].ID, got[1].ID)
	}
}

func TestReplay_StampsIngestedAt(t *testing.T) {
	path := writeJSONL(t, rawLine1)
	fixed := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &Replay{Path: path, Now: func() time.Time { return fixed }}
	got := collect(t, r, context.Background())
	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}
	if !got[0].IngestedAt.Equal(fixed) {
		t.Errorf("ingested_at = %v, want %v (source must stamp ingestion time)", got[0].IngestedAt, fixed)
	}
}

func TestReplay_InvalidLineIsError(t *testing.T) {
	path := writeJSONL(t, `{"id":`) // malformed JSON
	r := &Replay{Path: path}
	out := make(chan contract.NewsRaw, 4)
	err := r.Run(context.Background(), out)
	if err == nil {
		t.Fatal("expected error on malformed line")
	}
}

func TestReplay_MissingFileIsError(t *testing.T) {
	r := &Replay{Path: filepath.Join(t.TempDir(), "nope.jsonl")}
	if err := r.Run(context.Background(), make(chan contract.NewsRaw, 1)); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestReplay_ContextCancelStops(t *testing.T) {
	path := writeJSONL(t, rawLine1, rawLine2)
	// High interval so cancellation happens mid-stream.
	r := &Replay{Path: path, RatePerSec: 1, Loop: true}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan contract.NewsRaw, 64)
	errCh := make(chan error, 1)
	go func() {
		err := r.Run(ctx, out)
		close(out)
		errCh <- err
	}()
	for range out {
	}
	if err := <-errCh; err == nil {
		t.Fatal("expected ctx error on cancellation")
	}
}

// Package sources implements the ingestion sources that feed raw items into the
// pipeline. Replay is the offline default; live GDELT/RSS sources land in Phase 8.
package sources

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

// Replay streams a JSONL dataset of raw news items onto a channel at a
// configurable rate, so the whole pipeline runs offline with zero keys. It is
// the mandatory Docker default.
type Replay struct {
	// Path is the JSONL file to stream (one contract.NewsRaw per line).
	Path string
	// RatePerSec caps emission to this many items/sec; <= 0 means as fast as the
	// downstream can accept (bounded only by the output channel = backpressure).
	RatePerSec float64
	// Loop restarts from the top of the file when it ends (useful for demos and
	// load tests). When false, Run returns nil after one pass.
	Loop bool
	// Now is an injectable clock, defaulting to time.Now. Kept for deterministic
	// tests.
	Now func() time.Time
}

// Run streams the dataset onto out until the file is exhausted (Loop == false)
// or ctx is cancelled. It does NOT close out; the caller owns the channel so
// multiple sources can fan in. On ctx cancellation it returns ctx.Err().
func (r *Replay) Run(ctx context.Context, out chan<- contract.NewsRaw) error {
	now := r.Now
	if now == nil {
		now = time.Now
	}

	var interval time.Duration
	if r.RatePerSec > 0 {
		interval = time.Duration(float64(time.Second) / r.RatePerSec)
	}

	for {
		if err := r.streamOnce(ctx, out, now, interval); err != nil {
			return err
		}
		if !r.Loop {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (r *Replay) streamOnce(ctx context.Context, out chan<- contract.NewsRaw, now func() time.Time, interval time.Duration) error {
	f, err := os.Open(r.Path)
	if err != nil {
		return fmt.Errorf("open replay file: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// Allow long article bodies on a single line.
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}

		var item contract.NewsRaw
		if err := json.Unmarshal(line, &item); err != nil {
			return fmt.Errorf("line %d: unmarshal: %w", lineNo, err)
		}

		// Fill defaults, then stamp ingestion time: this item is entering the
		// pipeline right now, regardless of when the dataset was authored.
		if item.SchemaVersion == "" {
			item.SchemaVersion = contract.SchemaVersion
		}
		if item.Source == "" {
			item.Source = contract.SourceReplay
		}
		item.IngestedAt = now().UTC()

		if err := item.Validate(); err != nil {
			return fmt.Errorf("line %d: invalid item: %w", lineNo, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- item:
		}

		if interval > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan replay file: %w", err)
	}
	return nil
}

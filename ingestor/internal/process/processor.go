package process

import (
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

// Decision is the outcome of processing one raw item.
type Decision int

const (
	// DecisionEmit means the item is clean, unique, and relevant — forward it.
	DecisionEmit Decision = iota
	// DecisionFiltered means the item was dropped by the filter.
	DecisionFiltered
	// DecisionDuplicate means the item's content was already forwarded.
	DecisionDuplicate
)

// Processor applies normalize -> filter -> dedup and builds the NewsProcessed
// output. It is safe for concurrent use (the Deduper is locked), though the
// processor service drives it from a single goroutine to keep offset commits
// ordered.
type Processor struct {
	dedup  *Deduper
	filter FilterConfig
}

// NewProcessor builds a Processor with the given dedup capacity and filter config.
func NewProcessor(dedupSize int, filter FilterConfig) *Processor {
	return &Processor{dedup: NewDeduper(dedupSize), filter: filter}
}

// Process normalizes and evaluates a raw item. On DecisionEmit it returns the
// NewsProcessed to forward; on any other decision the NewsProcessed is zero and
// reason explains a filter drop. now is the processing timestamp (injected for
// deterministic tests).
func (p *Processor) Process(raw contract.NewsRaw, now time.Time) (contract.NewsProcessed, Decision, string) {
	title := Normalize(raw.Title)
	body := Normalize(raw.Body)

	if keep, reason := Filter(title, body, raw.Language, p.filter); !keep {
		return contract.NewsProcessed{}, DecisionFiltered, reason
	}

	hash := ContentHash(title, body)
	if p.dedup.Seen(hash) {
		return contract.NewsProcessed{}, DecisionDuplicate, ""
	}

	out := contract.NewsProcessed{
		SchemaVersion: contract.SchemaVersion,
		ID:            raw.ID,
		ContentHash:   hash,
		Source:        raw.Source,
		Feed:          raw.Feed,
		URL:           raw.URL,
		Title:         title,
		Body:          body,
		Language:      raw.Language,
		PublishedAt:   raw.PublishedAt,
		IngestedAt:    raw.IngestedAt,
		ProcessedAt:   now.UTC(),
		Tickers:       raw.Tickers,
	}
	return out, DecisionEmit, ""
}

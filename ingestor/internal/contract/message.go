// Package contract is the Go side of the shared Kafka message contract defined
// once in schemas/. Kafka is the language boundary: Go produces messages that
// conform to these types and the JSON Schemas, and Python consumes them. Keep
// these structs and schemas/*.schema.json in lockstep.
package contract

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// SchemaVersion mirrors `schema_version` in schemas/*.schema.json. It is a
// const so a version drift fails loudly instead of silently corrupting the
// stream across the language boundary.
const SchemaVersion = "1.0.0"

// Source enumerates where a raw item came from. Mirrors the `source` enum in
// schemas/news.raw.schema.json.
type Source string

const (
	SourceReplay Source = "replay"
	SourceGDELT  Source = "gdelt"
	SourceRSS    Source = "rss"
)

func (s Source) valid() bool {
	switch s {
	case SourceReplay, SourceGDELT, SourceRSS:
		return true
	default:
		return false
	}
}

// NewsRaw is a raw news item produced to the `news.raw` topic. The JSON tags are
// the wire contract — they must match schemas/news.raw.schema.json exactly.
type NewsRaw struct {
	SchemaVersion string    `json:"schema_version"`
	ID            string    `json:"id"`
	Source        Source    `json:"source"`
	Feed          string    `json:"feed,omitempty"`
	URL           string    `json:"url,omitempty"`
	Title         string    `json:"title"`
	Body          string    `json:"body"`
	Language      string    `json:"language,omitempty"`
	PublishedAt   time.Time `json:"published_at"`
	IngestedAt    time.Time `json:"ingested_at"`
	Tickers       []string  `json:"tickers,omitempty"`
}

// Key returns the Kafka message key. `id` is the key so the same item lands on
// the same partition across news.raw and news.processed.
func (n NewsRaw) Key() []byte { return []byte(n.ID) }

// contentHashRe mirrors the content_hash pattern in
// schemas/news.processed.schema.json.
var contentHashRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// NewsProcessed is a clean, deduplicated, normalized item produced to the
// `news.processed` topic — the input to the Python inference layer. It is a
// superset of NewsRaw: identity/content fields carry over, plus `content_hash`
// (the dedup key) and `processed_at`. JSON tags must match
// schemas/news.processed.schema.json exactly. Model output (sentiment/entities)
// never appears here; Python writes that to the datastore, not back onto Kafka.
type NewsProcessed struct {
	SchemaVersion string    `json:"schema_version"`
	ID            string    `json:"id"`
	ContentHash   string    `json:"content_hash"`
	Source        Source    `json:"source"`
	Feed          string    `json:"feed,omitempty"`
	URL           string    `json:"url,omitempty"`
	Title         string    `json:"title"`
	Body          string    `json:"body"`
	Language      string    `json:"language,omitempty"`
	PublishedAt   time.Time `json:"published_at"`
	IngestedAt    time.Time `json:"ingested_at"`
	ProcessedAt   time.Time `json:"processed_at"`
	Tickers       []string  `json:"tickers,omitempty"`
}

// Key returns the Kafka message key (the item id), keeping an item on the same
// partition across news.raw and news.processed.
func (n NewsProcessed) Key() []byte { return []byte(n.ID) }

// Validate enforces the invariants asserted by
// schemas/news.processed.schema.json.
func (n NewsProcessed) Validate() error {
	switch {
	case n.SchemaVersion != SchemaVersion:
		return fmt.Errorf("schema_version %q != %q", n.SchemaVersion, SchemaVersion)
	case n.ID == "":
		return errors.New("id is required")
	case !contentHashRe.MatchString(n.ContentHash):
		return fmt.Errorf("content_hash %q does not match sha256:<64 hex>", n.ContentHash)
	case !n.Source.valid():
		return fmt.Errorf("invalid source %q", n.Source)
	case n.Title == "":
		return errors.New("title is required")
	case n.Body == "":
		return errors.New("body is required")
	case n.PublishedAt.IsZero():
		return errors.New("published_at is required")
	case n.IngestedAt.IsZero():
		return errors.New("ingested_at is required")
	case n.ProcessedAt.IsZero():
		return errors.New("processed_at is required")
	}
	return nil
}

// Validate enforces the required-field / enum invariants that the JSON Schema
// asserts on the wire, so we never produce a message the Python side rejects.
func (n NewsRaw) Validate() error {
	switch {
	case n.SchemaVersion != SchemaVersion:
		return fmt.Errorf("schema_version %q != %q", n.SchemaVersion, SchemaVersion)
	case n.ID == "":
		return errors.New("id is required")
	case !n.Source.valid():
		return fmt.Errorf("invalid source %q", n.Source)
	case n.Title == "":
		return errors.New("title is required")
	case n.Body == "":
		return errors.New("body is required")
	case n.PublishedAt.IsZero():
		return errors.New("published_at is required")
	case n.IngestedAt.IsZero():
		return errors.New("ingested_at is required")
	}
	return nil
}

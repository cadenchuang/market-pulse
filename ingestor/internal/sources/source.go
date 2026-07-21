package sources

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

// Source is an ingestion source that streams raw news items onto a channel until
// it is exhausted or ctx is cancelled. Implementations must NOT close out — the
// caller owns the channel so multiple sources can fan in (see run.go). Live
// sources (GDELT, RSS) poll forever and only return on ctx cancellation; the
// replay source returns nil after one pass (unless Loop is set).
type Source interface {
	// Name is a short, stable identifier used in logs and metrics.
	Name() string
	// Run streams items onto out. It returns ctx.Err() on cancellation, or a
	// non-nil error only for unrecoverable startup failures. Transient
	// fetch/parse errors during polling must be handled internally (log + skip),
	// never propagated, so one bad poll cannot kill the pipeline.
	Run(ctx context.Context, out chan<- contract.NewsRaw) error
}

// Name identifies the replay source.
func (r *Replay) Name() string { return "replay" }

// normalizeLang maps assorted language spellings (GDELT's "English", RSS's
// "en-US", etc.) to a lowercase 2-letter code, or "" if unknown. The contract's
// language field is optional, so "" is a valid "unspecified".
func normalizeLang(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "", "und", "unknown":
		return ""
	case "english":
		return "en"
	}
	// "en-us" / "en_gb" -> "en"
	if i := strings.IndexAny(s, "-_"); i > 0 {
		s = s[:i]
	}
	return s
}

// hashID derives a short, stable id from a source-unique string (usually the
// article URL or feed guid), so the same article maps to the same Kafka key
// across polls and the downstream dedup/processor can collapse repeats.
func hashID(prefix, unique string) string {
	sum := sha1.Sum([]byte(unique))
	return prefix + "-" + hex.EncodeToString(sum[:8])
}

// seenSet is a tiny bounded set used by live sources to avoid re-emitting the
// same article on every poll. It is best-effort: when full it evicts in FIFO
// order. The processor stage still dedups authoritatively by content hash, so
// this is only a politeness/efficiency optimization.
type seenSet struct {
	mu    sync.Mutex
	max   int
	set   map[string]struct{}
	order []string
}

func newSeenSet(max int) *seenSet {
	if max <= 0 {
		max = 4096
	}
	return &seenSet{max: max, set: make(map[string]struct{}, max)}
}

// add reports whether key was newly added (true) or already present (false).
func (s *seenSet) add(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[key]; ok {
		return false
	}
	if len(s.order) >= s.max {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.set, oldest)
	}
	s.set[key] = struct{}{}
	s.order = append(s.order, key)
	return true
}

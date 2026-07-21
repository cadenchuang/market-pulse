package process

import "sync"

// Deduper is a bounded, in-memory set of content hashes with FIFO eviction. It
// makes the processing stage skip items whose normalized content it has already
// forwarded.
//
// Scope & limitation: dedup is per-processor-instance and best-effort. Because
// news.raw is partitioned by item id, identical content published under
// different ids can land on different partitions handled by different processor
// replicas, so cross-instance dedup is not guaranteed. A shared store (e.g.
// Redis) would be needed for exactly-once global dedup; that is out of scope for
// this portfolio project and documented here on purpose.
type Deduper struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string
	max   int
}

// NewDeduper returns a Deduper that remembers up to max recent hashes.
func NewDeduper(max int) *Deduper {
	if max < 1 {
		max = 1
	}
	return &Deduper{seen: make(map[string]struct{}, max), max: max}
}

// Seen reports whether hash was already recorded. If not, it records it (evicting
// the oldest hash when over capacity) and returns false. Safe for concurrent use.
func (d *Deduper) Seen(hash string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.seen[hash]; ok {
		return true
	}
	d.seen[hash] = struct{}{}
	d.order = append(d.order, hash)
	if len(d.order) > d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
	return false
}

// Len returns the number of hashes currently retained.
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

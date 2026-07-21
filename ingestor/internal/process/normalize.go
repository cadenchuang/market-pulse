// Package process holds the cheap, high-throughput transforms the Go processing
// stage applies to every raw item before the expensive Python model ever sees
// it: normalize, content-hash dedup, and filter. Doing this in Go is the whole
// point of the two-stage pipeline — the model must never run on duplicate or
// irrelevant items.
package process

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalize canonicalizes text so that trivially-different strings dedup to the
// same content hash and the model sees consistent input. It applies Unicode NFC,
// drops control characters, collapses all whitespace runs to a single space,
// lowercases, and trims. Lowercasing is safe here: FinBERT uses an uncased
// tokenizer, and it makes dedup robust to casing differences.
func Normalize(s string) string {
	s = norm.NFC.String(s)

	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	for _, r := range s {
		switch {
		case unicode.IsSpace(r):
			// Any whitespace run collapses to a single separating space.
			pendingSpace = true
		case unicode.IsControl(r):
			// Non-space control characters are dropped entirely.
		default:
			if pendingSpace && b.Len() > 0 {
				b.WriteRune(' ')
			}
			pendingSpace = false
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// ContentHash returns the dedup key over normalized title + body, formatted as
// `sha256:<64 lowercase hex>` to match schemas/news.processed.schema.json.
func ContentHash(normalizedTitle, normalizedBody string) string {
	sum := sha256.Sum256([]byte(normalizedTitle + "\n" + normalizedBody))
	return "sha256:" + hex.EncodeToString(sum[:])
}

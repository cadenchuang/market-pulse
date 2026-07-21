package process

import "unicode/utf8"

// FilterConfig controls which normalized items are worth sending to the model.
type FilterConfig struct {
	// MinBodyLen drops items whose normalized body is shorter than this (in
	// runes). 0 disables the length check.
	MinBodyLen int
	// Language, if non-empty, keeps only items whose language matches (items with
	// no language set are kept, since language is optional in the contract).
	Language string
}

// DefaultFilterConfig is a sensible default: drop near-empty items, keep English.
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{MinBodyLen: 20, Language: "en"}
}

// Filter reports whether a normalized item should be forwarded. When it returns
// false, reason is a short, metric-friendly label.
func Filter(normalizedTitle, normalizedBody, language string, cfg FilterConfig) (keep bool, reason string) {
	if normalizedTitle == "" && normalizedBody == "" {
		return false, "empty"
	}
	if cfg.MinBodyLen > 0 && utf8.RuneCountInString(normalizedBody) < cfg.MinBodyLen {
		return false, "too_short"
	}
	if cfg.Language != "" && language != "" && language != cfg.Language {
		return false, "language"
	}
	return true, ""
}

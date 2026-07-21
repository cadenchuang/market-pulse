package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

// DefaultGDELTBaseURL is the GDELT Doc 2.0 API endpoint. The API needs no key.
// NOTE (honest-claims): GDELT's ArtList mode returns article *metadata*
// (headline, url, domain, language, seen-date) — not full body text. We analyze
// the headline, so Body is set to the title. This is documented in the README.
const DefaultGDELTBaseURL = "https://api.gdeltproject.org/api/v2/doc/doc"

// GDELT polls the GDELT Doc 2.0 API for recent articles matching a query and
// streams them as raw items. It is rate-limited by PollInterval and is resilient
// to transient failures (log + back off, never crash the pipeline).
type GDELT struct {
	// Query is the GDELT search expression (e.g. `(stocks OR earnings) sourcelang:eng`).
	Query string
	// MaxRecords caps articles per poll (GDELT allows up to 250).
	MaxRecords int
	// PollInterval is the delay between polls. GDELT updates ~every 15 min, so a
	// short interval mostly returns dupes (dropped by the seen-set / processor).
	PollInterval time.Duration
	// Client is the HTTP client (injectable for tests). Defaults to a 15s client.
	Client *http.Client
	// BaseURL overrides the API endpoint (for tests). Defaults to DefaultGDELTBaseURL.
	BaseURL string
	// Now is an injectable clock (defaults to time.Now).
	Now func() time.Time

	seen *seenSet
}

// Name identifies the GDELT source.
func (g *GDELT) Name() string { return "gdelt" }

type gdeltResponse struct {
	Articles []gdeltArticle `json:"articles"`
}

type gdeltArticle struct {
	URL      string `json:"url"`
	Title    string `json:"title"`
	SeenDate string `json:"seendate"` // "20060102T150405Z"
	Domain   string `json:"domain"`
	Language string `json:"language"` // e.g. "English"
}

// Run polls until ctx is cancelled. Transient errors are logged and retried on
// the next tick; only ctx cancellation ends the loop.
func (g *GDELT) Run(ctx context.Context, out chan<- contract.NewsRaw) error {
	if g.Client == nil {
		g.Client = &http.Client{Timeout: 15 * time.Second}
	}
	if g.BaseURL == "" {
		g.BaseURL = DefaultGDELTBaseURL
	}
	if g.MaxRecords <= 0 {
		g.MaxRecords = 75
	}
	if g.PollInterval <= 0 {
		g.PollInterval = 60 * time.Second
	}
	if g.Now == nil {
		g.Now = time.Now
	}
	if g.seen == nil {
		g.seen = newSeenSet(8192)
	}

	timer := time.NewTimer(0) // fire immediately for the first poll
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if err := g.pollOnce(ctx, out); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("gdelt: poll error (will retry): %v", err)
		}
		timer.Reset(g.PollInterval)
	}
}

func (g *GDELT) pollOnce(ctx context.Context, out chan<- contract.NewsRaw) error {
	endpoint := g.buildURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "market-pulse/1.0 (portfolio project)")

	resp, err := g.Client.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed gdeltResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}

	for _, a := range parsed.Articles {
		item, ok := g.toItem(a)
		if !ok {
			continue
		}
		if !g.seen.add(a.URL) {
			continue // already emitted in a previous poll
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- item:
		}
	}
	return nil
}

func (g *GDELT) buildURL() string {
	q := url.Values{}
	q.Set("query", g.Query)
	q.Set("mode", "ArtList")
	q.Set("format", "json")
	q.Set("maxrecords", fmt.Sprintf("%d", g.MaxRecords))
	q.Set("sort", "DateDesc")
	return g.BaseURL + "?" + q.Encode()
}

// toItem converts a GDELT article to a NewsRaw. Returns ok=false when the
// article lacks the minimum required fields.
func (g *GDELT) toItem(a gdeltArticle) (contract.NewsRaw, bool) {
	title := strings.TrimSpace(a.Title)
	if title == "" || strings.TrimSpace(a.URL) == "" {
		return contract.NewsRaw{}, false
	}
	published := parseGDELTDate(a.SeenDate)
	if published.IsZero() {
		published = g.Now().UTC()
	}
	return contract.NewsRaw{
		SchemaVersion: contract.SchemaVersion,
		ID:            hashID("gdelt", a.URL),
		Source:        contract.SourceGDELT,
		Feed:          a.Domain,
		URL:           a.URL,
		Title:         title,
		Body:          title, // GDELT provides no full text; the headline is the analyzed text
		Language:      normalizeLang(a.Language),
		PublishedAt:   published,
		IngestedAt:    g.Now().UTC(),
	}, true
}

// parseGDELTDate parses GDELT's compact "20060102T150405Z" timestamp.
func parseGDELTDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse("20060102T150405Z", s); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

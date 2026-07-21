package sources

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

// RSS polls a set of RSS 2.0 / Atom feeds and streams their entries as raw
// items. It is rate-limited by PollInterval and resilient to per-feed failures:
// a broken or unreachable feed is logged and skipped, never fatal.
type RSS struct {
	// Feeds is the list of feed URLs to poll.
	Feeds []string
	// PollInterval is the delay between poll rounds across all feeds.
	PollInterval time.Duration
	// Client is the HTTP client (injectable for tests). Defaults to a 15s client.
	Client *http.Client
	// Now is an injectable clock (defaults to time.Now).
	Now func() time.Time

	seen *seenSet
}

// Name identifies the RSS source.
func (r *RSS) Name() string { return "rss" }

// --- Feed XML shapes (RSS 2.0 + Atom) parsed with encoding/xml. ---

type rssFeed struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		Language string    `xml:"language"`
		Items    []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	GUID        string `xml:"guid"`
}

type atomFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Lang    string      `xml:"lang,attr"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title   string `xml:"title"`
	ID      string `xml:"id"`
	Updated string `xml:"updated"`
	Summary string `xml:"summary"`
	Content string `xml:"content"`
	Links   []struct {
		Href string `xml:"href,attr"`
		Rel  string `xml:"rel,attr"`
	} `xml:"link"`
}

// Run polls all feeds every PollInterval until ctx is cancelled.
func (r *RSS) Run(ctx context.Context, out chan<- contract.NewsRaw) error {
	if r.Client == nil {
		r.Client = &http.Client{Timeout: 15 * time.Second}
	}
	if r.PollInterval <= 0 {
		r.PollInterval = 60 * time.Second
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.seen == nil {
		r.seen = newSeenSet(8192)
	}

	timer := time.NewTimer(0) // first round immediately
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		for _, feed := range r.Feeds {
			if err := r.pollFeed(ctx, feed, out); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("rss: feed %q error (will retry): %v", feed, err)
			}
		}
		timer.Reset(r.PollInterval)
	}
}

func (r *RSS) pollFeed(ctx context.Context, feedURL string, out chan<- contract.NewsRaw) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", "market-pulse/1.0 (portfolio project)")
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml, text/xml")

	resp, err := r.Client.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	items, err := parseFeed(body, feedURL, r.Now)
	if err != nil {
		return err
	}
	for _, item := range items {
		if !r.seen.add(item.ID) {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- item:
		}
	}
	return nil
}

// parseFeed decodes either RSS 2.0 or Atom and returns valid NewsRaw items.
func parseFeed(body []byte, feedURL string, now func() time.Time) ([]contract.NewsRaw, error) {
	trimmed := strings.TrimSpace(string(body))

	// Try RSS 2.0 first, then Atom, based on the root element.
	if strings.Contains(trimmed[:min(256, len(trimmed))], "<feed") {
		return parseAtom(body, feedURL, now)
	}
	return parseRSS(body, feedURL, now)
}

func parseRSS(body []byte, feedURL string, now func() time.Time) ([]contract.NewsRaw, error) {
	var f rssFeed
	if err := xml.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("parse rss: %w", err)
	}
	lang := normalizeLang(f.Channel.Language)
	var out []contract.NewsRaw
	for _, it := range f.Channel.Items {
		title := strings.TrimSpace(it.Title)
		if title == "" {
			continue
		}
		unique := firstNonEmpty(it.GUID, it.Link, feedURL+"|"+title)
		body := cleanText(it.Description)
		if body == "" {
			body = title
		}
		published := parseFeedDate(it.PubDate)
		if published.IsZero() {
			published = now().UTC()
		}
		item := contract.NewsRaw{
			SchemaVersion: contract.SchemaVersion,
			ID:            hashID("rss", unique),
			Source:        contract.SourceRSS,
			Feed:          feedURL,
			URL:           strings.TrimSpace(it.Link),
			Title:         title,
			Body:          body,
			Language:      lang,
			PublishedAt:   published,
			IngestedAt:    now().UTC(),
		}
		if item.Validate() == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

func parseAtom(body []byte, feedURL string, now func() time.Time) ([]contract.NewsRaw, error) {
	var f atomFeed
	if err := xml.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("parse atom: %w", err)
	}
	lang := normalizeLang(f.Lang)
	var out []contract.NewsRaw
	for _, e := range f.Entries {
		title := strings.TrimSpace(e.Title)
		if title == "" {
			continue
		}
		link := atomLink(e.Links)
		unique := firstNonEmpty(e.ID, link, feedURL+"|"+title)
		text := cleanText(firstNonEmpty(e.Content, e.Summary))
		if text == "" {
			text = title
		}
		published := parseFeedDate(e.Updated)
		if published.IsZero() {
			published = now().UTC()
		}
		item := contract.NewsRaw{
			SchemaVersion: contract.SchemaVersion,
			ID:            hashID("rss", unique),
			Source:        contract.SourceRSS,
			Feed:          feedURL,
			URL:           link,
			Title:         title,
			Body:          text,
			Language:      lang,
			PublishedAt:   published,
			IngestedAt:    now().UTC(),
		}
		if item.Validate() == nil {
			out = append(out, item)
		}
	}
	return out, nil
}

// atomLink prefers the rel="alternate" link, falling back to the first one.
func atomLink(links []struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}) string {
	for _, l := range links {
		if l.Rel == "alternate" || l.Rel == "" {
			return strings.TrimSpace(l.Href)
		}
	}
	if len(links) > 0 {
		return strings.TrimSpace(links[0].Href)
	}
	return ""
}

var tagRe = regexp.MustCompile(`<[^>]*>`)

// cleanText strips HTML tags and unescapes entities from feed descriptions, so
// the model sees plain text rather than markup.
func cleanText(s string) string {
	s = tagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

// parseFeedDate tries the common RSS/Atom date formats.
func parseFeedDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	formats := []string{
		time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		time.RFC3339, "2006-01-02T15:04:05Z07:00", "Mon, 2 Jan 2006 15:04:05 -0700",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

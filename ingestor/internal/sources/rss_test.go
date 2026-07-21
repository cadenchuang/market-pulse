package sources

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cadenchuang/market-pulse/ingestor/internal/contract"
)

const rssBody = `<?xml version="1.0"?>
<rss version="2.0"><channel>
  <title>Market Feed</title>
  <language>en-US</language>
  <item>
    <title>Gamma Ltd raises guidance</title>
    <link>https://feed.com/1</link>
    <description>&lt;p&gt;Profit &lt;b&gt;rises&lt;/b&gt; sharply this quarter.&lt;/p&gt;</description>
    <pubDate>Mon, 20 Jul 2026 12:00:00 +0000</pubDate>
    <guid>guid-1</guid>
  </item>
  <item>
    <title>Delta Co cuts outlook</title>
    <link>https://feed.com/2</link>
    <description>Shares slide on the news.</description>
    <pubDate>Mon, 20 Jul 2026 12:05:00 +0000</pubDate>
  </item>
</channel></rss>`

const atomBody = `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="en">
  <title>Atom Market</title>
  <entry>
    <title>Epsilon PLC announces buyback</title>
    <id>tag:feed.com,2026:1</id>
    <link href="https://atom.com/1" rel="alternate"/>
    <updated>2026-07-20T12:00:00Z</updated>
    <summary>The board approved a large buyback program.</summary>
  </entry>
</feed>`

func TestRSS_ParsesRSS2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = w.Write([]byte(rssBody))
	}))
	defer srv.Close()

	r := &RSS{Feeds: []string{srv.URL}, PollInterval: time.Hour}
	got := drainN(t, r, 2)

	if got[0].Source != contract.SourceRSS {
		t.Errorf("source = %q, want rss", got[0].Source)
	}
	if got[0].Title != "Gamma Ltd raises guidance" {
		t.Errorf("title = %q", got[0].Title)
	}
	// HTML tags stripped, entities unescaped, whitespace collapsed.
	if got[0].Body != "Profit rises sharply this quarter." {
		t.Errorf("cleaned body = %q", got[0].Body)
	}
	if got[0].Language != "en" {
		t.Errorf("language = %q, want en", got[0].Language)
	}
	want := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	if !got[0].PublishedAt.Equal(want) {
		t.Errorf("published_at = %v, want %v", got[0].PublishedAt, want)
	}
	for _, it := range got {
		if err := it.Validate(); err != nil {
			t.Errorf("rss item fails contract validation: %v", err)
		}
	}
}

func TestRSS_ParsesAtom(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(atomBody))
	}))
	defer srv.Close()

	r := &RSS{Feeds: []string{srv.URL}, PollInterval: time.Hour}
	got := drainN(t, r, 1)

	if got[0].Title != "Epsilon PLC announces buyback" {
		t.Errorf("title = %q", got[0].Title)
	}
	if got[0].URL != "https://atom.com/1" {
		t.Errorf("url = %q", got[0].URL)
	}
	if got[0].Body != "The board approved a large buyback program." {
		t.Errorf("body = %q", got[0].Body)
	}
}

func TestParseFeedDate(t *testing.T) {
	cases := []string{
		"Mon, 20 Jul 2026 12:00:00 +0000",
		"2026-07-20T12:00:00Z",
	}
	want := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	for _, c := range cases {
		if got := parseFeedDate(c); !got.Equal(want) {
			t.Errorf("parseFeedDate(%q) = %v, want %v", c, got, want)
		}
	}
	if !parseFeedDate("garbage").IsZero() {
		t.Error("unparseable date should return zero time")
	}
}

func TestCleanText(t *testing.T) {
	in := "<p>Hello&nbsp;&amp; <b>world</b></p>\n  extra   spaces"
	got := cleanText(in)
	want := "Hello & world extra spaces"
	if got != want {
		t.Errorf("cleanText = %q, want %q", got, want)
	}
}

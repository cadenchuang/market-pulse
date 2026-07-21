package sources

import "testing"

func TestNormalizeLang(t *testing.T) {
	cases := map[string]string{
		"English": "en",
		"en-US":   "en",
		"en_GB":   "en",
		"EN":      "en",
		"fr":      "fr",
		"":        "",
		"und":     "",
		"Unknown": "",
	}
	for in, want := range cases {
		if got := normalizeLang(in); got != want {
			t.Errorf("normalizeLang(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHashIDStableAndPrefixed(t *testing.T) {
	a := hashID("gdelt", "https://example.com/x")
	b := hashID("gdelt", "https://example.com/x")
	c := hashID("gdelt", "https://example.com/y")
	if a != b {
		t.Errorf("hashID not stable: %q != %q", a, b)
	}
	if a == c {
		t.Errorf("hashID collision for different inputs: %q", a)
	}
	if a[:6] != "gdelt-" {
		t.Errorf("hashID missing prefix: %q", a)
	}
}

func TestSeenSetDeduplicatesAndEvicts(t *testing.T) {
	s := newSeenSet(2)
	if !s.add("a") || !s.add("b") {
		t.Fatal("first inserts should be new")
	}
	if s.add("a") {
		t.Fatal("duplicate insert should report not-new")
	}
	// Adding a third evicts the oldest ("a"), so "a" becomes new again.
	if !s.add("c") {
		t.Fatal("third distinct insert should be new")
	}
	if !s.add("a") {
		t.Fatal("after eviction, 'a' should be treated as new again")
	}
}

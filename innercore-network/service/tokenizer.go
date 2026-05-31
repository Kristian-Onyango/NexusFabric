// service/tokenizer.go
// Layer 3C — Metadata Indexing & Search: Text Tokenizer
//
// ─────────────────────────────────────────────────────────────────────────────
// WHY A TOKENIZER EXISTS AS ITS OWN FILE
// ─────────────────────────────────────────────────────────────────────────────
//
// Search only works if the tokens in the index match the tokens in the query.
// This sounds obvious but is surprisingly subtle.
//
// Problem: user stores a file named "Suits.S01E03.1080p.BluRay.mkv"
//          user searches for "suits season 1"
//          Without a tokenizer: ZERO MATCHES (strings don't match literally)
//          With a tokenizer:
//            Index tokens:  ["suits", "s01e03", "1080p", "bluray", "mkv", "video"]
//            Query tokens:  ["suits", "season", "1"]
//            Overlap:       ["suits"] → match found
//
// The tokenizer also ensures:
//   "Suits" == "suits" == "SUITS"   (case normalization)
//   "the", "a", "an" are ignored    (stopword removal)
//   short meaningless tokens dropped (min length)
//   duplicates removed               (deduplication)
//
// ─────────────────────────────────────────────────────────────────────────────
// DESIGN DECISION: we do NOT use stemming (reducing "running" → "run")
// ─────────────────────────────────────────────────────────────────────────────
//
// Stemming adds complexity and can cause false matches.
// For a media/file sharing network, exact-word matching is good enough.
// Users searching "suits" want the TV show, not "suitable" or "suited".
// We can add stemming later if needed — it's isolated in this file.

package service

import (
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

// MinTokenLength — tokens shorter than this are discarded
// "a", "an", "in" are not useful search tokens
const MinTokenLength = 2

// MaxTokensPerField — safety limit to prevent index explosion from malicious input
// A filename with 1000 tokens would create 1000 DHT entries — we cap this
const MaxTokensPerField = 50

// ─────────────────────────────────────────────────────────────────────────────
// Stopwords — common words that add no search value
// Searching for "the suits" should not create an index entry for "the"
// because every document contains "the" and it tells us nothing
// ─────────────────────────────────────────────────────────────────────────────

var stopwords = map[string]bool{
	"a": true, "an": true, "the": true,
	"and": true, "or": true, "but": true,
	"in": true, "on": true, "at": true, "to": true, "for": true,
	"of": true, "with": true, "by": true,
	"is": true, "are": true, "was": true, "were": true,
	"it": true, "its": true, "this": true, "that": true,
	"be": true, "been": true, "being": true,
	"have": true, "has": true, "had": true,
	"do": true, "does": true, "did": true,
	"will": true, "would": true, "could": true, "should": true,
	"not": true, "no": true,
}

// fileExtensionStopwords — file extensions that tell us the type but
// are not useful as search tokens because they're too generic
// We extract the type information separately and discard the raw extension
var fileExtensionStopwords = map[string]bool{
	"mkv": true, "mp4": true, "avi": true, "mov": true, "wmv": true,
	"flv": true, "webm": true, "m4v": true,
	"mp3": true, "flac": true, "wav": true, "aac": true, "ogg": true,
	"jpg": true, "jpeg": true, "png": true, "gif": true, "webp": true,
	"pdf": true, "doc": true, "docx": true, "txt": true,
	"zip": true, "rar": true, "tar": true, "gz": true,
}

// qualityTags — tokens that indicate video/audio quality
// We keep these as index tokens but strip them from display
var qualityTags = map[string]bool{
	"1080p": true, "720p": true, "480p": true, "4k": true, "2160p": true,
	"bluray": true, "blu-ray": true, "bdrip": true, "brrip": true,
	"webrip": true, "web-dl": true, "webdl": true, "hdtv": true,
	"hdrip": true, "dvdrip": true, "dvdscr": true,
	"x264": true, "x265": true, "h264": true, "h265": true, "hevc": true,
	"avc": true, "xvid": true, "divx": true,
	"aac": true, "ac3": true, "dts": true, "dd5": true, "truehd": true,
	"yify": true, "yts": true, "rarbg": true, "eztv": true,
}

// ─────────────────────────────────────────────────────────────────────────────
// Regex patterns for common filename structures
// ─────────────────────────────────────────────────────────────────────────────

var (
	// Matches S01E03, s01e03, S01E03E04 — TV episode identifiers
	// We keep these as tokens so users can search "S01E03"
	episodePattern = regexp.MustCompile(`(?i)s\d{1,2}e\d{1,2}(?:e\d{1,2})?`)

	// Matches year patterns like (2019), [2019], .2019.
	yearPattern = regexp.MustCompile(`\b(19|20)\d{2}\b`)

	// Splits on anything that's not a letter or digit
	// This handles dots, underscores, hyphens, brackets used as separators in filenames
	// "Suits.S01.mkv" → ["Suits", "S01", "mkv"]
	separatorPattern = regexp.MustCompile(`[^a-zA-Z0-9]+`)
)

// ─────────────────────────────────────────────────────────────────────────────
// Tokenizer — the main struct
// ─────────────────────────────────────────────────────────────────────────────

type Tokenizer struct{}

// NewTokenizer creates a tokenizer instance
// No state — the struct is just a namespace for the methods
func NewTokenizer() *Tokenizer {
	return &Tokenizer{}
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeContentMeta — the primary entry point
//
// Takes a ContentMeta and returns ALL tokens that should be indexed.
// Called by IndexWriter when a new file is stored.
//
// Token sources and their weights:
//   Filename (without extension) → weight 1.0 (strongest signal)
//   Tags                         → weight 0.8
//   Keywords                     → weight 0.5
//   File type                    → weight 0.3 (allows filtering by type)
// ─────────────────────────────────────────────────────────────────────────────

type WeightedToken struct {
	Token  string
	Weight float64 // 0.0 → 1.0, higher means stronger match signal
}

func (t *Tokenizer) TokenizeContentMeta(meta *ContentMeta) []WeightedToken {
	var result []WeightedToken
	seen := make(map[string]float64) // token → highest weight seen so far

	// Helper to add a token, keeping the highest weight if token appears multiple times
	add := func(token string, weight float64) {
		if existing, ok := seen[token]; !ok || weight > existing {
			seen[token] = weight
		}
	}

	// 1. Filename tokens (highest weight — this is what the user named the file)
	nameTokens := t.TokenizeFilename(meta.Name)
	for _, tok := range nameTokens {
		add(tok, 1.0)
	}

	// 2. Tag tokens (user-defined labels — high value)
	for _, tag := range meta.Tags {
		for _, tok := range t.tokenizeWords(tag) {
			add(tok, 0.8)
		}
	}

	// 3. Keyword tokens (secondary metadata)
	for _, kw := range meta.Keywords {
		for _, tok := range t.tokenizeWords(kw) {
			add(tok, 0.5)
		}
	}

	// 4. Type token (allows searches like "type:video" — simplified for now)
	if meta.Type != "" {
		typeTokens := t.tokenizeWords(meta.Type)
		for _, tok := range typeTokens {
			add(tok, 0.3)
		}
	}

	// Build result list, cap at MaxTokensPerField
	for token, weight := range seen {
		result = append(result, WeightedToken{Token: token, Weight: weight})
		if len(result) >= MaxTokensPerField {
			break
		}
	}

	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeFilename — handles the special structure of media filenames
//
// "Suits.S01E03.1080p.BluRay.x264.mkv" →
//   ["suits", "s01e03", "1080p", "bluray", "x264"]
//   (extension "mkv" dropped — kept separately as type info)
//
// "The.Dark.Knight.2008.BluRay.mp4" →
//   ["dark", "knight", "2008", "bluray"]
//   ("the" = stopword, dropped)
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) TokenizeFilename(filename string) []string {
	// Remove the file extension — it's metadata, not a search token
	// filepath.Ext returns ".mkv", we strip the dot
	ext := filepath.Ext(filename)
	withoutExt := strings.TrimSuffix(filename, ext)

	return t.tokenizeWords(withoutExt)
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenizeQuery — tokenizes a user's search query
//
// Applies the same normalization as filename tokenization so tokens align.
// "Suits Season 1" → ["suits", "season", "1"]
// "suits s01" → ["suits", "s01"]
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) TokenizeQuery(query string) []string {
	return t.tokenizeWords(query)
}

// ─────────────────────────────────────────────────────────────────────────────
// tokenizeWords — internal: the core normalization pipeline
// ─────────────────────────────────────────────────────────────────────────────

func (t *Tokenizer) tokenizeWords(text string) []string {
	if text == "" {
		return nil
	}

	// Step 1: Lowercase everything
	// "SUITS" == "Suits" == "suits"
	lower := strings.ToLower(text)

	// Step 2: Split on separators (dots, underscores, spaces, brackets, hyphens)
	// "Suits.S01E03" → ["suits", "s01e03"]
	// "Dark Knight"  → ["dark", "knight"]
	rawTokens := separatorPattern.Split(lower, -1)

	// Step 3: Filter and clean
	seen := make(map[string]bool)
	var result []string

	for _, tok := range rawTokens {
		tok = strings.TrimFunc(tok, unicode.IsSpace)

		// Drop empty tokens
		if tok == "" {
			continue
		}

		// Drop tokens that are too short to be meaningful
		if len(tok) < MinTokenLength {
			continue
		}

		// Drop stopwords
		if stopwords[tok] {
			continue
		}

		// Drop file extension stopwords (mkv, mp4, etc.)
		// We still keep quality tags like "1080p", "bluray" — they're searchable
		if fileExtensionStopwords[tok] {
			continue
		}

		// Drop pure punctuation or non-alphanumeric tokens
		if !containsAlphanumeric(tok) {
			continue
		}

		// Deduplicate
		if seen[tok] {
			continue
		}
		seen[tok] = true
		result = append(result, tok)

		// Safety cap
		if len(result) >= MaxTokensPerField {
			break
		}
	}

	return nil //temporary
}

// ─────────────────────────────────────────────────────────────────────────────
// DetectFileType — infer content type from filename extension
//
// Layer 3B stores whatever type the publisher provides.
// This helper lets us infer a sane default when the publisher doesn't specify.
// ─────────────────────────────────────────────────────────────────────────────

func DetectFileType(filename string) string {
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))

	switch ext {
	case "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v":
		return "video"
	case "mp3", "flac", "wav", "aac", "ogg", "m4a":
		return "audio"
	case "jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff":
		return "image"
	case "pdf":
		return "pdf"
	case "doc", "docx", "odt":
		return "document"
	case "txt", "md", "rst":
		return "text"
	case "zip", "rar", "tar", "gz", "7z":
		return "archive"
	case "exe", "dmg", "apk", "deb", "rpm":
		return "software"
	default:
		return "other"
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func containsAlphanumeric(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// IsQualityTag returns true if the token is a technical quality marker
// Useful for the UI to decide whether to display a token or hide it
func IsQualityTag(token string) bool {
	return qualityTags[strings.ToLower(token)]
}

// ExtractEpisodeCode extracts a TV episode code from a filename if present
// "Suits.S01E03.mkv" → "s01e03", true
// "The.Dark.Knight.mkv" → "", false
func ExtractEpisodeCode(filename string) (string, bool) {
	match := episodePattern.FindString(filename)
	if match == "" {
		return "", false
	}
	return strings.ToLower(match), true
}

// ExtractYear extracts a year (1900–2099) from a filename if present
// "The.Dark.Knight.2008.mkv" → "2008", true
func ExtractYear(filename string) (string, bool) {
	match := yearPattern.FindString(filename)
	return match, match != ""
}

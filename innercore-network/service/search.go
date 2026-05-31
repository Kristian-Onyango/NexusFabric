// service/search.go
// Layer 3B — Distributed Search Index

package service

import (
	"fmt"
	"strings"
)

func IndexContent(meta *ContentMeta) {
	for _, kw := range meta.Keywords {
		fmt.Printf("[SEARCH] Indexed keyword '%s' for %s\n", kw, meta.Name)
	}
}

func searchInternal(keyword string) []*ContentRef {
	if cs := GetContentStore(); cs != nil {
		var results []*ContentRef
		for _, meta := range cs.ListLocalContent() {
			if strings.Contains(strings.ToLower(meta.Name), strings.ToLower(keyword)) {
				results = append(results, &ContentRef{
					ContentID: meta.ContentID,
					Name:      meta.Name,
					Type:      meta.Type,
					SizeBytes: meta.SizeBytes,
				})
			}
		}
		return results
	}
	return nil
}

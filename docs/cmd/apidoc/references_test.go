package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootReferencesMarkdownFences(t *testing.T) {
	for _, tc := range []struct {
		name, markdown string
		wantInvalid    bool
	}{
		{"backticks", "```go\nasync.Removed()\n```\n", true},
		{"tildes", "~~~go\nasync.Removed()\n~~~\n", true},
		{"indented tilde fence", "   ~~~~go\nasync.Removed()\n   ~~~~\n", true},
		{"shorter inner fence", "````go\n```\nasync.Removed()\n````\n", true},
		{"prose after tilde", "~~~go\nasync.Existing()\n~~~\nText mentions async.Removed().\n", false},
		{"mismatched close", "~~~go\n```\nasync.Removed()\n~~~\n", true},
		{"longer close", "~~~go\nasync.Existing()\n~~~~\nText mentions async.Removed().\n", false},
		{"close with info is content", "~~~go\n~~~go\nasync.Removed()\n~~~\n", true},
		{"invalid backtick info", "```go`bad\nText mentions async.Removed().\n", false},
		{"indented code", "    async.Removed()\n", true},
		{"four space run is code", "    ~~~~\nText mentions async.Removed().\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			if err := os.Mkdir("docs", 0700); err != nil {
				t.Fatal(err)
			}
			for path, data := range map[string]string{
				"async.go":                        "package async\nfunc Existing() {}\n",
				"README.md":                       tc.markdown,
				filepath.Join("docs", "guide.md"): tc.markdown,
			} {
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				checkRootReferences()
			}()
			if tc.wantInvalid {
				message := fmt.Sprint(recovered)
				if !strings.Contains(message, "README.md:") || !strings.Contains(message, "guide.md:") || !strings.Contains(message, "async.Removed") {
					t.Fatalf("expected both unknown API locations, got %v", recovered)
				}
			} else if recovered != nil {
				t.Fatalf("unexpected reference error: %v", recovered)
			}
		})
	}
}

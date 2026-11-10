package main

import (
	"errors"
	"os"
	"os/exec"
	"slices"
	"testing"
)

func TestCompileProgramsChecksTildeFences(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.Mkdir("docs", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("cache", 0700); err != nil {
		t.Fatal(err)
	}
	// An invalid complete program must fail compilation regardless of fence style.
	markdown := "~~~go\npackage main\nfunc main() { missingFunction() }\n~~~\n"
	if err := os.WriteFile("README.md", []byte(markdown), 0600); err != nil {
		t.Fatal(err)
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		compilePrograms("cache")
	}()
	compileErr, ok := recovered.(error)
	var exitErr *exec.ExitError
	if !ok || !errors.As(compileErr, &exitErr) {
		t.Fatalf("expected compiler failure for invalid tilde program, got %v", recovered)
	}
}

func TestMarkdownGoBlocks(t *testing.T) {
	for _, tc := range []struct {
		name, markdown string
		want           []string
	}{
		{"tildes and attributes", "~~~go title\na()\n~~~\n", []string{"a()\n"}},
		{"long backticks", "````go\na()\n```\nb()\n````\n", []string{"a()\n```\nb()\n"}},
		{"different marker is content", "~~~go\n```\na()\n~~~\n", []string{"```\na()\n"}},
		{"non Go block", "~~~text\n```go\na()\n```\n~~~\n", nil},
		{"unclosed", "~~~go\na()", []string{"a()\n"}},
		{"two blocks", "```go\na()\n```\n~~~go\nb()\n~~~", []string{"a()\n", "b()\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := markdownGoBlocks(tc.markdown); !slices.Equal(got, tc.want) {
				t.Fatalf("blocks=%q want=%q", got, tc.want)
			}
		})
	}
}

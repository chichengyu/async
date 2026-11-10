package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Check root-package selectors in fenced and indented Markdown examples. This catches removed or
// renamed entry points; it deliberately does not claim to type-check snippets.
func checkRootReferences() {
	fs := token.NewFileSet()
	public := map[string]bool{}
	files, err := filepath.Glob("*.go")
	must(err)
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fs, path, nil, 0)
		must(err)
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					public[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						public[s.Name.Name] = s.Name.IsExported()
					case *ast.ValueSpec:
						for _, n := range s.Names {
							public[n.Name] = n.IsExported()
						}
					}
				}
			}
		}
	}
	paths := []string{"README.md"}
	must(filepath.WalkDir("docs", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			paths = append(paths, path)
		}
		return nil
	}))
	var invalid []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		var fence byte
		fenceLength := 0
		for lineNo, line := range strings.Split(string(data), "\n") {
			marker, length, tail := markdownFence(line)
			if fence == 0 {
				if length >= 3 && (marker != '`' || !strings.ContainsRune(tail, '`')) {
					fence, fenceLength = marker, length
					continue
				}
			} else if marker == fence && length >= fenceLength && strings.TrimSpace(tail) == "" {
				fence, fenceLength = 0, 0
				continue
			}
			if fence == 0 && !strings.HasPrefix(line, "\t") && !strings.HasPrefix(line, "    ") {
				continue
			}
			var scan scanner.Scanner
			file := fs.AddFile(path, -1, len(line))
			scan.Init(file, []byte(line), nil, 0)
			state := 0
			for {
				_, tok, lit := scan.Scan()
				if tok == token.EOF {
					break
				}
				if state == 2 && tok == token.IDENT && !public[lit] {
					invalid = append(invalid, fmt.Sprintf("%s:%d: async.%s", path, lineNo+1, lit))
				}
				switch {
				case tok == token.IDENT && lit == "async":
					state = 1
				case state == 1 && tok == token.PERIOD:
					state = 2
				default:
					state = 0
				}
			}
		}
	}
	if len(invalid) > 0 {
		panic("unknown public API references:\n" + strings.Join(invalid, "\n"))
	}
}

// CommonMark fences allow up to three leading spaces. A closing fence must
// match the opening marker and be at least as long; shorter runs are content.
func markdownFence(line string) (marker byte, length int, tail string) {
	indent := 0
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}
	if indent > 3 || indent == len(line) || (line[indent] != '`' && line[indent] != '~') {
		return 0, 0, ""
	}
	marker = line[indent]
	end := indent
	for end < len(line) && line[end] == marker {
		end++
	}
	return marker, end - indent, line[end:]
}

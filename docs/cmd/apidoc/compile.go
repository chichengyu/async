package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// Compile the invocation shape of exported declarations in their own package.
// These functions are never executed: zero-value receivers are compile fixtures.
// This verifies arity, variadic expansion and generic constraints, not behavior.
func compileCalls() {
	dirs := []string{"."}
	children, err := os.ReadDir("internal")
	must(err)
	for _, c := range children {
		if c.IsDir() {
			dirs = append(dirs, filepath.Join("internal", c.Name()))
		}
	}
	overlay := map[string]string{}
	cache, err := os.MkdirTemp(".gotmp", "apidoc-compile-")
	must(err)
	count := 0
	for _, dir := range dirs {
		paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
		must(err)
		constraints := map[string]*ast.FieldList{}
		var parsed []*ast.File
		for _, path := range paths {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			must(err)
			parsed = append(parsed, f)
			for _, d := range f.Decls {
				if gd, ok := d.(*ast.GenDecl); ok && gd.Tok == token.TYPE {
					for _, spec := range gd.Specs {
						t := spec.(*ast.TypeSpec)
						constraints[t.Name.Name] = t.TypeParams
					}
				}
			}
		}
		for fileNo, f := range parsed {
			var body strings.Builder
			var functions []*ast.FuncDecl
			for _, decl := range f.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok {
					functions = append(functions, fn)
				}
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.TYPE {
					continue
				}
				for _, spec := range gd.Specs {
					typ := spec.(*ast.TypeSpec)
					if !typ.Name.IsExported() {
						continue
					}
					var fields *ast.FieldList
					switch t := typ.Type.(type) {
					case *ast.InterfaceType:
						fields = t.Methods
					case *ast.StructType:
						fields = t.Fields
					}
					if fields == nil {
						continue
					}
					var receiver ast.Expr = ast.NewIdent(typ.Name.Name)
					if typ.TypeParams != nil {
						var indices []ast.Expr
						for _, field := range typ.TypeParams.List {
							for _, n := range field.Names {
								indices = append(indices, ast.NewIdent(n.Name))
							}
						}
						receiver = &ast.IndexListExpr{X: receiver, Indices: indices}
					}
					for _, field := range fields.List {
						ft, ok := field.Type.(*ast.FuncType)
						if !ok || len(field.Names) == 0 {
							continue
						}
						functions = append(functions, &ast.FuncDecl{Name: field.Names[0], Type: ft, Recv: &ast.FieldList{List: []*ast.Field{{Type: receiver}}}})
					}
				}
			}
			for _, fn := range functions {
				if !fn.Name.IsExported() {
					continue
				}
				e := functionEntry(fn, f.Name.Name, "", 0)
				var params, args []string
				tp := fn.Type.TypeParams
				call := fn.Name.Name
				if fn.Recv != nil {
					params = append(params, "instance "+e.receiver)
					call = "instance." + fn.Name.Name
					r := fn.Recv.List[0].Type
					if star, ok := r.(*ast.StarExpr); ok {
						r = star.X
					}
					var base ast.Expr
					switch r := r.(type) {
					case *ast.IndexExpr:
						base = r.X
					case *ast.IndexListExpr:
						base = r.X
					}
					if id, ok := base.(*ast.Ident); ok {
						tp = constraints[id.Name]
					}
				} else if len(e.types) > 0 {
					call += "[" + strings.Join(e.types, ",") + "]"
				}
				for _, p := range e.params {
					params = append(params, p.name+" "+p.typ)
					arg := p.name
					if strings.HasPrefix(p.typ, "...") {
						arg += "..."
					}
					args = append(args, arg)
				}
				generic := ""
				if tp != nil {
					var fields []string
					for _, field := range tp.List {
						var names []string
						for _, n := range field.Names {
							names = append(names, n.Name)
						}
						fields = append(fields, strings.Join(names, ",")+" "+render(field.Type))
					}
					generic = "[" + strings.Join(fields, ",") + "]"
				}
				fmt.Fprintf(&body, "func apidocCall%d%s(%s) { %s(%s) }\n", count, generic, strings.Join(params, ","), call, strings.Join(args, ","))
				count++
			}
			if body.Len() == 0 {
				continue
			}
			text := body.String()
			fixture, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package p\n"+text, 0)
			must(err)
			used := map[string]bool{}
			ast.Inspect(fixture, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok {
						used[id.Name] = true
					}
				}
				return true
			})
			var out strings.Builder
			fmt.Fprintf(&out, "package %s\n", f.Name.Name)
			for _, imp := range f.Imports {
				p, err := strconv.Unquote(imp.Path.Value)
				must(err)
				alias := filepath.Base(p)
				if imp.Name != nil {
					alias = imp.Name.Name
				}
				if used[alias] {
					fmt.Fprintf(&out, "import %s %s\n", alias, imp.Path.Value)
				}
			}
			out.WriteString(text)
			name := fmt.Sprintf("zz_apidoc_%d_test.go", fileNo)
			actual, err := filepath.Abs(filepath.Join(cache, filepath.Base(dir)+"_"+name))
			must(err)
			virtual, err := filepath.Abs(filepath.Join(dir, name))
			must(err)
			must(os.WriteFile(actual, []byte(out.String()), 0644))
			overlay[virtual] = actual
		}
	}
	data, err := json.Marshal(map[string]any{"Replace": overlay})
	must(err)
	path, err := filepath.Abs(filepath.Join(cache, "overlay.json"))
	must(err)
	must(os.WriteFile(path, data, 0644))
	cmd := exec.Command("go", "test", "-overlay", path, "-run", "^$", ".", "./internal/...")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	must(cmd.Run())
	fmt.Printf("Compiled %d declaration call fragments (no business code executed).\n", count)
	compilePrograms(cache)
}

// Whole-program examples have all imports and business values defined, so their
// compilation checks more than a declaration-level call shape.
func compilePrograms(cache string) {
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
	count := 0
	for _, path := range paths {
		data, err := os.ReadFile(path)
		must(err)
		for _, code := range markdownGoBlocks(string(data)) {
			if !strings.HasPrefix(strings.TrimSpace(code), "package main") {
				continue
			}
			file := filepath.Join(cache, fmt.Sprintf("program%d.go", count))
			must(os.WriteFile(file, []byte(code), 0644))
			cmd := exec.Command("go", "test", "-run", "^$", file)
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			fmt.Printf("Compiling complete example from %s\n", path)
			must(cmd.Run())
			count++
		}
	}
	fmt.Printf("Compiled %d complete Markdown programs.\n", count)
}

// Extract Go blocks using the same fence rules as the API reference checker.
func markdownGoBlocks(markdown string) []string {
	var blocks []string
	var fence byte
	var fenceLength int
	var goBlock bool
	var code strings.Builder
	for _, line := range strings.Split(markdown, "\n") {
		marker, length, tail := markdownFence(line)
		if fence == 0 {
			if length >= 3 && (marker != '`' || !strings.ContainsRune(tail, '`')) {
				fence, fenceLength = marker, length
				info := strings.Fields(tail)
				goBlock = len(info) > 0 && info[0] == "go"
				code.Reset()
			}
			continue
		}
		if marker == fence && length >= fenceLength && strings.TrimSpace(tail) == "" {
			if goBlock {
				blocks = append(blocks, code.String())
			}
			fence, fenceLength = 0, 0
			continue
		}
		if goBlock {
			code.WriteString(line)
			code.WriteByte('\n')
		}
	}
	if fence != 0 && goBlock {
		blocks = append(blocks, code.String())
	}
	return blocks
}

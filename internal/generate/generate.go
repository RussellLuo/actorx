// Package generate implements the optional build-time binding generator.
package generate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// ActorPath is the runtime package used by generated bindings and type checks.
const ActorPath = "github.com/RussellLuo/actorx"

type listedPackage struct {
	Dir        string
	Name       string
	ImportPath string
	Export     string
	GoFiles    []string
	CgoFiles   []string
}

type method struct {
	decl             *ast.FuncDecl
	owner            string
	explicit, ignore bool
	condition        string
}

type target struct {
	name      string
	automatic bool
	methods   []method
	adapters  []string
}

func marker(doc *ast.CommentGroup, name string) bool {
	if doc == nil {
		return false
	}

	for _, c := range doc.List {
		if strings.TrimSpace(c.Text) == "//actorx:"+name {
			return true
		}
	}

	return false
}

func receiver(d *ast.FuncDecl) string {
	if d.Recv == nil || len(d.Recv.List) != 1 {
		return ""
	}

	x := d.Recv.List[0].Type
	if p, ok := x.(*ast.StarExpr); ok {
		x = p.X
	}
	if id, ok := x.(*ast.Ident); ok {
		return id.Name
	}

	return ""
}

// Run generates deterministic Bindings methods for the current build target.
// check compares the generated content without changing any file.
func Run(dir, output string, check bool) error {
	if filepath.Base(output) != output || !strings.HasSuffix(output, ".go") || strings.HasSuffix(output, "_test.go") {
		return fmt.Errorf("output must be a Go source filename in the target package")
	}

	absolute, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	// Load actorx export data even before the first generated import exists.
	cmd := exec.Command("go", "list", "-e", "-deps", "-export", "-json", ".", ActorPath)
	cmd.Dir = absolute
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("go list: %w: %s", err, stderr.String())
	}

	exports := map[string]string{}
	var pkg listedPackage
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var p listedPackage
		err := decoder.Decode(&p)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
		if p.Dir == absolute {
			pkg = p
		}
	}

	if pkg.Name == "" {
		return fmt.Errorf("no buildable Go package in %s", dir)
	}
	if len(pkg.CgoFiles) > 0 {
		return fmt.Errorf("cgo packages are not supported by actorx-gen")
	}

	// Collect declarations only from files selected for the current build target.
	fset := token.NewFileSet()
	platforms, err := buildPlatforms()
	if err != nil {
		return err
	}

	var files []*ast.File
	typeConditions := map[string]string{}
	targets := map[string]*target{}
	var methods []method
	for _, name := range pkg.GoFiles {
		if name == output {
			continue
		}

		file, err := parser.ParseFile(fset, filepath.Join(absolute, name), nil, parser.ParseComments)
		if err != nil {
			return err
		}

		files = append(files, file)
		condition, err := fileCondition(file, name, platforms)
		if err != nil {
			return err
		}

		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, s := range d.Specs {
					ts := s.(*ast.TypeSpec)
					typeConditions[ts.Name.Name] = condition
					if marker(ts.Doc, "actor") || marker(d.Doc, "actor") {
						if _, ok := ts.Type.(*ast.StructType); !ok || ts.Assign.IsValid() || ts.TypeParams != nil {
							return fmt.Errorf("%s: actor must be a non-generic struct", ts.Name.Name)
						}
						if len(d.Specs) > 1 && marker(d.Doc, "actor") {
							return fmt.Errorf("actorx:actor requires an unambiguous type declaration")
						}

						targets[ts.Name.Name] = &target{name: ts.Name.Name, automatic: true}
					}
				}
			case *ast.FuncDecl:
				owner := receiver(d)
				if owner == "" {
					if marker(d.Doc, "handler") {
						return fmt.Errorf("handler requires a non-generic receiver")
					}

					continue
				}
				methods = append(methods, method{
					decl:      d,
					owner:     owner,
					explicit:  marker(d.Doc, "handler"),
					ignore:    marker(d.Doc, "ignore"),
					condition: condition,
				})
			}
		}
	}

	// Resolve explicit handler selection before collecting each actor's methods.
	for _, m := range methods {
		if m.explicit {
			if !m.decl.Name.IsExported() {
				return fmt.Errorf("%s: handlers must be public methods", m.decl.Name.Name)
			}
			if m.ignore {
				return fmt.Errorf("%s: handler and ignore cannot mix", m.decl.Name.Name)
			}
			if t := targets[m.owner]; t != nil && t.automatic {
				return fmt.Errorf("%s: type and method selection modes cannot mix", m.owner)
			}
			if targets[m.owner] == nil {
				targets[m.owner] = &target{name: m.owner}
			}
		}
	}

	if len(targets) == 0 {
		return fmt.Errorf("no actorx:actor or actorx:handler declarations")
	}

	for _, m := range methods {
		t := targets[m.owner]
		if t == nil {
			continue
		}
		if m.decl.Name.Name == "Bindings" {
			return fmt.Errorf("%s: existing Bindings conflicts with generation", m.owner)
		}
		if m.ignore {
			continue
		}
		if (t.automatic && m.decl.Name.IsExported()) || m.explicit {
			t.methods = append(t.methods, m)
		}
	}

	names := make([]string, 0, len(targets))
	for name := range targets {
		names = append(names, name)
	}

	sort.Strings(names)

	// One output has one build condition. Refuse mixed variants instead of
	// silently dropping methods or narrowing unrelated actors to an intersection.
	condition := typeConditions[names[0]]
	for _, name := range names {
		t := targets[name]
		if typeConditions[name] != condition {
			return fmt.Errorf(
				"%s: mixed build constraints in one output; use handwritten Bindings for mixed variants",
				name,
			)
		}

		for _, m := range t.methods {
			if m.condition != condition {
				return fmt.Errorf(
					"%s.%s: mixed build constraints in one output; actor and selected handlers must share a build condition",
					name, m.decl.Name.Name,
				)
			}
		}
	}

	if suffix := filenameCondition(output, platforms); suffix != "" && suffix != condition {
		return fmt.Errorf(
			"output filename adds a different build constraint; use a filename without a GOOS/GOARCH suffix",
		)
	}

	// A package-level interface assertion may need Bindings during analysis.
	// Supply only the signatures that will be generated; normal type checking
	// still validates assertions and all author declarations.
	stubAlias := "actorx"
	for suffix := 0; sourceNameUsed(files, stubAlias); suffix++ {
		stubAlias = fmt.Sprintf("actorx%d", suffix)
	}
	var stubs strings.Builder
	fmt.Fprintf(&stubs, "package %s\nimport %s %q\n", pkg.Name, stubAlias, ActorPath)
	for _, name := range names {
		fmt.Fprintf(&stubs, "func (*%s) Bindings() []%s.Binding { return nil }\n", name, stubAlias)
	}

	stubFile, err := parser.ParseFile(fset, "actorx-signatures.go", stubs.String(), 0)
	if err != nil {
		return err
	}

	files = append(files, stubFile)

	lookup := func(path string) (io.ReadCloser, error) {
		if file := exports[path]; file != "" {
			return os.Open(file)
		}

		return nil, fmt.Errorf("missing export data for %s", path)
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	conf := types.Config{Importer: importer.ForCompiler(fset, "gc", lookup), IgnoreFuncBodies: true}
	typed, err := conf.Check(pkg.ImportPath, fset, files, info)
	if err != nil {
		return fmt.Errorf("type check: %w", err)
	}

	// Sort actors and handlers so output does not depend on map or file order.
	runtimeAlias := generatedName(typed, "actorx")
	receiverName := generatedName(typed, "a")
	runtimeImport := fmt.Sprintf("%q", ActorPath)
	if runtimeAlias != "actorx" {
		runtimeImport = runtimeAlias + " " + runtimeImport
	}
	var source bytes.Buffer
	if condition != "" {
		fmt.Fprintf(&source, "//go:build %s\n\n", condition)
	}

	fmt.Fprintf(&source,
		"// Code generated by actorx-gen. DO NOT EDIT.\n\npackage %s\n\nimport %s\n",
		typed.Name(), runtimeImport,
	)
	imports := map[string]string{}
	for _, name := range names {
		t := targets[name]
		obj := typed.Scope().Lookup(name)
		if obj == nil {
			return fmt.Errorf("unknown actor %s", name)
		}

		named, ok := obj.Type().(*types.Named)
		if !ok || named.TypeParams().Len() != 0 {
			return fmt.Errorf("%s: actor must be non-generic", name)
		}
		if _, ok := named.Underlying().(*types.Struct); !ok {
			return fmt.Errorf("%s: actor must be a struct", name)
		}

		sort.Slice(t.methods, func(i, j int) bool {
			return t.methods[i].decl.Name.Name < t.methods[j].decl.Name.Name
		})

		var messages []types.Type
		for _, m := range t.methods {
			fn, ok := info.Defs[m.decl.Name].(*types.Func)
			if !ok {
				return fmt.Errorf("missing method type %s", m.decl.Name.Name)
			}

			adapter, q, err := classify(fn.Type().(*types.Signature))
			if err != nil {
				return fmt.Errorf("%s.%s: %w", name, m.decl.Name.Name, err)
			}

			for _, prev := range messages {
				if types.Identical(prev, q) {
					return fmt.Errorf("%s: duplicate message type %s", name, q)
				}
			}

			messages = append(messages, q)
			handler := receiverName + "." + m.decl.Name.Name
			sig := fn.Type().(*types.Signature)
			if _, pointer := sig.Recv().Type().(*types.Pointer); !pointer {
				// Delay the implicit dereference until dispatch. TypeString uses
				// package aliases registered below for imported signature types.
				handler = delayedHandler(sig, m.decl.Name.Name, typed, imports, runtimeAlias, receiverName)
			}
			t.adapters = append(t.adapters, fmt.Sprintf("%s.%s(%s)", runtimeAlias, adapter, handler))
		}

		if len(t.adapters) == 0 {
			return fmt.Errorf("%s: no selected handlers", name)
		}

		fmt.Fprintf(&source, "\nfunc (%s *%s) Bindings() []%s.Binding {\nreturn []%s.Binding{\n", receiverName, name, runtimeAlias, runtimeAlias)
		for _, binding := range t.adapters {
			fmt.Fprintf(&source, "%s,\n", binding)
		}

		source.WriteString("}\n}\n")
	}

	raw := source.String()
	if len(imports) > 0 {
		paths := make([]string, 0, len(imports))
		for path := range imports {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		var extra strings.Builder
		for _, path := range paths {
			fmt.Fprintf(&extra, "import %s %q\n", imports[path], path)
		}
		marker := fmt.Sprintf("import %s\n", runtimeImport)
		raw = strings.Replace(raw, marker, marker+extra.String(), 1)
	}
	result, err := format.Source([]byte(raw))
	if err != nil {
		return err
	}

	path := filepath.Join(absolute, output)
	previous, readErr := os.ReadFile(path)
	if check {
		if readErr != nil || !bytes.Equal(previous, result) {
			return fmt.Errorf("%s is missing or stale; run actorx-gen", output)
		}

		return nil
	}
	if bytes.Equal(previous, result) {
		return nil
	}
	if readErr == nil && !generatedByActorx(previous) {
		return fmt.Errorf("refusing to overwrite non-generated file %s", output)
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		return readErr
	}

	// Publish only complete output; validation failures leave the old file intact.
	temporary, err := os.CreateTemp(absolute, ".actorx-gen-*")
	if err != nil {
		return err
	}

	defer os.Remove(temporary.Name())

	if _, err := temporary.Write(result); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0644); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}

	return os.Rename(temporary.Name(), path)
}

func namedType(t types.Type, path, name string) bool {
	n, ok := types.Unalias(t).(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == path && n.Obj().Name() == name
}

func classify(sig *types.Signature) (string, types.Type, error) {
	invalid := func() (string, types.Type, error) {
		return "", nil, fmt.Errorf(
			"expected (*actorx.Context, Q) (R, error), (*actorx.Context, M) error, or an explicit Reply/StreamReply handler",
		)
	}

	p, r := sig.Params(), sig.Results()
	if sig.Variadic() || p.Len() < 2 || p.Len() > 3 {
		return invalid()
	}

	ctx, ok := types.Unalias(p.At(0).Type()).(*types.Pointer)
	if !ok || !namedType(ctx.Elem(), ActorPath, "Context") {
		return invalid()
	}

	q := p.At(1).Type()
	if _, ok := q.Underlying().(*types.Interface); ok {
		return "", nil, fmt.Errorf("message must have a concrete type")
	}

	errType := types.Universe.Lookup("error").Type()
	base := types.Unalias(q)
	seen := map[types.Type]bool{}
	for {
		ptr, ok := base.Underlying().(*types.Pointer)
		if !ok || seen[base] {
			break
		}

		seen[base] = true
		base = types.Unalias(ptr.Elem())
	}

	for _, event := range []string{"Started", "Stopping", "Stopped"} {
		if namedType(base, ActorPath, event) {
			if !types.Identical(q, base) || p.Len() != 2 || r.Len() != 1 || !types.Identical(r.At(0).Type(), errType) {
				return "", nil, fmt.Errorf("lifecycle message handler requires (*actorx.Context, actorx.%s) error", event)
			}

			return "HandleMessage", q, nil
		}
	}

	if p.Len() == 3 {
		if r.Len() != 1 || !types.Identical(r.At(0).Type(), errType) {
			return invalid()
		}

		for _, kind := range []string{"Reply", "StreamReply"} {
			if namedType(p.At(2).Type(), ActorPath, kind) {
				if kind == "Reply" {
					return "HandleDeferred", q, nil
				}

				return "HandleStream", q, nil
			}
		}

		return invalid()
	}
	if r.Len() == 1 && types.Identical(r.At(0).Type(), errType) {
		return "HandleMessage", q, nil
	}
	if r.Len() == 2 && types.Identical(r.At(1).Type(), errType) {
		return "Handle", q, nil
	}

	return invalid()
}

// delayedHandler preserves value-receiver support without dereferencing nil a
// during Bindings. Imports use stable aliases independent of author file names.
func delayedHandler(sig *types.Signature, method string, pkg *types.Package, imports map[string]string, runtimeAlias, receiverName string) string {
	qualify := func(p *types.Package) string {
		if p.Path() == ActorPath {
			return runtimeAlias
		}
		if p == pkg {
			return ""
		}
		if alias, ok := imports[p.Path()]; ok {
			return alias
		}
		alias := generatedName(pkg, fmt.Sprintf("actorxType%d", len(imports)))
		for suffix := 0; alias == runtimeAlias || alias == receiverName || importAliasUsed(imports, alias); suffix++ {
			alias = generatedName(pkg, fmt.Sprintf("actorxType%d_%d", len(imports), suffix))
		}
		imports[p.Path()] = alias
		return alias
	}
	var params, args, results []string
	for i := 0; i < sig.Params().Len(); i++ {
		name := fmt.Sprintf("p%d", i)
		params = append(params, name+" "+types.TypeString(sig.Params().At(i).Type(), qualify))
		args = append(args, name)
	}
	for i := 0; i < sig.Results().Len(); i++ {
		results = append(results, types.TypeString(sig.Results().At(i).Type(), qualify))
	}
	return fmt.Sprintf("func(%s) (%s) { return %s.%s(%s) }", strings.Join(params, ", "), strings.Join(results, ", "), receiverName, method, strings.Join(args, ", "))
}

func generatedName(pkg *types.Package, base string) string {
	name := base
	for suffix := 0; pkg.Scope().Lookup(name) != nil; suffix++ {
		name = fmt.Sprintf("%s%d", base, suffix)
	}
	return name
}

func sourceNameUsed(files []*ast.File, name string) bool {
	for _, file := range files {
		if file.Scope.Lookup(name) != nil {
			return true
		}
	}
	return false
}

func importAliasUsed(imports map[string]string, alias string) bool {
	for _, used := range imports {
		if alias == used {
			return true
		}
	}
	return false
}

package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}

	files["go.mod"] = `module fixture

go 1.27

require github.com/RussellLuo/actorx v0.0.0
replace github.com/RussellLuo/actorx => ` + root + "\n"
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func TestCrossFileMethodsAndDeterministicCheck(t *testing.T) {
	dir := fixture(t, map[string]string{
		"a.go": `package fixture

import "github.com/RussellLuo/actorx"

type Q struct{}

type R struct{}

type Embedded struct{}

func (*Embedded) Promoted(*actorx.Context, Q) error { return nil }

//actorx:actor
type A struct{ Embedded }

func (A) Read(*actorx.Context, Q) (int, error) { return 1, nil }

func (*A) helper() {}

//actorx:ignore
func (*A) Helper() {}
`,
		"b.go": `package fixture

import "github.com/RussellLuo/actorx"

type HandlerContext = actorx.Context

func (*A) Reset(*HandlerContext, R) error { return nil }
`,
		// This file must not be included in the current build target.
		"excluded.go": "//go:build actorx_excluded\n\npackage fixture\nfunc invalid(\n",
	})

	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	first, err := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"return a.Read(p0, p1)", "actorx.HandleMessage(a.Reset)"} {
		if !strings.Contains(string(first), name) {
			t.Fatal(string(first))
		}
	}

	for _, name := range []string{"Promoted", "helper", "Helper"} {
		if strings.Contains(string(first), name) {
			t.Fatal("unexpected binding", name)
		}
	}

	// Checking and regenerating unchanged input must preserve the same bytes.
	if err := Run(dir, "actorx.gen.go", true); err != nil {
		t.Fatal(err)
	}
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	second, _ := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
	if string(first) != string(second) {
		t.Fatal("non-deterministic output")
	}

	// Adding a method must make check fail until bindings are regenerated.
	file, err := os.OpenFile(filepath.Join(dir, "b.go"), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	file.WriteString(`
type More struct{}

func (*A) More(*actorx.Context, More) error { return nil }
`)
	file.Close()
	if err := Run(dir, "actorx.gen.go", true); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatal(err)
	}
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestGeneratorRejectsInvalidAuthorDeclarations(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "signature",
			body: `//actorx:actor
type A struct{}

func (*A) Bad(int) {}`,
			want: "expected",
		},
		{
			name: "standard-context",
			body: `//actorx:actor
type A struct{}

func (*A) One(context.Context, Q) error { return nil }`,
			want: "*actorx.Context",
		},
		{
			name: "context-value",
			body: `//actorx:actor
type A struct{}

func (*A) One(actorx.Context, Q) error { return nil }`,
			want: "*actorx.Context",
		},
		{
			name: "unrelated-context",
			body: `type Context struct{}

//actorx:actor
type A struct{}

func (*A) One(*Context, Q) error { return nil }`,
			want: "*actorx.Context",
		},
		{
			name: "duplicate",
			body: `//actorx:actor
type A struct{}

func (*A) One(*actorx.Context, Q) error { return nil }

func (*A) Two(*actorx.Context, Alias) error { return nil }`,
			want: "duplicate",
		},
		{
			name: "interface",
			body: `//actorx:actor
type A struct{}

func (*A) One(*actorx.Context, any) error { return nil }`,
			want: "concrete",
		},
		{
			name: "mixed",
			body: `//actorx:actor
type A struct{}

//actorx:handler
func (*A) One(*actorx.Context, Q) error { return nil }`,
			want: "mix",
		},
		{
			name: "existing",
			body: `//actorx:actor
type A struct{}

func (*A) Bindings() {}`,
			want: "Bindings",
		},
		{
			name: "generic",
			body: `//actorx:actor
type A[T any] struct{}`,
			want: "non-generic",
		},
		{
			name: "ignore-handler",
			body: `type A struct{}

//actorx:handler
//actorx:ignore
func (*A) One(*actorx.Context, Q) error { return nil }`,
			want: "mix",
		},
		{
			name: "variadic",
			body: `//actorx:actor
type A struct{}

func (*A) One(*actorx.Context, ...Q) error { return nil }`,
			want: "expected",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := fixture(t, map[string]string{
				"a.go": `package fixture

import (
	"context"

	"github.com/RussellLuo/actorx"
)

var _ context.Context
var _ *actorx.Context

type Q struct{}

type Alias = Q

` + tc.body + "\n",
			})
			err := Run(dir, "actorx.gen.go", false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestExplicitAndDeferredStreamHandlers(t *testing.T) {
	dir := fixture(t, map[string]string{"a.go": `package fixture

import (
	"github.com/RussellLuo/actorx"
)

type Q struct{}

type S struct{}

type A struct{}

//actorx:handler
func (*A) Deferred(*actorx.Context, Q, actorx.Reply[int]) error { return nil }

//actorx:handler
func (*A) Stream(*actorx.Context, S, actorx.StreamReply[string]) error { return nil }

func (*A) PublicHelper() {}
`})
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
	for _, want := range []string{"HandleDeferred(a.Deferred)", "HandleStream(a.Stream)"} {
		if !strings.Contains(string(data), want) {
			t.Fatal(string(data))
		}
	}

	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestGeneratorPreservesHandwrittenOutput(t *testing.T) {
	dir := fixture(t, map[string]string{
		"a.go": `package fixture

import "github.com/RussellLuo/actorx"

type Q struct{}

//actorx:actor
type A struct{}

func (*A) One(*actorx.Context, Q) error { return nil }
`,
		"manual.go": "package fixture\n// User-owned source.\n",
	})

	err := Run(dir, "manual.go", false)
	if err == nil || !strings.Contains(err.Error(), "non-generated") {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "manual.go"))
	if string(data) != "package fixture\n// User-owned source.\n" {
		t.Fatal("handwritten file changed")
	}
}

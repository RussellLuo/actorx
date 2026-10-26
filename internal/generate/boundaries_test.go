package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func buildFixture(t *testing.T, dir string, env ...string) {
	t.Helper()

	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fixture: %v\n%s", err, out)
	}
}

func TestActorInterfaceAssertionBeforeAndAfterGeneration(t *testing.T) {
	dir := fixture(t, map[string]string{"a.go": `package fixture

import (
	"github.com/go-aie/actorx"
)

type Q struct{}

//actorx:actor
type A struct{}

func (*A) One(actorx.Context, Q) error { return nil }

var _ actorx.Actor = (*A)(nil)
`})
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	buildFixture(t, dir)
	if err := Run(dir, "actorx.gen.go", true); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "a.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	source = append(source, []byte("\nvar _ interface { Missing() } = (*A)(nil)\n")...)
	if err := os.WriteFile(path, source, 0644); err != nil {
		t.Fatal(err)
	}
	if err := Run(dir, "actorx.gen.go", true); err == nil || !strings.Contains(err.Error(), "Missing") {
		t.Fatal("invalid interface assertion was ignored", err)
	}
}

func TestGeneratedBuildConstraints(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "modern", true: "legacy"}[legacy], func(t *testing.T) {
			line := "//go:build actorx_feature"
			if legacy {
				line = "// +build actorx_feature"
			}

			dir := fixture(t, map[string]string{
				"base.go": "package fixture\n",
				"feature.go": line + `

package fixture

import "github.com/go-aie/actorx"

type Q struct{}

//actorx:actor
type A struct{}

func (*A) One(actorx.Context, Q) error { return nil }
`,
			})
			t.Setenv("GOFLAGS", "-tags=actorx_feature")
			if err := Run(dir, "actorx.gen.go", false); err != nil {
				t.Fatal(err)
			}
			if err := Run(dir, "actorx.gen.go", true); err != nil {
				t.Fatal(err)
			}

			buildFixture(t, dir)
			buildFixture(t, dir, "GOFLAGS=")

			// Change output content and regenerate: a build header must not be
			// mistaken for a handwritten file by the overwrite guard.
			path := filepath.Join(dir, "feature.go")
			source, _ := os.ReadFile(path)
			source = append(source, []byte(`
type R struct{}

func (*A) Two(actorx.Context, R) error { return nil }
`)...)
			if err := os.WriteFile(path, source, 0644); err != nil {
				t.Fatal(err)
			}
			if err := Run(dir, "actorx.gen.go", false); err != nil {
				t.Fatal(err)
			}

			buildFixture(t, dir)
			buildFixture(t, dir, "GOFLAGS=")
		})
	}
}

func TestGeneratedImplicitPlatformConstraint(t *testing.T) {
	dir := fixture(t, map[string]string{
		"base.go": "package fixture\n",
		"actor_" + runtime.GOOS + ".go": `package fixture

import "github.com/go-aie/actorx"

type Q struct{}

//actorx:actor
type A struct{}

func (*A) One(actorx.Context, Q) error { return nil }
`,
	})

	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	buildFixture(t, dir)
	otherOS := "linux"
	if runtime.GOOS == otherOS {
		otherOS = "darwin"
	}

	buildFixture(t, dir, "GOOS="+otherOS, "GOARCH=amd64", "CGO_ENABLED=0")
}

func TestMixedBuildConstraintsAreRejectedWithoutWriting(t *testing.T) {
	dir := fixture(t, map[string]string{
		"a.go": `package fixture

import "github.com/go-aie/actorx"

type Q struct{}

//actorx:actor
type A struct{}

func (*A) One(actorx.Context, Q) error { return nil }
`,
		"feature.go": `//go:build actorx_feature

package fixture

import "github.com/go-aie/actorx"

type R struct{}

func (*A) Two(actorx.Context, R) error { return nil }
`,
	})

	t.Setenv("GOFLAGS", "")
	if err := Run(dir, "actorx.gen.go", false); err != nil {
		t.Fatal(err)
	}

	before, _ := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
	t.Setenv("GOFLAGS", "-tags=actorx_feature")
	if err := Run(dir, "actorx.gen.go", false); err == nil ||
		!strings.Contains(err.Error(), "mixed build constraints") {
		t.Fatal(err)
	}

	after, _ := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
	if string(before) != string(after) {
		t.Fatal("rejected generation changed existing output")
	}
	if err := Run(dir, "actorx_linux.go", false); err == nil {
		t.Fatal("mixed constraints accepted with a platform output suffix")
	}
}

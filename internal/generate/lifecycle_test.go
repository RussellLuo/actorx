package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Deliberately use custom method names to verify classification by event type.
func TestLifecycleGenerationByEventType(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			actorMarker, methodMarker := "//actorx:actor", ""
			if explicit {
				actorMarker, methodMarker = "", "//actorx:handler"
			}

			dir := fixture(t, map[string]string{"a.go": fmt.Sprintf(`package fixture
import ax "github.com/go-aie/actorx"
type Begin = ax.Started
type Stop struct{}
%s
type A struct{}
%s
func (*A) Initialize(ax.Context, Begin) error { return nil }
%s
func (*A) Unblock(ax.Context, ax.Stopping) error { return nil }
%s
func (*A) Finalize(ax.Context, ax.Stopped) error { return nil }
%s
func (*A) Stop(ax.Context, Stop) error { return nil }
var _ ax.Actor = (*A)(nil)
`, actorMarker, methodMarker, methodMarker, methodMarker, methodMarker)})
			if err := Run(dir, "actorx.gen.go", false); err != nil {
				t.Fatal(err)
			}
			if err := Run(dir, "actorx.gen.go", true); err != nil {
				t.Fatal(err)
			}

			data, _ := os.ReadFile(filepath.Join(dir, "actorx.gen.go"))
			for _, name := range []string{"Initialize", "Unblock", "Finalize", "Stop"} {
				if !strings.Contains(string(data), "actorx.HandleMessage(a."+name+")") {
					t.Fatal(string(data))
				}
			}

			buildFixture(t, dir)
		})
	}
}

func TestLifecycleGeneratorRejectsUnsupportedBindings(t *testing.T) {
	for _, event := range []string{"Started", "Stopping", "Stopped"} {
		for _, signature := range []string{
			"(actorx.Context, *actorx.%s) error",
			"(actorx.Context, actorx.%s) (int, error)",
			"(actorx.Context, actorx.%s, actorx.Reply[int]) error",
			"(actorx.Context, actorx.%s, actorx.StreamReply[int]) error",
		} {
			t.Run(event+signature, func(t *testing.T) {
				sig := fmt.Sprintf(signature, event)
				body := "return nil"
				if strings.Contains(sig, "(int, error)") {
					body = "return 0, nil"
				}

				dir := fixture(t, map[string]string{"a.go": `package fixture
import "github.com/go-aie/actorx"
//actorx:actor
type A struct{}
func (*A) ` + event + sig + " { " + body + " }\n"})
				if err := Run(dir, "actorx.gen.go", false); err == nil || !strings.Contains(err.Error(), "lifecycle message handler requires") {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(dir, "actorx.gen.go")); !os.IsNotExist(err) {
					t.Fatal("invalid output published", err)
				}
			})
		}
	}

	for _, declaration := range []string{
		"type Alias = actorx.Started\nfunc (*A) One(actorx.Context, actorx.Started) error { return nil }\nfunc (*A) Two(actorx.Context, Alias) error { return nil }",
		"type Pointer *actorx.Started\nfunc (*A) One(actorx.Context, Pointer) error { return nil }",
	} {
		dir := fixture(t, map[string]string{"a.go": "package fixture\nimport \"github.com/go-aie/actorx\"\n//actorx:actor\ntype A struct{}\n" + declaration})
		if err := Run(dir, "actorx.gen.go", false); err == nil {
			t.Fatal("accepted invalid lifecycle declaration")
		}
	}
}

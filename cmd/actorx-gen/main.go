// actorx-gen generates typed actor bindings in the current Go package.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/RussellLuo/actorx/internal/generate"
)

func main() {
	output := flag.String("output", "actorx.gen.go", "generated file in the current package")
	check := flag.Bool("check", false, "fail if generated output is missing or stale")
	flag.Parse()

	if flag.NArg() != 0 ||
		filepath.Base(*output) != *output ||
		!strings.HasSuffix(*output, ".go") ||
		strings.HasSuffix(*output, "_test.go") {
		fmt.Fprintln(os.Stderr, "actorx-gen: output must be a Go source filename; run in the target package")
		os.Exit(1)
	}

	if err := generate.Run(".", *output, *check); err != nil {
		fmt.Fprintln(os.Stderr, "actorx-gen:", err)
		os.Exit(1)
	}
}

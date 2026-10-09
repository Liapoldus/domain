// Command contracts exports public Domain artifacts from code-owned definitions.
package main

import (
	"flag"
	"os"
	"path/filepath"
	"sort"

	"github.com/Liapoldus/domain/contracts"
)

func main() {
	output := flag.String("out", "contracts", "output directory")
	flag.Parse()
	if err := generate(*output); err != nil {
		panic(err)
	}
}

func generate(output string) error {
	artifacts, err := contracts.Artifacts()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(artifacts))
	for name := range artifacts {
		names = append(names, name)
	}
	sort.Strings(names)
	root, err := os.OpenRoot(output)
	if err != nil {
		return err
	}
	defer func() {
		if err := root.Close(); err != nil {
			panic(err)
		}
	}()
	for _, name := range names {
		if err := root.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			return err
		}
		if err := root.WriteFile(name, artifacts[name], 0o644); err != nil {
			return err
		}
	}
	return nil
}

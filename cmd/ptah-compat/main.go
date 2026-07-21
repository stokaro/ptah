package main

import (
	"os"
	"path/filepath"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/root"
)

func main() {
	root.ExecuteCommand(atlas.NewCompatCommand(filepath.Base(os.Args[0])))
}

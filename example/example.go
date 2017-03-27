package main

import (
	"os"

	examples "github.com/fiatjaf/levelup/examples"
	"github.com/fiatjaf/rocksdown"
)

func main() {
	db := rocksdown.NewDatabase("/tmp/rocksdownexample")
	defer os.RemoveAll("/tmp/rocksdownexample")

	examples.Example(db)
}

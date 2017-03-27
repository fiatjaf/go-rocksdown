package main

import (
	"os"

	"github.com/fiatjaf/levelup"
	"github.com/fiatjaf/rocksdown"
)

func main() {
	db := rocksdown.NewDatabase("/tmp/rocksdownexample")
	defer os.RemoveAll("/tmp/rocksdownexample")

	levelup.Example(db)
}

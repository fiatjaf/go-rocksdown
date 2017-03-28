package main

import (
	examples "github.com/fiatjaf/levelup/examples"
	"github.com/fiatjaf/rocksdown"
)

func main() {
	db := rocksdown.NewDatabase("/tmp/rocksdownexample")
	defer db.Erase()

	examples.Example(db)
}

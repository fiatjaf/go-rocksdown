package rocksdown

import (
	"os"
	"testing"

	"github.com/fiatjaf/levelup"
)

func TestAll(t *testing.T) {
	db := NewDatabase("/tmp/rocksdowntest")
	defer os.RemoveAll("/tmp/rocksdowntest")

	levelup.BasicTests(db, t)
}

package rocksdown

import (
	"os"
	"testing"

	"github.com/fiatjaf/go-levelup"
)

func TestAll(t *testing.T) {
	db := NewDatabase("/tmp/rocksdowntest")
	defer os.Remove("/tmp/rocksdowntest")

	levelup.BasicTests(db, t)
}

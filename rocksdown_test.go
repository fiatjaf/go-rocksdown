package rocksdown

import (
	"os"
	"testing"

	tests "github.com/fiatjaf/levelup/tests"
)

func TestAll(t *testing.T) {
	db := NewDatabase("/tmp/rocksdowntest")
	defer os.RemoveAll("/tmp/rocksdowntest")

	tests.Test(db, t)
}

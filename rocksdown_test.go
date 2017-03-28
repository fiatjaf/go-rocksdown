package rocksdown

import (
	"testing"

	tests "github.com/fiatjaf/levelup/tests"
)

func TestAll(t *testing.T) {
	db := NewDatabase("/tmp/rocksdowntest")
	defer db.Erase()

	tests.Test(db, t)
}

package user

import "testing"

func TestCount(t *testing.T) {
	if people().MustCount() != 1 {
		t.Fatal("count")
	}
	if n, _ := people().Count(t.Context()); n != 1 {
		t.Fatal("count")
	}
}

package user

import (
	"github.com/shpandrak/shpanstream/stream"
	"testing"
)

func TestCount(t *testing.T) {
	if stream.MustCount(people()) != 1 {
		t.Fatal("count")
	}
	if n, _ := stream.Count(t.Context(), people()); n != 1 {
		t.Fatal("count")
	}
}

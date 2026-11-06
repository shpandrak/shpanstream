//go:build ignore

package main

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

func main() { stream.FromSlice([]int{1}).Count(context.Background()) }

package cgo

import "C"

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

func N(ctx context.Context, s stream.Stream[int]) (int, error) { return s.Count(ctx) }

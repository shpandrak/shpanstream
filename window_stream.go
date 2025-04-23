package shpanstream

import (
	"context"
	"fmt"
	"io"
	"time"
)

type WindowOption func(*windowConfig)

type windowConfig struct {
	step            int
	timeout         time.Duration
	omitLastPartial bool
}

// WithSlidingWindowStepOption sets the step size for the window. allowing for overlapping windows.
func WithSlidingWindowStepOption(n int) WindowOption {
	return func(cfg *windowConfig) {
		cfg.step = n
	}
}

// todo:support this :)
//func WithWindowTimeoutOption(d time.Duration) WindowOption {
//	return func(cfg *windowConfig) {
//		cfg.timeout = d
//	}
//}

// WithOmitLastPartialWindowOption causes the stream to avoid emitting last window even if it is not full.
func WithOmitLastPartialWindowOption() WindowOption {
	return func(cfg *windowConfig) {
		cfg.omitLastPartial = true
	}
}

func Window[T any](s Stream[T], size int, opts ...WindowOption) Stream[[]T] {
	if size <= 0 {
		return ErrorStream[[]T](fmt.Errorf("window size must be greater than 0"))
	}

	cfg := windowConfig{
		step: size, // default: no overlap
	}

	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.step <= 0 {
		return ErrorStream[[]T](fmt.Errorf("step must be greater than 0, or omit the option for no overlap"))
	}
	if cfg.step > size {
		return ErrorStream[[]T](fmt.Errorf("step must be less than or equal to size"))
	}
	var buffer []T
	done := false

	return NewDownStream(
		s,
		func(ctx context.Context, srcProviderFunc StreamProviderFunc[T]) ([]T, error) {

			// check if previous iteration pulled EOF from the source
			if done {
				return nil, io.EOF
			}

			// Fill buffer until we have enough for a window
			for len(buffer) < size {
				// first check if the context is done
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}

				v, err := srcProviderFunc(ctx)
				if err != nil {
					// partial buffer, if EOF returning it
					if err == io.EOF && len(buffer) > 0 && !cfg.omitLastPartial && cfg.step != 1 {
						window := make([]T, len(buffer))
						copy(window, buffer)

						buffer = nil
						// Make sure that next iteration will return EOF
						done = true
						return window, nil
					}
					return nil, err
				}
				buffer = append(buffer, v)
			}

			window := make([]T, size)
			copy(window, buffer[:size])

			// Shift or drop elements for next window
			if cfg.step >= len(buffer) {
				// drop all and refill
				buffer = nil
			} else {
				// slide the buffer forward
				buffer = buffer[cfg.step:]
			}

			return window, nil
		},
		nil,
		nil,
	)
}

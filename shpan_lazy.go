package shpanstream

import (
	"context"
	"encoding/json"
	"io"
)

// Lazy - a generic Lazy type
type Lazy[T any] struct {
	fetcher func(ctx context.Context) (*T, error)
}

type lazyStreamProvider[T any] struct {
	fetcher func(ctx context.Context) (*T, error)
	fetched bool
}

func (lp *lazyStreamProvider[T]) Open(_ context.Context) error {
	lp.fetched = false
	return nil
}

func (lp *lazyStreamProvider[T]) Close() {

}

func (lp *lazyStreamProvider[T]) Emit(ctx context.Context) (T, error) {
	if lp.fetched {
		return defaultValue[T](), io.EOF
	}
	lp.fetched = true
	v, err := lp.fetcher(ctx)
	if err != nil {
		return defaultValue[T](), err
	}
	if v == nil {
		return defaultValue[T](), io.EOF
	}
	return *v, nil
}

// NewLazy creates a new Lazy with value
func NewLazy[T any](fetcher func(ctx context.Context) (*T, error)) Lazy[T] {
	return Lazy[T]{fetcher: fetcher}
}

// NewCompletedLazy creates a new Lazy with value and err
func NewCompletedLazy[T any](v *T, err error) Lazy[T] {
	return Lazy[T]{fetcher: func(ctx context.Context) (*T, error) {
		return v, err
	}}
}

// NewLazyNonNil creates a new Lazy with value, does not allow nil values
func NewLazyNonNil[T any](fetcher func(ctx context.Context) (T, error)) Lazy[T] {
	return Lazy[T]{fetcher: func(ctx context.Context) (*T, error) {
		v, err := fetcher(ctx)
		if err != nil {
			return nil, err
		}
		return &v, nil
	}}
}

// EmptyLazy gets an empty Lazy
func EmptyLazy[T any]() Lazy[T] {
	return NewLazyFromPtr[T](nil)
}

// NewLazyFromPtr creates an Lazy from Ptr (or nil)
func NewLazyFromPtr[T any](v *T) Lazy[T] {
	return Lazy[T]{fetcher: func(_ context.Context) (*T, error) {
		return v, nil
	},
	}
}

func (o Lazy[T]) AsStream() Stream[T] {
	return NewStream[T](&lazyStreamProvider[T]{
		fetcher: o.fetcher,
	})
}

// Get returns the int value or an error if not present.
func (o Lazy[T]) Get(ctx context.Context) (*T, error) {
	return o.fetcher(ctx)
}

// OrElse returns the int value or a default value if the value is not present.
func (o Lazy[T]) OrElse(ctx context.Context, v T) (T, error) {
	d, err := o.fetcher(ctx)
	if err != nil {
		return defaultValue[T](), err
	}
	if d == nil {
		return v, nil
	}
	return *d, nil
}

func (o Lazy[T]) Filter(predicate func(*T) bool) Lazy[T] {
	return NewLazy[T](func(_ context.Context) (*T, error) {
		v, err := o.fetcher(context.Background())
		if err != nil {
			return nil, err
		}
		if v == nil || !predicate(v) {
			return nil, nil
		}
		return v, nil
	})
}

func MapLazy[SRC any, TGT any](src Lazy[SRC], mapper func(SRC) TGT) Lazy[TGT] {
	return NewLazy[TGT](func(ctx context.Context) (*TGT, error) {
		srcValue, err := src.Get(ctx)
		if err != nil {
			return nil, err
		}
		if srcValue != nil {
			tgt := mapper(*srcValue)
			return &tgt, nil
		} else {
			return nil, nil
		}
	})
}

func MapLazyWithError[SRC any, TGT any](src Lazy[SRC], mapper func(SRC) (TGT, error)) Lazy[TGT] {
	return NewLazy[TGT](func(ctx context.Context) (*TGT, error) {
		srcValue, err := src.Get(ctx)
		if err != nil {
			return nil, err
		}
		if srcValue != nil {
			tgt, err := mapper(*srcValue)
			if err != nil {
				return nil, err
			}
			return &tgt, nil
		} else {
			return nil, nil
		}
	})
}

// OrElseGet returns the int value or a default value if the value is not present.
func (o Lazy[T]) OrElseGet(ctx context.Context, alt func() T) (T, error) {
	ret, err := o.Get(ctx)
	if err != nil {
		return defaultValue[T](), err
	}
	if ret != nil {
		return *ret, nil
	}
	return alt(), nil
}

func (o Lazy[T]) Or(alt Lazy[T]) Lazy[T] {
	return NewLazy(func(ctx context.Context) (*T, error) {
		orig, err := o.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if orig != nil {
			return orig, nil
		}
		return alt.fetcher(ctx)
	})
}

func (o Lazy[T]) MarshalJSON() ([]byte, error) {
	data, err := o.fetcher(context.Background())
	if err != nil {
		return nil, err
	}
	return json.Marshal(data)
}

func (o Lazy[T]) UnmarshalJSON(data []byte) error {

	if string(data) == "null" {
		o.fetcher = func(_ context.Context) (*T, error) {
			return nil, nil
		}
		return nil
	}

	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	o.fetcher = func(_ context.Context) (*T, error) {
		return &value, nil
	}
	return nil
}

func (o Lazy[T]) IsEmpty(ctx context.Context) (bool, error) {
	v, err := o.Get(ctx)
	if err != nil {
		return false, err
	}
	return v == nil, nil
}

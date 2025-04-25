package shpanstream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

// Lazy - a generic Lazy type allowing deferred computation of a value
// Lazy supports optional values, caller can decide whether to require a value or use the Optional methods
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
		return util.DefaultValue[T](), io.EOF
	}
	lp.fetched = true

	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	v, err := lp.fetcher(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if v == nil {
		return util.DefaultValue[T](), io.EOF
	}
	return *v, nil
}

// NewLazyOptional creates a new Lazy, allow optional value
func NewLazyOptional[T any](fetcher func(ctx context.Context) (*T, error)) Lazy[T] {
	return Lazy[T]{fetcher: fetcher}
}

// JustLazy creates a new Lazy with value
func JustLazy[T any](v T) Lazy[T] {
	return Lazy[T]{fetcher: func(ctx context.Context) (*T, error) {
		return &v, nil
	}}
}

// JustLazyOptional creates a new Lazy with value and err
func JustLazyOptional[T any](v *T) Lazy[T] {
	return Lazy[T]{fetcher: func(ctx context.Context) (*T, error) {
		return v, nil
	}}
}

// NewLazy creates a new Lazy with value, does not allow nil values (not optional
func NewLazy[T any](fetcher func(ctx context.Context) (T, error)) Lazy[T] {
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
	return NewLazyOptional[T](nil)
}

// ErrorLazy creates a new Lazy with error
func ErrorLazy[T any](err error) Lazy[T] {
	return Lazy[T]{fetcher: func(_ context.Context) (*T, error) {
		return nil, err
	}}
}

// AsStream converts the Lazy to a Stream (or either a single element, empty stream, or an error stream)
func (o Lazy[T]) AsStream() Stream[T] {
	return NewStream[T](&lazyStreamProvider[T]{
		fetcher: o.fetcher,
	})
}

// Get returns the int value or an error. it will return an error if the value is not present. see GetOptional
// to get an optional value
func (o Lazy[T]) Get(ctx context.Context) (T, error) {
	v, err := o.fetcher(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if v == nil {
		return util.DefaultValue[T](), fmt.Errorf("lazy value is empty")
	}
	return *v, err
}

// GetOptional returns the value or an error. it will return nil if lazy value is empty
func (o Lazy[T]) GetOptional(ctx context.Context) (*T, error) {
	return o.fetcher(ctx)
}

// MustGetOptional returns the value or an error. it will return nil if lazy value is empty
// it will panic in case of error, use for testing or when the value is static
func (o Lazy[T]) MustGetOptional() *T {
	optional, err := o.GetOptional(context.Background())
	if err != nil {
		panic(err)
	}
	return optional
}

// OrElse returns the int value or a default value if the value is not present.
func (o Lazy[T]) OrElse(ctx context.Context, v T) (T, error) {
	d, err := o.fetcher(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if d == nil {
		return v, nil
	}
	return *d, nil
}

// MustOrElse returns the int value or a default value if the value is not present.
// it will panic in case of error, use for testing or when the value is static
func (o Lazy[T]) MustOrElse(v T) T {
	d, err := o.fetcher(context.Background())
	if err != nil {
		panic(err)
	}
	if d == nil {
		return v
	}
	return *d
}

// Filter filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) Filter(predicate Predicate[T]) Lazy[T] {
	return o.FilterWithErrAndCtx(predicateToErrCtx(predicate))
}

// FilterWithErr filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) FilterWithErr(predicate PredicateWithErr[T]) Lazy[T] {
	return o.FilterWithErrAndCtx(predicateErrToErrCtx(predicate))
}

// FilterWithErrAndCtx filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) FilterWithErrAndCtx(predicate PredicateWithErrAndCtx[T]) Lazy[T] {
	return NewLazyOptional[T](func(ctx context.Context) (*T, error) {
		v, err := o.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		matchesFilter, err := predicate(ctx, *v)
		if err != nil {
			return nil, err
		}
		if v == nil || !matchesFilter {
			return nil, nil
		}
		return v, nil
	})
}

// MapLazy maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func MapLazy[SRC any, TGT any](src Lazy[SRC], mapper Mapper[SRC, TGT]) Lazy[TGT] {
	return MapLazyWithErrAndCtx(src, mapperToErrCtx(mapper))
}

// MapLazyWithErr maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func MapLazyWithErr[SRC any, TGT any](src Lazy[SRC], mapper MapperWithErr[SRC, TGT]) Lazy[TGT] {
	return MapLazyWithErrAndCtx(src, mapperErrToErrCtx(mapper))
}

// MapLazyWithErrAndCtx maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func MapLazyWithErrAndCtx[SRC any, TGT any](src Lazy[SRC], mapper MapperWithErrAndCtx[SRC, TGT]) Lazy[TGT] {
	return NewLazyOptional[TGT](func(ctx context.Context) (*TGT, error) {
		srcValue, err := src.GetOptional(ctx)
		if err != nil {
			return nil, err
		}
		if srcValue != nil {
			tgt, err := mapper(ctx, *srcValue)
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
	ret, err := o.GetOptional(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if ret != nil {
		return *ret, nil
	}
	return alt(), nil
}

func (o Lazy[T]) Or(alt Lazy[T]) Lazy[T] {
	return NewLazyOptional(func(ctx context.Context) (*T, error) {
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
	v, err := o.GetOptional(ctx)
	if err != nil {
		return false, err
	}
	return v == nil, nil
}

// MustGet returns the underlying lazy value or panics if the value is not present. can be used for shorter tests or when
// the caller is sure that the value is present
func (o Lazy[T]) MustGet() T {
	v, err := o.Get(context.Background())
	if err != nil {
		panic(err)
	}
	return v
}

// MustIsEmpty returns whether the lazy value is empty or not. panics if value retrieval fails, use for testing or when
// the caller is sure that the value retrieval will not fail
func (o Lazy[T]) MustIsEmpty() bool {
	isEmpty, err := o.IsEmpty(context.Background())
	if err != nil {
		panic(err)
	}
	return isEmpty
}

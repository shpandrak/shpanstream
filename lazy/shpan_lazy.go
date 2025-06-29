package lazy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
)

var defaultEmptyValueErr = func() error {
	return fmt.Errorf("lazy value is empty")
}

// Lazy - a generic Lazy type allowing deferred computation of a value
// Lazy supports optional values, caller can decide whether to require a value or use the Optional methods
type Lazy[T any] struct {
	fetcher               func(ctx context.Context) (*T, error)
	emptyValueErrSupplier func() error
}

// NewLazyOptional creates a new Lazy, allow optional value
func NewLazyOptional[T any](fetcher func(ctx context.Context) (*T, error)) Lazy[T] {
	return newLazy[T](fetcher, defaultEmptyValueErr)
}

// NewLazyOptionalOrElseThrow creates a new Lazy, allow optional value, allows supplying empty value error
func NewLazyOptionalOrElseThrow[T any](
	fetcher func(ctx context.Context) (*T, error),
	emptyValueErrSup func() error,
) Lazy[T] {
	return newLazy[T](fetcher, emptyValueErrSup)
}

// Just creates a new Lazy with value
func Just[T any](v T) Lazy[T] {
	return newLazy(func(ctx context.Context) (*T, error) {
		return &v, nil
	}, defaultEmptyValueErr)
}

// JustOptional creates a new Lazy with optional value
func JustOptional[T any](v *T) Lazy[T] {
	return newLazy[T](func(ctx context.Context) (*T, error) {
		return v, nil
	}, defaultEmptyValueErr)
}

// JustOptionalOrElseThrow creates a new Lazy with optional value and alternative empty value error
func JustOptionalOrElseThrow[T any](v *T, errSup func() error) Lazy[T] {
	return newLazy[T](func(ctx context.Context) (*T, error) {
		return v, nil
	}, errSup)
}

// NewLazy creates a new Lazy with value, does not allow nil values (not optional
func NewLazy[T any](fetcher func(ctx context.Context) (T, error)) Lazy[T] {
	return newLazy(
		func(ctx context.Context) (*T, error) {
			v, err := fetcher(ctx)
			if err != nil {
				return nil, err
			}
			return &v, nil
		},
		defaultEmptyValueErr,
	)
}

func newLazy[T any](fetcher func(ctx context.Context) (*T, error), emptyValueErrSup func() error) Lazy[T] {
	return Lazy[T]{
		emptyValueErrSupplier: emptyValueErrSup,
		fetcher:               fetcher,
	}
}

// Empty gets an empty Lazy
func Empty[T any]() Lazy[T] {
	return newLazy[T](func(ctx context.Context) (*T, error) {
		return nil, nil
	}, defaultEmptyValueErr)
}

// Error creates a new Lazy with error
func Error[T any](err error) Lazy[T] {
	return newLazy(func(_ context.Context) (*T, error) {
		return nil, err
	}, defaultEmptyValueErr)
}

// Get returns the int value or an error. it will return an error if the value is not present. see GetOptional
// to get an optional value
func (o Lazy[T]) Get(ctx context.Context) (T, error) {
	v, err := o.fetcher(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if v == nil {
		return util.DefaultValue[T](), o.emptyValueErrSupplier()
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

// OrElseThrow returns lazy that will emit a specified error if the value is not present
func (o Lazy[T]) OrElseThrow(errSup func() error) Lazy[T] {
	return newLazy[T](o.fetcher, errSup)
}

// Filter filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) Filter(predicate shpanstream.Predicate[T]) Lazy[T] {
	return o.FilterWithErrAndCtx(predicate.ToErrCtx())
}

// FilterWithErr filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) FilterWithErr(predicate shpanstream.PredicateWithErr[T]) Lazy[T] {
	return o.FilterWithErrAndCtx(predicate.ToErrCtx())
}

// FilterWithErrAndCtx filters the value of the Lazy using the provided predicate function.
func (o Lazy[T]) FilterWithErrAndCtx(predicate shpanstream.PredicateWithErrAndCtx[T]) Lazy[T] {
	return newLazy(func(ctx context.Context) (*T, error) {
		v, err := o.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if v == nil {
			// If the value is nil, no need to check the predicate
			return nil, nil
		}
		matchesFilter, err := predicate(ctx, *v)
		if err != nil {
			return nil, err
		}
		if !matchesFilter {
			return nil, nil
		}
		return v, nil
	}, o.emptyValueErrSupplier)
}

// Map maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func Map[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.Mapper[SRC, TGT]) Lazy[TGT] {
	return MapWithErrAndCtx(src, mapper.ToErrCtx())
}

// MapWithErr maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func MapWithErr[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErr[SRC, TGT]) Lazy[TGT] {
	return MapWithErrAndCtx(src, mapper.ToErrCtx())
}

// MapWithErrAndCtx maps the value of the Lazy to a new value using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func MapWithErrAndCtx[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErrAndCtx[SRC, TGT]) Lazy[TGT] {
	return newLazy[TGT](func(ctx context.Context) (*TGT, error) {
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
	}, src.emptyValueErrSupplier)
}

func MapWhileFiltering[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.Mapper[SRC, *TGT]) Lazy[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx())
}
func MapWhileFilteringWithErr[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErr[SRC, *TGT]) Lazy[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx())
}
func MapWhileFilteringWithErrAndCtx[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErrAndCtx[SRC, *TGT]) Lazy[TGT] {
	return newLazy[TGT](func(ctx context.Context) (*TGT, error) {
		srcValue, err := src.GetOptional(ctx)
		if err != nil {
			return nil, err
		}
		if srcValue != nil {
			return mapper(ctx, *srcValue)
		} else {
			return nil, nil
		}
	}, src.emptyValueErrSupplier)
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

// MustOrElseGet returns the int value or a default value if the value is not present.
// it will panic in case of error, use for testing or when the value is static
func (o Lazy[T]) MustOrElseGet(alt func() T) T {
	ret := o.MustGetOptional()
	if ret != nil {
		return *ret
	}
	return alt()
}

func (o Lazy[T]) Or(alt Lazy[T]) Lazy[T] {
	return newLazy(func(ctx context.Context) (*T, error) {
		orig, err := o.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if orig != nil {
			return orig, nil
		}
		return alt.fetcher(ctx)
	}, alt.emptyValueErrSupplier)
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

// FlatMap maps the value of the Lazy to a new Lazy using the provided mapper function.
// If lazy is empty, it will return an empty Lazy.
func FlatMap[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.Mapper[SRC, Lazy[TGT]]) Lazy[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, func(ctx context.Context, src SRC) (*TGT, error) {
		return mapper(src).GetOptional(ctx)
	})
}

// Consume consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
func (o Lazy[T]) Consume(ctx context.Context, consumer func(T)) error {
	return o.ConsumeWithErrAndCtx(ctx, func(_ context.Context, value T) error {
		consumer(value)
		return nil
	})
}

// MustConsume consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
func (o Lazy[T]) MustConsume(consumer func(T)) {
	err := o.Consume(context.Background(), consumer)
	if err != nil {
		panic(err)
	}
}

// ConsumeWithErr consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
func (o Lazy[T]) ConsumeWithErr(ctx context.Context, f func(value T) error) error {
	return o.ConsumeWithErrAndCtx(ctx, func(_ context.Context, value T) error {
		return f(value)
	})
}

// ConsumeWithErrAndCtx consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
func (o Lazy[T]) ConsumeWithErrAndCtx(ctx context.Context, f func(ctx context.Context, value T) error) error {
	v, err := o.fetcher(ctx)
	if err != nil {
		return err
	}
	if v != nil {
		return f(ctx, *v)
	}
	return nil
}

package lazy

import (
	"context"
	"encoding/json"

	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
)

// Lazy - a generic Lazy type allowing deferred computation of a value
// Lazy supports optional values, caller can decide whether to require a value or use the Optional methods
//
// Lazy[T] is a thin shell over the non-generic lazyCore (lazy_core.go): the methods here only box
// the fetcher and the callbacks they are given and unbox the *T they get back. The box* adapters
// are marked noinline so that each adapter closure is compiled once per element type rather than
// once per call site (and once more in every compiler-generated pointer-receiver wrapper).
type Lazy[T any] struct {
	core lazyCore
}

// unbox returns the *T a core value holds; a nil interface (absent value) unboxes to a nil *T.
func unbox[T any](v any) *T {
	p, _ := v.(*T)
	return p
}

// boxPtr boxes a *T, keeping an absent value a nil interface rather than a typed nil.
func boxPtr[T any](p *T) any {
	if p == nil {
		return nil
	}
	return p
}

//go:noinline
func boxFetcher[T any](fetcher func(ctx context.Context) (*T, error)) anyFetcher {
	return func(ctx context.Context) (any, error) {
		p, err := fetcher(ctx)
		return boxPtr(p), err
	}
}

//go:noinline
func boxValueFetcher[T any](fetcher func(ctx context.Context) (T, error)) anyFetcher {
	return func(ctx context.Context) (any, error) {
		v, err := fetcher(ctx)
		if err != nil {
			return nil, err
		}
		return &v, nil
	}
}

//go:noinline
func boxPredicate[T any](predicate shpanstream.PredicateWithErrAndCtx[T]) func(ctx context.Context, v any) (bool, error) {
	return func(ctx context.Context, v any) (bool, error) {
		return predicate(ctx, *unbox[T](v))
	}
}

//go:noinline
func boxConsumer[T any](f func(ctx context.Context, value T) error) func(ctx context.Context, v any) error {
	return func(ctx context.Context, v any) error {
		return f(ctx, *unbox[T](v))
	}
}

// NewLazyOptional creates a new Lazy, allow optional value
func NewLazyOptional[T any](fetcher func(ctx context.Context) (*T, error)) Lazy[T] {
	return Lazy[T]{core: newCore(boxFetcher(fetcher), defaultEmptyValueErr)}
}

// NewLazyOptionalOrElseThrow creates a new Lazy, allow optional value, allows supplying empty value error
func NewLazyOptionalOrElseThrow[T any](
	fetcher func(ctx context.Context) (*T, error),
	emptyValueErrSup func() error,
) Lazy[T] {
	return Lazy[T]{core: newCore(boxFetcher(fetcher), emptyValueErrSup)}
}

// Just creates a new Lazy with value
func Just[T any](v T) Lazy[T] {
	return Lazy[T]{core: newCore(justFetcher(&v), defaultEmptyValueErr)}
}

func JustWithErr[T any](v T, err error) Lazy[T] {
	return Lazy[T]{core: newCore(justWithErrFetcher(&v, err), defaultEmptyValueErr)}
}

// JustOptional creates a new Lazy with optional value
func JustOptional[T any](v *T) Lazy[T] {
	return Lazy[T]{core: newCore(justFetcher(boxPtr(v)), defaultEmptyValueErr)}
}

func JustOptionalWithErr[T any](v *T, err error) Lazy[T] {
	return Lazy[T]{core: newCore(justWithErrFetcher(boxPtr(v), err), defaultEmptyValueErr)}
}

// JustOptionalOrElseThrow creates a new Lazy with optional value and alternative empty value error
func JustOptionalOrElseThrow[T any](v *T, errSup func() error) Lazy[T] {
	return Lazy[T]{core: newCore(justFetcher(boxPtr(v)), errSup)}
}

// NewLazy creates a new Lazy with value, does not allow nil values (not optional
func NewLazy[T any](fetcher func(ctx context.Context) (T, error)) Lazy[T] {
	return Lazy[T]{core: newCore(boxValueFetcher(fetcher), defaultEmptyValueErr)}
}

// Empty gets an empty Lazy
func Empty[T any]() Lazy[T] {
	return Lazy[T]{core: newCore(emptyFetcher, defaultEmptyValueErr)}
}

// Error creates a new Lazy with error
func Error[T any](err error) Lazy[T] {
	return Lazy[T]{core: newCore(errorFetcher(err), defaultEmptyValueErr)}
}

// Get returns the int value or an error. it will return an error if the value is not present. see GetOptional
// to get an optional value
func (o Lazy[T]) Get(ctx context.Context) (T, error) {
	v, err := o.core.get(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	return *unbox[T](v), nil
}

// GetOptional returns the value or an error. it will return nil if lazy value is empty
func (o Lazy[T]) GetOptional(ctx context.Context) (*T, error) {
	v, err := o.core.fetch(ctx)
	return unbox[T](v), err
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
	d, err := o.core.fetch(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if d == nil {
		return v, nil
	}
	return *unbox[T](d), nil
}

// MustOrElse returns the int value or a default value if the value is not present.
// it will panic in case of error, use for testing or when the value is static
func (o Lazy[T]) MustOrElse(v T) T {
	d, err := o.OrElse(context.Background(), v)
	if err != nil {
		panic(err)
	}
	return d
}

// OrElseThrow returns lazy that will emit a specified error if the value is not present
func (o Lazy[T]) OrElseThrow(errSup func() error) Lazy[T] {
	return Lazy[T]{core: o.core.orElseThrow(errSup)}
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
	return Lazy[T]{core: o.core.filter(boxPredicate(predicate))}
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
//
//go:noinline
func MapWithErrAndCtx[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErrAndCtx[SRC, TGT]) Lazy[TGT] {
	return Lazy[TGT]{core: src.core.mapCore(func(ctx context.Context, v any) (any, error) {
		tgt, err := mapper(ctx, *unbox[SRC](v))
		if err != nil {
			return nil, err
		}
		return &tgt, nil
	})}
}

func MapWhileFiltering[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.Mapper[SRC, *TGT]) Lazy[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx())
}
func MapWhileFilteringWithErr[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErr[SRC, *TGT]) Lazy[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx())
}

//go:noinline
func MapWhileFilteringWithErrAndCtx[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.MapperWithErrAndCtx[SRC, *TGT]) Lazy[TGT] {
	return Lazy[TGT]{core: src.core.mapCore(func(ctx context.Context, v any) (any, error) {
		p, err := mapper(ctx, *unbox[SRC](v))
		return boxPtr(p), err
	})}
}

// OrElseGet returns the int value or a default value if the value is not present.
func (o Lazy[T]) OrElseGet(ctx context.Context, alt func() T) (T, error) {
	ret, err := o.core.fetch(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if ret != nil {
		return *unbox[T](ret), nil
	}
	return alt(), nil
}

// MustOrElseGet returns the int value or a default value if the value is not present.
// it will panic in case of error, use for testing or when the value is static
func (o Lazy[T]) MustOrElseGet(alt func() T) T {
	ret, err := o.OrElseGet(context.Background(), alt)
	if err != nil {
		panic(err)
	}
	return ret
}

func (o Lazy[T]) Or(alt Lazy[T]) Lazy[T] {
	return Lazy[T]{core: o.core.or(alt.core)}
}

func (o Lazy[T]) MarshalJSON() ([]byte, error) {
	return o.core.marshalJSON()
}

func (o Lazy[T]) UnmarshalJSON(data []byte) error {

	if string(data) == "null" {
		o.core.fetcher = emptyFetcher
		return nil
	}

	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	o.core.fetcher = justFetcher(&value)
	return nil
}

func (o Lazy[T]) IsEmpty(ctx context.Context) (bool, error) {
	return o.core.isEmpty(ctx)
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
//
//go:noinline
func FlatMap[SRC any, TGT any](src Lazy[SRC], mapper shpanstream.Mapper[SRC, Lazy[TGT]]) Lazy[TGT] {
	return Lazy[TGT]{core: src.core.mapCore(func(ctx context.Context, v any) (any, error) {
		return mapper(*unbox[SRC](v)).core.fetch(ctx)
	})}
}

// Consume consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
//
//go:noinline
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
//
//go:noinline
func (o Lazy[T]) ConsumeWithErr(ctx context.Context, f func(value T) error) error {
	return o.ConsumeWithErrAndCtx(ctx, func(_ context.Context, value T) error {
		return f(value)
	})
}

// ConsumeWithErrAndCtx consumes the value of the Lazy using the provided consumer function.
// If lazy is empty, it will not call the consumer function.
func (o Lazy[T]) ConsumeWithErrAndCtx(ctx context.Context, f func(ctx context.Context, value T) error) error {
	return o.core.consume(ctx, boxConsumer(f))
}

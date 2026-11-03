package lazy

import (
	"context"
	"encoding/json"
	"fmt"
)

// lazyCore is the type-independent half of Lazy[T], compiled once for every T: the deferred fetch,
// the empty-value rule and the combinators over it. The value travels as a boxed *T — a nil
// interface when the value is absent — so boxing it is free (a pointer never allocates when
// converted to an interface) and Lazy[T] itself is reduced to unboxing and to the adapter closures
// that touch T (see docs/compile-size-per-element-type.md).
type lazyCore struct {
	fetcher               anyFetcher
	emptyValueErrSupplier func() error
}

// anyFetcher fetches the value as a boxed *T, or a nil interface when the value is absent.
type anyFetcher func(ctx context.Context) (any, error)

var defaultEmptyValueErr = func() error {
	return fmt.Errorf("lazy value is empty")
}

func newCore(fetcher anyFetcher, emptyValueErrSup func() error) lazyCore {
	return lazyCore{fetcher: fetcher, emptyValueErrSupplier: emptyValueErrSup}
}

// Fetchers for the constant lazies (Just*, Empty, Error). Compiled once, like everything here.

func justFetcher(v any) anyFetcher {
	return func(context.Context) (any, error) {
		return v, nil
	}
}

func justWithErrFetcher(v any, err error) anyFetcher {
	return func(context.Context) (any, error) {
		return v, err
	}
}

func emptyFetcher(context.Context) (any, error) {
	return nil, nil
}

func errorFetcher(err error) anyFetcher {
	return func(context.Context) (any, error) {
		return nil, err
	}
}

// fetch returns the boxed value (nil when absent) or the fetch error.
func (c lazyCore) fetch(ctx context.Context) (any, error) {
	return c.fetcher(ctx)
}

// get returns the boxed value, the fetch error, or the empty-value error when the value is absent.
func (c lazyCore) get(ctx context.Context) (any, error) {
	v, err := c.fetcher(ctx)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, c.emptyValueErrSupplier()
	}
	return v, nil
}

func (c lazyCore) isEmpty(ctx context.Context) (bool, error) {
	v, err := c.fetcher(ctx)
	if err != nil {
		return false, err
	}
	return v == nil, nil
}

func (c lazyCore) orElseThrow(errSup func() error) lazyCore {
	return newCore(c.fetcher, errSup)
}

// filter keeps the value only when predicate (called with the boxed, non-nil value) holds.
func (c lazyCore) filter(predicate func(ctx context.Context, v any) (bool, error)) lazyCore {
	return newCore(func(ctx context.Context) (any, error) {
		v, err := c.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if v == nil {
			// If the value is nil, no need to check the predicate
			return nil, nil
		}
		matchesFilter, err := predicate(ctx, v)
		if err != nil {
			return nil, err
		}
		if !matchesFilter {
			return nil, nil
		}
		return v, nil
	}, c.emptyValueErrSupplier)
}

// mapCore maps a present value through mapper, which receives the boxed non-nil source value and
// returns the boxed target value (nil to filter it out). An absent value stays absent, and the
// empty-value error supplier is inherited.
func (c lazyCore) mapCore(mapper func(ctx context.Context, v any) (any, error)) lazyCore {
	return newCore(func(ctx context.Context) (any, error) {
		srcValue, err := c.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if srcValue == nil {
			return nil, nil
		}
		return mapper(ctx, srcValue)
	}, c.emptyValueErrSupplier)
}

// or falls back to alt when this value is absent; the result reports alt's empty-value error.
func (c lazyCore) or(alt lazyCore) lazyCore {
	return newCore(func(ctx context.Context) (any, error) {
		orig, err := c.fetcher(ctx)
		if err != nil {
			return nil, err
		}
		if orig != nil {
			return orig, nil
		}
		return alt.fetcher(ctx)
	}, alt.emptyValueErrSupplier)
}

// consume hands a present value (boxed, non-nil) to f; an absent value is not consumed.
func (c lazyCore) consume(ctx context.Context, f func(ctx context.Context, v any) error) error {
	v, err := c.fetcher(ctx)
	if err != nil {
		return err
	}
	if v != nil {
		return f(ctx, v)
	}
	return nil
}

// marshalJSON marshals the fetched *T (so a present value marshals as T, an absent one as null).
func (c lazyCore) marshalJSON() ([]byte, error) {
	data, err := c.fetcher(context.Background())
	if err != nil {
		return nil, err
	}
	return json.Marshal(data)
}

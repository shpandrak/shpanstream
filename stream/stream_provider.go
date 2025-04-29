package stream

import "context"

// Provider is what needs to implement to expose a stream.
// it includes the lifecycle methods Open and Close.
// and a "generator method" Emit that returns the next item in the stream.
type Provider[T any] interface {
	Lifecycle

	// Emit returns the next item in the stream, or an error
	// When the stream is done, it should return io.EOF
	// The shpanstream package will handle the io.EOF error and will not propagate it to the user
	// The shpanstream will not call Emit concurrently, from multiple goroutines.
	// it is the provider's responsibility to respect context cancellation if supported.
	// the shpanstream package will check for context cancellation between calls to Emit.
	Emit(ctx context.Context) (T, error)
}

// Lifecycle is an interface that is used to add functionality to the stream lifecycle.
type Lifecycle interface {
	Open(ctx context.Context) error
	Close()
}

type lifecycleWrapper struct {
	openFunc  func(ctx context.Context) error
	closeFunc func()
}

func NewLifecycle(openFunc func(ctx context.Context) error, closeFunc func()) Lifecycle {
	return &lifecycleWrapper{openFunc: openFunc, closeFunc: closeFunc}
}

func (s *lifecycleWrapper) Open(ctx context.Context) error {
	if s.openFunc != nil {
		return s.openFunc(ctx)
	}
	return nil
}

func (s *lifecycleWrapper) Close() {
	if s.closeFunc != nil {
		s.closeFunc()
	}
}

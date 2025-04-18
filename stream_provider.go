package shpanstream

import "context"

// StreamProvider is what needs to implement to expose a stream.
// it includes the lifecycle methods Open and Close.
// and a "generator method" Emit that returns the next item in the stream.
type StreamProvider[T any] interface {
	StreamLifecycle

	// Emit returns the next item in the stream, or an error
	// When the stream is done, it should return io.EOF
	// The shpanstream package will handle the io.EOF error and will not propagate it to the user
	// The shpanstream will not call Emit concurrently, from multiple goroutines.
	// it is the provider's responsibility to respect context cancellation if supported.
	// the shpanstream package will check for context cancellation between calls to Emit.
	Emit(ctx context.Context) (T, error)
}

// StreamLifecycle is an interface that is used to add functionality to the stream lifecycle.
type StreamLifecycle interface {
	Open(ctx context.Context) error
	Close()
}

type streamLifecycleWrapper struct {
	openFunc  func(ctx context.Context) error
	closeFunc func()
}

func NewStreamLifecycle(openFunc func(ctx context.Context) error, closeFunc func()) StreamLifecycle {
	return &streamLifecycleWrapper{openFunc: openFunc, closeFunc: closeFunc}
}

func (s *streamLifecycleWrapper) Open(ctx context.Context) error {
	if s.openFunc != nil {
		return s.openFunc(ctx)
	}
	return nil
}

func (s *streamLifecycleWrapper) Close() {
	if s.closeFunc != nil {
		s.closeFunc()
	}
}

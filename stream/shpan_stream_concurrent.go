package stream

// ConsumeOption defines options for consuming a stream
type ConsumeOption interface {
	consumeOptionName() string
}

type concurrentConsumeOption struct {
	concurrency int
}

// WithConcurrentConsumeOption causes the consumer function to be called concurrently in separate goroutines.
// Note that the source stream is being read sequentially, and only the consumer function is called concurrently.
// This is useful when the consumer function is expensive (e.g., I/O bound operations).
// Note that the order of consumption is not guaranteed to match the order of the source stream.
func WithConcurrentConsumeOption(concurrency int) ConsumeOption {
	return &concurrentConsumeOption{concurrency: concurrency}
}

func (c *concurrentConsumeOption) consumeOptionName() string {
	return "concurrent"
}

// concurrentConsumeOptionOf reports the concurrency of the first concurrent option among options,
// if any. Other option kinds are ignored, as they always were.
func concurrentConsumeOptionOf(options []ConsumeOption) (int, bool) {
	for _, opt := range options {
		if cOpt, ok := opt.(*concurrentConsumeOption); ok {
			return cOpt.concurrency, true
		}
	}
	return 0, false
}

package shpanstream

import (
	"context"
	"fmt"
	"io"
)

type clusterSortedStream[T any, O any, C comparable] struct {
	nextItem              *T
	currClassifier        C
	src                   Stream[T]
	clusterClassifierFunc func(a *T) C
	merger                func(ctx context.Context, clusterClassifier C, clusterStream Stream[T]) (O, error)
}

// ClusterSortedStream creates a Stream that clusters items from the source Stream based on a classifier function.
// it assumes the stream is sorted by the classifier function so that items with the same classifier are adjacent.
// this is useful for many applications, e.g. when the data is time sorted and you want to group by time intervals
// and provide aggregate values.
func ClusterSortedStream[T any, O any, C comparable](
	clusterFactory func(ctx context.Context, clusterClassifier C, clusterStream Stream[T]) (O, error),
	clusterClassifierFunc func(a *T) C,
	src Stream[T]) Stream[O] {

	return NewStream[O](&clusterSortedStream[T, O, C]{
		src:                   src,
		clusterClassifierFunc: clusterClassifierFunc,
		merger:                clusterFactory,
	})

}

func (fs *clusterSortedStream[T, O, C]) Open(ctx context.Context) error {
	openErr := openSubStream(ctx, fs.src)
	if openErr != nil {
		return openErr
	}
	nextItem, firstErr := fs.src.provider(ctx)
	if firstErr != nil {
		if firstErr == io.EOF {
			fs.nextItem = nil
			return nil
		}
		return firstErr
	}

	fs.nextItem = &nextItem
	fs.currClassifier = fs.clusterClassifierFunc(fs.nextItem)
	return nil
}

func (fs *clusterSortedStream[T, O, C]) Close() {
	closeSubStream(fs.src)
}

func (fs *clusterSortedStream[T, O, C]) Emit(ctx context.Context) (O, error) {
	if fs.nextItem == nil {
		return defaultValue[O](), io.EOF
	}

	currClusterClassifier := fs.currClassifier

	// Create a cluster Stream that yields items belonging to the current cluster
	clusterStream := newStream(

		func(ctx context.Context) (T, error) {
			if fs.nextItem == nil {
				return defaultValue[T](), io.EOF
			}

			nextClassifier := fs.clusterClassifierFunc(fs.nextItem)
			if nextClassifier != currClusterClassifier {
				// Next item belongs to a new cluster
				return defaultValue[T](), io.EOF
			}

			// Yield fs.nextItem
			item := *fs.nextItem

			// Advance fs.nextItem
			next, err := fs.src.provider(ctx)
			if err != nil {
				if err == io.EOF {
					// No more items
					fs.nextItem = nil
				} else {
					// Error occurred
					return defaultValue[T](), err
				}
			} else {
				fs.nextItem = &next
			}

			return item, nil
		},

		// Avoid closing the underlying Stream!
		nil,
	)

	// Call the merger function with the current cluster classifier and the cluster Stream
	result, mergeErr := fs.merger(ctx, currClusterClassifier, clusterStream)
	if mergeErr != nil {
		// Make sure we wrap the error so e.g. even if it is io.EOF, it is not mistaken for end of Stream (because go is stupid)
		return defaultValue[O](), fmt.Errorf("failed merging: %w", mergeErr)
	}

	// Update fs.currClassifier if fs.nextItem is not nil
	if fs.nextItem != nil {
		fs.currClassifier = fs.clusterClassifierFunc(fs.nextItem)
	}

	return result, nil
}

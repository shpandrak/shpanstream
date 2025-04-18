package shpanstream

import (
	"context"
	"fmt"
	"io"
)

type clusterSortedStream[T any, O any, C comparable] struct {
	nextItem              *T
	currClassifier        C
	clusterClassifierFunc func(a *T) C
	merger                func(ctx context.Context, clusterClassifier C, clusterStream Stream[T]) (O, error)
}

// ClusterSortedStream creates a Stream that clusters items from the source Stream based on a classifier function.
// it assumes the stream is sorted by the classifier function so that items with the same classifier are adjacent.
// this is useful for many applications, e.g. when the data is time sorted, and you want to group by time intervals
// and provide aggregate values.
// The stream is memory efficient and does not load ech cluster into memory, but stream the items down the merger function.
func ClusterSortedStream[T any, O any, C comparable](
	clusterFactory func(ctx context.Context, clusterClassifier C, clusterStream Stream[T]) (O, error),
	clusterClassifierFunc func(a *T) C,
	src Stream[T]) Stream[O] {

	css := &clusterSortedStream[T, O, C]{
		clusterClassifierFunc: clusterClassifierFunc,
		merger:                clusterFactory,
	}

	return NewDownStream[T, O](
		src,
		css.emit,
		css.open,
		nil,
	)
}

func (fs *clusterSortedStream[T, O, C]) open(ctx context.Context, srcProviderFunc StreamProviderFunc[T]) error {
	nextItem, firstErr := srcProviderFunc(ctx)
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

func (fs *clusterSortedStream[T, O, C]) emit(ctx context.Context, srcProviderFunc StreamProviderFunc[T]) (O, error) {
	if fs.nextItem == nil {
		return defaultValue[O](), io.EOF
	}

	currClusterClassifier := fs.currClassifier

	// Create a cluster Stream that yields items belonging to the current cluster
	// This will create a "virtual" Stream that will yield items until the next cluster is found
	// The source stream will not be closed
	clusterStream := NewSimpleStream[T](

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
			next, err := srcProviderFunc(ctx)
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

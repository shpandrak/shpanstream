package shpanstream

import (
	"cmp"
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type clusterSortedStream[T any, O any, C any] struct {
	nextItem              *T
	currClassifier        C
	comparator            Comparator[C]
	clusterClassifierFunc func(a T) C
	merger                func(ctx context.Context, clusterClassifier C, clusterStream Stream[T], lastItemOnPreviousCluster *T) (O, error)

	lastItemOnPreviousCluster *T
}

// ClusterSortedStream creates a Stream that clusters items from the source Stream based on a classifier function.
// it assumes the stream is sorted by the classifier function so that items with the same classifier are adjacent.
// this is useful for many applications, e.g. when the data is time sorted, and you want to group by time intervals
// and provide aggregate values.
// The stream is memory efficient and does not load ech cluster into memory, but stream the items down the merger function.
func ClusterSortedStream[T any, O any, C any](
	clusterFactory func(ctx context.Context, clusterClassifier C, clusterStream Stream[T], lastItemOnPreviousCluster *T) (O, error),
	clusterClassifierFunc func(a T) C,
	comparator Comparator[C],
	src Stream[T]) Stream[O] {
	return NewDownStream[T, O](
		src,
		&clusterSortedStream[T, O, C]{
			clusterClassifierFunc: clusterClassifierFunc,
			merger:                clusterFactory,
			comparator:            comparator,
		},
	)

}

// ClusterSortedStreamComparable creates a Stream that clusters items from the source Stream based on a classifier function.
// it assumes the stream is sorted by the classifier function so that items with the same classifier are adjacent.
// this is useful for many applications, e.g. when the data is time sorted, and you want to group by time intervals
// and provide aggregate values.
// The stream is memory efficient and does not load ech cluster into memory, but stream the items down the merger function.
func ClusterSortedStreamComparable[T any, O any, C Comparable[C]](
	clusterFactory func(ctx context.Context, clusterClassifier C, clusterStream Stream[T], lastItemOnPreviousCluster *T) (O, error),
	clusterClassifierFunc func(a T) C,
	src Stream[T]) Stream[O] {

	return ClusterSortedStream(
		clusterFactory,
		clusterClassifierFunc,
		// Default comparator for Comparable
		ComparatorForComparable[C](),
		src,
	)
}

// ClusterSortedStreamOrdered creates a Stream that clusters items from the source Stream based on a classifier function.
// it assumes the stream is sorted by the classifier function so that items with the same classifier are adjacent.
// this is useful for many applications, e.g. when the data is time sorted, and you want to group by time intervals
// and provide aggregate values.
// The stream is memory efficient and does not load ech cluster into memory, but stream the items down the merger function.
func ClusterSortedStreamOrdered[T any, O any, C cmp.Ordered](
	clusterFactory func(ctx context.Context, clusterClassifier C, clusterStream Stream[T], lastItemOnPreviousCluster *T) (O, error),
	clusterClassifierFunc func(a T) C,
	src Stream[T]) Stream[O] {
	return ClusterSortedStream(
		clusterFactory,
		clusterClassifierFunc,
		// Default comparator for Comparable
		cmp.Compare,
		src,
	)
}

func (fs *clusterSortedStream[T, O, C]) Open(ctx context.Context, srcProviderFunc StreamProviderFunc[T]) error {
	nextItem, firstErr := srcProviderFunc(ctx)
	if firstErr != nil {
		if firstErr == io.EOF {
			fs.nextItem = nil
			return nil
		}
		return firstErr
	}

	fs.nextItem = &nextItem
	fs.currClassifier = fs.clusterClassifierFunc(*fs.nextItem)
	return nil
}

func (fs *clusterSortedStream[T, O, C]) Emit(ctx context.Context, srcProviderFunc StreamProviderFunc[T]) (O, error) {
	if fs.nextItem == nil {
		return util.DefaultValue[O](), io.EOF
	}

	currClusterClassifier := fs.currClassifier

	// Create a cluster Stream that yields items belonging to the current cluster
	// This will create a "virtual" Stream that will yield items until the next cluster is found
	// The source stream will not be closed
	currClusterStream := NewSimpleStream[T](

		func(ctx context.Context) (T, error) {
			if fs.nextItem == nil {
				return util.DefaultValue[T](), io.EOF
			}

			nextClassifier := fs.clusterClassifierFunc(*fs.nextItem)
			compareRes := fs.comparator(currClusterClassifier, nextClassifier)
			if compareRes != 0 {
				// Next item belongs to a new cluster
				return util.DefaultValue[T](), io.EOF
			}

			// Yield fs.nextItem
			item := *fs.nextItem
			fs.lastItemOnPreviousCluster = fs.nextItem
			// Advance fs.nextItem
			next, err := srcProviderFunc(ctx)
			if err != nil {
				if err == io.EOF {
					// No more items
					fs.nextItem = nil
				} else {
					// Error occurred
					return util.DefaultValue[T](), err
				}
			} else {
				fs.nextItem = &next
			}

			return item, nil
		},
	)

	// Call the merger function with the current cluster classifier and the cluster Stream
	result, mergeErr := fs.merger(ctx, currClusterClassifier, currClusterStream, fs.lastItemOnPreviousCluster)
	if mergeErr != nil {
		// Make sure we wrap the error so e.g. even if it is io.EOF, it is not mistaken for end of Stream (because go is stupid)
		return util.DefaultValue[O](), fmt.Errorf("failed merging: %w", mergeErr)
	}

	// Update fs.currClassifier if fs.nextItem is not nil
	if fs.nextItem != nil {
		nextClassifier := fs.clusterClassifierFunc(*fs.nextItem)

		compareRes := fs.comparator(currClusterClassifier, nextClassifier)
		// Advance fs.nextItem until we find the next cluster, this is needed since the merger function
		// might have not consumed all items (e.g. limit, findFirst)
		for compareRes == 0 && fs.nextItem != nil {
			// Advance fs.nextItem
			var err error
			next, err := srcProviderFunc(ctx)
			if err != nil {
				if err == io.EOF {
					fs.nextItem = nil
				} else {
					return util.DefaultValue[O](), err
				}
			} else {
				nextClassifier = fs.clusterClassifierFunc(next)
				compareRes = fs.comparator(currClusterClassifier, nextClassifier)
				if compareRes > 0 {
					return util.DefaultValue[O](), fmt.Errorf("cluster stream is not sorted: %v < %v", currClusterClassifier, nextClassifier)
				}
				fs.lastItemOnPreviousCluster = fs.nextItem
				fs.nextItem = &next
			}
		}
		fs.currClassifier = nextClassifier
	}

	return result, nil
}

func (fs *clusterSortedStream[T, O, C]) Close() {
	// Nop
}

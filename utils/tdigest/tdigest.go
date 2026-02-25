package tdigest

import (
	"math"
	"sort"
)

// TDigest is a streaming quantile estimator using the merging t-digest algorithm.
// It provides approximate quantile queries in O(compression) space regardless of
// the number of values added.
type TDigest struct {
	compression float64
	centroids   []centroid
	buffer      []float64
	bufferCap   int
	totalCount  float64
}

type centroid struct {
	mean  float64
	count float64
}

// New creates a TDigest with the given compression parameter.
// Higher compression yields better accuracy at the cost of more memory.
// A value of 100 is standard and provides <1% error for most quantiles.
func New(compression float64) *TDigest {
	bufCap := int(compression) * 5
	return &TDigest{
		compression: compression,
		centroids:   make([]centroid, 0, int(compression)*2),
		buffer:      make([]float64, 0, bufCap),
		bufferCap:   bufCap,
	}
}

// Add inserts a value into the digest. O(1) amortized.
// Non-finite values (NaN, Inf) are silently skipped because NaN causes
// undefined behavior in sort.Slice and Inf corrupts interpolation math.
func (td *TDigest) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}
	td.buffer = append(td.buffer, value)
	if len(td.buffer) >= td.bufferCap {
		td.compress()
	}
}

// Count returns the total number of values added.
func (td *TDigest) Count() int64 {
	return int64(td.totalCount) + int64(len(td.buffer))
}

// Quantile returns the estimated value at quantile q (0 <= q <= 1).
// Returns 0 if no values have been added.
func (td *TDigest) Quantile(q float64) float64 {
	td.compress()
	if len(td.centroids) == 0 {
		return 0
	}
	if q <= 0 {
		return td.centroids[0].mean
	}
	if q >= 1 {
		return td.centroids[len(td.centroids)-1].mean
	}
	if len(td.centroids) == 1 {
		return td.centroids[0].mean
	}

	target := q * td.totalCount

	// Walk centroids accumulating count. Interpolate within the centroid
	// whose cumulative weight range contains the target.
	cumulative := 0.0
	for i, c := range td.centroids {
		lower := cumulative
		upper := cumulative + c.count
		mid := lower + c.count/2

		if target < mid {
			// Interpolate between previous centroid and this one
			if i == 0 {
				// Before the midpoint of the first centroid — clamp to min
				return c.mean
			}
			prev := td.centroids[i-1]
			prevMid := lower - prev.count/2
			// Linear interpolation between prev midpoint and this midpoint
			frac := (target - prevMid) / (mid - prevMid)
			return prev.mean + frac*(c.mean-prev.mean)
		}
		cumulative = upper
	}
	return td.centroids[len(td.centroids)-1].mean
}

// compress merges the buffer into the sorted centroid list.
func (td *TDigest) compress() {
	if len(td.buffer) == 0 {
		return
	}

	// Merge buffer values as unit-weight centroids
	newCentroids := make([]centroid, len(td.centroids), len(td.centroids)+len(td.buffer))
	copy(newCentroids, td.centroids)
	for _, v := range td.buffer {
		newCentroids = append(newCentroids, centroid{mean: v, count: 1})
	}
	td.totalCount += float64(len(td.buffer))
	td.buffer = td.buffer[:0]

	// Sort by mean
	sort.Slice(newCentroids, func(i, j int) bool {
		return newCentroids[i].mean < newCentroids[j].mean
	})

	// Merge centroids using the k1 scale function
	merged := make([]centroid, 0, int(td.compression)*2)
	merged = append(merged, newCentroids[0])
	cumCount := newCentroids[0].count

	for i := 1; i < len(newCentroids); i++ {
		c := newCentroids[i]
		proposedCount := merged[len(merged)-1].count + c.count
		q := (cumCount - merged[len(merged)-1].count/2) / td.totalCount

		// k1 scale function: max cluster size at quantile q
		maxSize := td.maxClusterSize(q)

		if proposedCount <= maxSize {
			// Merge into the last centroid
			last := &merged[len(merged)-1]
			last.mean += (c.mean - last.mean) * c.count / proposedCount
			last.count = proposedCount
		} else {
			// Start a new centroid
			merged = append(merged, c)
		}
		cumCount += c.count
	}
	td.centroids = merged
}

// maxClusterSize returns the maximum count a centroid at quantile q can hold,
// using the k1 scale function: k(q) = (compression / (2*pi)) * asin(2q - 1).
func (td *TDigest) maxClusterSize(q float64) float64 {
	// The derivative of k1 is: k'(q) = (compression / pi) / sqrt(4q(1-q))
	// Max cluster weight = totalCount / k'(q) = totalCount * pi * sqrt(4q(1-q)) / compression
	qClamped := math.Max(1e-10, math.Min(q, 1-1e-10))
	return td.totalCount * math.Pi * math.Sqrt(4*qClamped*(1-qClamped)) / td.compression
}

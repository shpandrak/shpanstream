package tdigest

import (
	"math"
	"sort"
	"testing"
)

func TestEmpty(t *testing.T) {
	td := New(100)
	if td.Count() != 0 {
		t.Fatalf("expected count 0, got %d", td.Count())
	}
	if q := td.Quantile(0.5); q != 0 {
		t.Fatalf("expected 0 for empty digest, got %f", q)
	}
}

func TestSingleValue(t *testing.T) {
	td := New(100)
	td.Add(42.0)
	if td.Count() != 1 {
		t.Fatalf("expected count 1, got %d", td.Count())
	}
	for _, q := range []float64{0, 0.25, 0.5, 0.75, 1.0} {
		got := td.Quantile(q)
		if got != 42.0 {
			t.Errorf("Quantile(%f) = %f, want 42.0", q, got)
		}
	}
}

func TestTwoValues(t *testing.T) {
	td := New(100)
	td.Add(10.0)
	td.Add(20.0)

	if got := td.Quantile(0); got != 10.0 {
		t.Errorf("Quantile(0) = %f, want 10.0", got)
	}
	if got := td.Quantile(1); got != 20.0 {
		t.Errorf("Quantile(1) = %f, want 20.0", got)
	}
	// Median should be close to 15
	got := td.Quantile(0.5)
	if math.Abs(got-15.0) > 1.0 {
		t.Errorf("Quantile(0.5) = %f, want ~15.0", got)
	}
}

func TestUniformDistribution(t *testing.T) {
	td := New(100)
	n := 10000
	for i := 1; i <= n; i++ {
		td.Add(float64(i))
	}

	if td.Count() != int64(n) {
		t.Fatalf("expected count %d, got %d", n, td.Count())
	}

	// For uniform 1..N, exact percentile (method 7) = 1 + phi*(N-1)
	tests := []struct {
		q      float64
		maxErr float64 // max relative error
	}{
		{0.50, 0.01},
		{0.75, 0.01},
		{0.90, 0.01},
		{0.95, 0.01},
		{0.99, 0.01},
		{0.999, 0.005},
	}

	for _, tc := range tests {
		exact := 1.0 + tc.q*float64(n-1)
		got := td.Quantile(tc.q)
		relErr := math.Abs(got-exact) / exact
		if relErr > tc.maxErr {
			t.Errorf("Quantile(%g) = %f, exact = %f, relErr = %f (max %f)",
				tc.q, got, exact, relErr, tc.maxErr)
		}
	}
}

func TestMonotonicallyIncreasing(t *testing.T) {
	td := New(100)
	for i := 0; i < 1000; i++ {
		td.Add(float64(i))
	}

	// Quantiles should be monotonically non-decreasing
	prev := td.Quantile(0)
	for q := 0.01; q <= 1.0; q += 0.01 {
		cur := td.Quantile(q)
		if cur < prev-1e-10 {
			t.Errorf("non-monotonic: Quantile(%f) = %f < Quantile(%f) = %f", q, cur, q-0.01, prev)
		}
		prev = cur
	}
}

func TestMonotonicallyDecreasing(t *testing.T) {
	td := New(100)
	for i := 999; i >= 0; i-- {
		td.Add(float64(i))
	}

	// Should produce same results regardless of insertion order
	prev := td.Quantile(0)
	for q := 0.01; q <= 1.0; q += 0.01 {
		cur := td.Quantile(q)
		if cur < prev-1e-10 {
			t.Errorf("non-monotonic: Quantile(%f) = %f < previous %f", q, cur, prev)
		}
		prev = cur
	}
}

func TestAccuracyAgainstExact(t *testing.T) {
	// Build a dataset and compare t-digest quantiles against exact computation
	values := make([]float64, 10000)
	for i := range values {
		// Mix of values to create a non-uniform distribution
		values[i] = float64(i*i) / 1000.0
	}

	td := New(100)
	for _, v := range values {
		td.Add(v)
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	quantiles := []float64{0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999}
	for _, q := range quantiles {
		exact := exactQuantile(sorted, q)
		got := td.Quantile(q)
		if exact == 0 {
			if math.Abs(got) > 1.0 {
				t.Errorf("Quantile(%g): got %f, exact %f", q, got, exact)
			}
			continue
		}
		relErr := math.Abs(got-exact) / math.Abs(exact)
		if relErr > 0.02 {
			t.Errorf("Quantile(%g): got %f, exact %f, relErr %f", q, got, exact, relErr)
		}
	}
}

// exactQuantile computes the exact quantile using linear interpolation (method 7).
func exactQuantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	idx := q * float64(n-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func TestBoundedMemory(t *testing.T) {
	td := New(100)
	for i := 0; i < 1_000_000; i++ {
		td.Add(float64(i))
	}
	// After compression, centroids should be bounded by O(compression)
	if len(td.centroids) > 500 {
		t.Errorf("too many centroids: %d (expected < 500 for compression=100)", len(td.centroids))
	}
}

func TestNaN(t *testing.T) {
	td := New(100)
	td.Add(1.0)
	td.Add(math.NaN())
	td.Add(2.0)
	td.Add(math.NaN())
	td.Add(3.0)

	if td.Count() != 3 {
		t.Fatalf("expected count 3 (NaN skipped), got %d", td.Count())
	}
	got := td.Quantile(0.5)
	if math.IsNaN(got) {
		t.Fatal("Quantile returned NaN — digest is corrupted")
	}
	if math.Abs(got-2.0) > 0.5 {
		t.Errorf("Quantile(0.5) = %f, want ~2.0", got)
	}
}

func TestInf(t *testing.T) {
	td := New(100)
	td.Add(math.Inf(1))
	td.Add(1.0)
	td.Add(math.Inf(-1))
	td.Add(2.0)
	td.Add(3.0)

	if td.Count() != 3 {
		t.Fatalf("expected count 3 (Inf skipped), got %d", td.Count())
	}
	got := td.Quantile(0.5)
	if math.IsInf(got, 0) {
		t.Fatal("Quantile returned Inf — digest is corrupted")
	}
	if math.Abs(got-2.0) > 0.5 {
		t.Errorf("Quantile(0.5) = %f, want ~2.0", got)
	}
}

func TestNegativeValues(t *testing.T) {
	td := New(100)
	for i := -500; i < 500; i++ {
		td.Add(float64(i))
	}

	if td.Count() != 1000 {
		t.Fatalf("expected count 1000, got %d", td.Count())
	}
	if got := td.Quantile(0); got != -500.0 {
		t.Errorf("Quantile(0) = %f, want -500", got)
	}
	if got := td.Quantile(1); math.Abs(got-499.0) > 1.0 {
		t.Errorf("Quantile(1) = %f, want ~499", got)
	}
	got := td.Quantile(0.5)
	if math.Abs(got-(-0.5)) > 10 {
		t.Errorf("Quantile(0.5) = %f, want ~-0.5", got)
	}
}

func TestAllIdenticalValues(t *testing.T) {
	td := New(100)
	for i := 0; i < 1000; i++ {
		td.Add(7.0)
	}

	if td.Count() != 1000 {
		t.Fatalf("expected count 1000, got %d", td.Count())
	}
	for _, q := range []float64{0, 0.25, 0.5, 0.75, 1.0} {
		got := td.Quantile(q)
		if got != 7.0 {
			t.Errorf("Quantile(%f) = %f, want 7.0", q, got)
		}
	}
}

func TestRepeatedValues(t *testing.T) {
	td := New(100)
	// 5 distinct values, 10K total — simulates low-cardinality metrics
	values := []float64{1, 2, 3, 4, 5}
	for i := 0; i < 10000; i++ {
		td.Add(values[i%len(values)])
	}

	if td.Count() != 10000 {
		t.Fatalf("expected count 10000, got %d", td.Count())
	}
	// Median should be 3 (the middle value)
	got := td.Quantile(0.5)
	if math.Abs(got-3.0) > 0.5 {
		t.Errorf("Quantile(0.5) = %f, want ~3.0", got)
	}
	// Low quantile should be near 1
	got = td.Quantile(0.1)
	if math.Abs(got-1.0) > 1.0 {
		t.Errorf("Quantile(0.1) = %f, want ~1.0", got)
	}
	// High quantile should be near 5
	got = td.Quantile(0.9)
	if math.Abs(got-5.0) > 1.0 {
		t.Errorf("Quantile(0.9) = %f, want ~5.0", got)
	}
}

func TestExtremeQuantiles(t *testing.T) {
	n := 100000
	td := New(100)
	for i := 1; i <= n; i++ {
		td.Add(float64(i))
	}

	// t-digest excels at tail accuracy — verify p0.1 and p99.9
	tests := []struct {
		q      float64
		maxErr float64
	}{
		{0.001, 0.01},
		{0.999, 0.01},
	}
	for _, tc := range tests {
		exact := 1.0 + tc.q*float64(n-1)
		got := td.Quantile(tc.q)
		relErr := math.Abs(got-exact) / exact
		if relErr > tc.maxErr {
			t.Errorf("Quantile(%g) = %f, exact = %f, relErr = %f (max %f)",
				tc.q, got, exact, relErr, tc.maxErr)
		}
	}
}

func TestNormalDistribution(t *testing.T) {
	// Box-Muller transform to generate normal(0,1) without math/rand
	td := New(100)
	n := 10000
	values := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		// Use a simple LCG to get reproducible pseudo-random numbers
		// without importing math/rand
		u1 := (float64(((i*1103515245+12345)>>16)&0x7fff) + 1) / 32768.0
		u2 := (float64(((i*1103515245+12345+7)>>16)&0x7fff) + 1) / 32768.0
		z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
		td.Add(z)
		values = append(values, z)
	}

	sort.Float64s(values)

	// Check p50 (should be near 0) and p99 accuracy
	p50 := td.Quantile(0.5)
	exactP50 := exactQuantile(values, 0.5)
	if math.Abs(p50-exactP50) > 0.1 {
		t.Errorf("p50 = %f, exact = %f, diff too large", p50, exactP50)
	}

	p99 := td.Quantile(0.99)
	exactP99 := exactQuantile(values, 0.99)
	relErr := math.Abs(p99-exactP99) / math.Abs(exactP99)
	if relErr > 0.05 {
		t.Errorf("p99 = %f, exact = %f, relErr = %f", p99, exactP99, relErr)
	}
}

func TestMixedMagnitudes(t *testing.T) {
	td := New(100)
	values := make([]float64, 0, 6000)
	// Add values spanning 1e-6 to 1e6
	for i := 0; i < 1000; i++ {
		for _, mag := range []float64{1e-6, 1e-3, 1, 1e3, 1e6, -1e3} {
			v := mag * (float64(i+1) / 1000.0)
			td.Add(v)
			values = append(values, v)
		}
	}

	sort.Float64s(values)

	// Verify no NaN or Inf in output across all quantiles
	for _, q := range []float64{0.01, 0.25, 0.5, 0.75, 0.99} {
		got := td.Quantile(q)
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Errorf("Quantile(%g) returned non-finite value: %f", q, got)
		}
	}
	// Verify min/max are in range
	gotMin := td.Quantile(0)
	gotMax := td.Quantile(1)
	if gotMin > gotMax {
		t.Errorf("min (%f) > max (%f)", gotMin, gotMax)
	}
}

func TestQuantileBoundary(t *testing.T) {
	td := New(100)
	td.Add(10.0)
	td.Add(20.0)
	td.Add(30.0)

	// q < 0 should return min
	if got := td.Quantile(-1); got != 10.0 {
		t.Errorf("Quantile(-1) = %f, want 10.0 (min)", got)
	}
	// q > 1 should return max
	if got := td.Quantile(2); got != 30.0 {
		t.Errorf("Quantile(2) = %f, want 30.0 (max)", got)
	}
	// q = 0 should return min
	if got := td.Quantile(0); got != 10.0 {
		t.Errorf("Quantile(0) = %f, want 10.0 (min)", got)
	}
	// q = 1 should return max
	if got := td.Quantile(1); got != 30.0 {
		t.Errorf("Quantile(1) = %f, want 30.0 (max)", got)
	}
}

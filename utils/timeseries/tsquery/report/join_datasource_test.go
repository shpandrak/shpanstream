package report

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// staticResultDatasource is a test helper that wraps a pre-built Result as a DataSource.
type staticResultDatasource struct {
	result Result
}

func (s staticResultDatasource) Execute(_ context.Context, from time.Time, to time.Time) (Result, error) {
	return NewResult(
		s.result.FieldsMeta(),
		s.result.Stream().Filter(func(r timeseries.TsRecord[[]any]) bool {
			return !r.Timestamp.Before(from) && r.Timestamp.Before(to)
		}),
	), nil
}

// Test data structures for join tests
type CPUMetrics struct {
	Timestamp time.Time
	Usage     float64
	Cores     int64
}

type MemoryMetrics struct {
	Timestamp time.Time
	UsedMB    float64
	TotalMB   float64
}

type DiskMetrics struct {
	Timestamp time.Time
	ReadMBps  float64
	WriteMBps float64
}

func TestJoinDatasource_InnerJoin_ThreeStreams(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create three streams with partial overlap
	// Only timestamps at hour 2 and hour 4 appear in all three streams
	cpuData := []CPUMetrics{
		{Timestamp: baseTime, Usage: 10.5, Cores: 4},
		{Timestamp: baseTime.Add(1 * time.Hour), Usage: 20.3, Cores: 4},
		{Timestamp: baseTime.Add(2 * time.Hour), Usage: 30.1, Cores: 4},
		{Timestamp: baseTime.Add(3 * time.Hour), Usage: 40.2, Cores: 4},
		{Timestamp: baseTime.Add(4 * time.Hour), Usage: 50.7, Cores: 4},
	}

	memoryData := []MemoryMetrics{
		{Timestamp: baseTime.Add(1 * time.Hour), UsedMB: 1024.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(2 * time.Hour), UsedMB: 2048.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(4 * time.Hour), UsedMB: 4096.0, TotalMB: 8192.0},
	}

	diskData := []DiskMetrics{
		{Timestamp: baseTime.Add(2 * time.Hour), ReadMBps: 100.0, WriteMBps: 50.0},
		{Timestamp: baseTime.Add(3 * time.Hour), ReadMBps: 150.0, WriteMBps: 75.0},
		{Timestamp: baseTime.Add(4 * time.Hour), ReadMBps: 200.0, WriteMBps: 100.0},
	}

	// Create datasources
	cpuDS, err := NewStaticStructDatasource(stream.Just(cpuData...))
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Just(memoryData...))
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Just(diskData...))
	require.NoError(t, err)

	// Create join datasource with inner join
	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		InnerJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Verify field metadata - should have all fields from all three datasources
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 6) // 2 CPU + 2 Memory + 2 Disk

	// Verify field URNs
	assert.Equal(t, "CPUMetrics:Usage", fieldsMeta[0].Urn())
	assert.Equal(t, "CPUMetrics:Cores", fieldsMeta[1].Urn())
	assert.Equal(t, "MemoryMetrics:UsedMB", fieldsMeta[2].Urn())
	assert.Equal(t, "MemoryMetrics:TotalMB", fieldsMeta[3].Urn())
	assert.Equal(t, "DiskMetrics:ReadMBps", fieldsMeta[4].Urn())
	assert.Equal(t, "DiskMetrics:WriteMBps", fieldsMeta[5].Urn())

	// Collect results
	records := result.Stream().MustCollect()

	// Inner join: only timestamps present in ALL streams
	// Only hour 2 and hour 4 are present in all three
	require.Len(t, records, 2)

	// First record at hour 2
	assert.Equal(t, baseTime.Add(2*time.Hour), records[0].Timestamp)
	assert.Equal(t, 30.1, records[0].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[0].Value[1]) // CPU Cores
	assert.Equal(t, 2048.0, records[0].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[0].Value[3])   // Memory TotalMB
	assert.Equal(t, 100.0, records[0].Value[4])    // Disk ReadMBps
	assert.Equal(t, 50.0, records[0].Value[5])     // Disk WriteMBps

	// Second record at hour 4
	assert.Equal(t, baseTime.Add(4*time.Hour), records[1].Timestamp)
	assert.Equal(t, 50.7, records[1].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[1].Value[1]) // CPU Cores
	assert.Equal(t, 4096.0, records[1].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[1].Value[3])   // Memory TotalMB
	assert.Equal(t, 200.0, records[1].Value[4])    // Disk ReadMBps
	assert.Equal(t, 100.0, records[1].Value[5])    // Disk WriteMBps
}

func TestJoinDatasource_LeftJoin_ThreeStreams(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Same data as InnerJoin test
	cpuData := []CPUMetrics{
		{Timestamp: baseTime, Usage: 10.5, Cores: 4},
		{Timestamp: baseTime.Add(1 * time.Hour), Usage: 20.3, Cores: 4},
		{Timestamp: baseTime.Add(2 * time.Hour), Usage: 30.1, Cores: 4},
		{Timestamp: baseTime.Add(3 * time.Hour), Usage: 40.2, Cores: 4},
		{Timestamp: baseTime.Add(4 * time.Hour), Usage: 50.7, Cores: 4},
	}

	memoryData := []MemoryMetrics{
		{Timestamp: baseTime.Add(1 * time.Hour), UsedMB: 1024.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(2 * time.Hour), UsedMB: 2048.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(4 * time.Hour), UsedMB: 4096.0, TotalMB: 8192.0},
	}

	diskData := []DiskMetrics{
		{Timestamp: baseTime.Add(2 * time.Hour), ReadMBps: 100.0, WriteMBps: 50.0},
		{Timestamp: baseTime.Add(3 * time.Hour), ReadMBps: 150.0, WriteMBps: 75.0},
		{Timestamp: baseTime.Add(4 * time.Hour), ReadMBps: 200.0, WriteMBps: 100.0},
	}

	// Create datasources
	cpuDS, err := NewStaticStructDatasource(stream.Just(cpuData...))
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Just(memoryData...))
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Just(diskData...))
	require.NoError(t, err)

	// Create join datasource with left join
	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		LeftJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Verify field metadata
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 6) // 2 CPU + 2 Memory + 2 Disk

	// Collect results
	records := result.Stream().MustCollect()

	// Left join: all timestamps from the LEFT stream (CPU)
	require.Len(t, records, 5)

	// Hour 0: CPU only
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 10.5, records[0].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[0].Value[1]) // CPU Cores
	assert.Nil(t, records[0].Value[2])             // Memory UsedMB - nil
	assert.Nil(t, records[0].Value[3])             // Memory TotalMB - nil
	assert.Nil(t, records[0].Value[4])             // Disk ReadMBps - nil
	assert.Nil(t, records[0].Value[5])             // Disk WriteMBps - nil

	// Hour 1: CPU + Memory
	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, 20.3, records[1].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[1].Value[1]) // CPU Cores
	assert.Equal(t, 1024.0, records[1].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[1].Value[3])   // Memory TotalMB
	assert.Nil(t, records[1].Value[4])             // Disk ReadMBps - nil
	assert.Nil(t, records[1].Value[5])             // Disk WriteMBps - nil

	// Hour 2: CPU + Memory + Disk (all three)
	assert.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	assert.Equal(t, 30.1, records[2].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[2].Value[1]) // CPU Cores
	assert.Equal(t, 2048.0, records[2].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[2].Value[3])   // Memory TotalMB
	assert.Equal(t, 100.0, records[2].Value[4])    // Disk ReadMBps
	assert.Equal(t, 50.0, records[2].Value[5])     // Disk WriteMBps

	// Hour 3: CPU + Disk
	assert.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
	assert.Equal(t, 40.2, records[3].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[3].Value[1]) // CPU Cores
	assert.Nil(t, records[3].Value[2])             // Memory UsedMB - nil
	assert.Nil(t, records[3].Value[3])             // Memory TotalMB - nil
	assert.Equal(t, 150.0, records[3].Value[4])    // Disk ReadMBps
	assert.Equal(t, 75.0, records[3].Value[5])     // Disk WriteMBps

	// Hour 4: CPU + Memory + Disk (all three)
	assert.Equal(t, baseTime.Add(4*time.Hour), records[4].Timestamp)
	assert.Equal(t, 50.7, records[4].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[4].Value[1]) // CPU Cores
	assert.Equal(t, 4096.0, records[4].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[4].Value[3])   // Memory TotalMB
	assert.Equal(t, 200.0, records[4].Value[4])    // Disk ReadMBps
	assert.Equal(t, 100.0, records[4].Value[5])    // Disk WriteMBps
}

func TestJoinDatasource_FullJoin_ThreeStreams(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Same data as previous tests
	cpuData := []CPUMetrics{
		{Timestamp: baseTime, Usage: 10.5, Cores: 4},
		{Timestamp: baseTime.Add(1 * time.Hour), Usage: 20.3, Cores: 4},
		{Timestamp: baseTime.Add(2 * time.Hour), Usage: 30.1, Cores: 4},
		{Timestamp: baseTime.Add(3 * time.Hour), Usage: 40.2, Cores: 4},
		{Timestamp: baseTime.Add(4 * time.Hour), Usage: 50.7, Cores: 4},
	}

	memoryData := []MemoryMetrics{
		{Timestamp: baseTime.Add(1 * time.Hour), UsedMB: 1024.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(2 * time.Hour), UsedMB: 2048.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(4 * time.Hour), UsedMB: 4096.0, TotalMB: 8192.0},
	}

	diskData := []DiskMetrics{
		{Timestamp: baseTime.Add(2 * time.Hour), ReadMBps: 100.0, WriteMBps: 50.0},
		{Timestamp: baseTime.Add(3 * time.Hour), ReadMBps: 150.0, WriteMBps: 75.0},
		{Timestamp: baseTime.Add(4 * time.Hour), ReadMBps: 200.0, WriteMBps: 100.0},
	}

	// Create datasources
	cpuDS, err := NewStaticStructDatasource(stream.Just(cpuData...))
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Just(memoryData...))
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Just(diskData...))
	require.NoError(t, err)

	// Create join datasource with full join
	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		FullJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Verify field metadata
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 6) // 2 CPU + 2 Memory + 2 Disk

	// Full join: all fields should be marked as not required (optional)
	for i := 0; i < 6; i++ {
		assert.False(t, fieldsMeta[i].Required(), "field %d (%s) should be optional after full join", i, fieldsMeta[i].Urn())
	}

	// Collect results
	records := result.Stream().MustCollect()

	// Full join: all unique timestamps from ANY stream
	// Hours: 0, 1, 2, 3, 4
	require.Len(t, records, 5)

	// Hour 0: CPU only
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 10.5, records[0].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[0].Value[1]) // CPU Cores
	assert.Nil(t, records[0].Value[2])             // Memory UsedMB - nil
	assert.Nil(t, records[0].Value[3])             // Memory TotalMB - nil
	assert.Nil(t, records[0].Value[4])             // Disk ReadMBps - nil
	assert.Nil(t, records[0].Value[5])             // Disk WriteMBps - nil

	// Hour 1: CPU + Memory
	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, 20.3, records[1].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[1].Value[1]) // CPU Cores
	assert.Equal(t, 1024.0, records[1].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[1].Value[3])   // Memory TotalMB
	assert.Nil(t, records[1].Value[4])             // Disk ReadMBps - nil
	assert.Nil(t, records[1].Value[5])             // Disk WriteMBps - nil

	// Hour 2: CPU + Memory + Disk (all three)
	assert.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	assert.Equal(t, 30.1, records[2].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[2].Value[1]) // CPU Cores
	assert.Equal(t, 2048.0, records[2].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[2].Value[3])   // Memory TotalMB
	assert.Equal(t, 100.0, records[2].Value[4])    // Disk ReadMBps
	assert.Equal(t, 50.0, records[2].Value[5])     // Disk WriteMBps

	// Hour 3: CPU + Disk
	assert.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
	assert.Equal(t, 40.2, records[3].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[3].Value[1]) // CPU Cores
	assert.Nil(t, records[3].Value[2])             // Memory UsedMB - nil
	assert.Nil(t, records[3].Value[3])             // Memory TotalMB - nil
	assert.Equal(t, 150.0, records[3].Value[4])    // Disk ReadMBps
	assert.Equal(t, 75.0, records[3].Value[5])     // Disk WriteMBps

	// Hour 4: CPU + Memory + Disk (all three)
	assert.Equal(t, baseTime.Add(4*time.Hour), records[4].Timestamp)
	assert.Equal(t, 50.7, records[4].Value[0])     // CPU Usage
	assert.Equal(t, int64(4), records[4].Value[1]) // CPU Cores
	assert.Equal(t, 4096.0, records[4].Value[2])   // Memory UsedMB
	assert.Equal(t, 8192.0, records[4].Value[3])   // Memory TotalMB
	assert.Equal(t, 200.0, records[4].Value[4])    // Disk ReadMBps
	assert.Equal(t, 100.0, records[4].Value[5])    // Disk WriteMBps
}

func TestJoinDatasource_EmptyStreams(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create datasources with empty streams
	cpuDS, err := NewStaticStructDatasource(stream.Empty[CPUMetrics]())
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Empty[MemoryMetrics]())
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Empty[DiskMetrics]())
	require.NoError(t, err)

	ctx := context.Background()

	// Test InnerJoin with empty streams
	innerJoinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		InnerJoin,
	)
	result, err := innerJoinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)
	records := result.Stream().MustCollect()
	assert.Len(t, records, 0)

	// Test LeftJoin with empty streams
	leftJoinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		LeftJoin,
	)
	result, err = leftJoinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)
	records = result.Stream().MustCollect()
	assert.Len(t, records, 0)

	// Test FullJoin with empty streams
	fullJoinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		FullJoin,
	)
	result, err = fullJoinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)
	records = result.Stream().MustCollect()
	assert.Len(t, records, 0)
}

func TestJoinDatasource_NoOverlap(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create streams with no overlapping timestamps
	cpuData := []CPUMetrics{
		{Timestamp: baseTime, Usage: 10.5, Cores: 4},
		{Timestamp: baseTime.Add(1 * time.Hour), Usage: 20.3, Cores: 4},
	}

	memoryData := []MemoryMetrics{
		{Timestamp: baseTime.Add(2 * time.Hour), UsedMB: 1024.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(3 * time.Hour), UsedMB: 2048.0, TotalMB: 8192.0},
	}

	diskData := []DiskMetrics{
		{Timestamp: baseTime.Add(4 * time.Hour), ReadMBps: 100.0, WriteMBps: 50.0},
		{Timestamp: baseTime.Add(5 * time.Hour), ReadMBps: 150.0, WriteMBps: 75.0},
	}

	cpuDS, err := NewStaticStructDatasource(stream.Just(cpuData...))
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Just(memoryData...))
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Just(diskData...))
	require.NoError(t, err)

	ctx := context.Background()

	// InnerJoin with no overlap should return empty
	innerJoinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		InnerJoin,
	)
	result, err := innerJoinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)
	records := result.Stream().MustCollect()
	assert.Len(t, records, 0)

	// FullJoin should return all 6 timestamps
	fullJoinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		FullJoin,
	)
	result, err = fullJoinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)
	records = result.Stream().MustCollect()
	assert.Len(t, records, 6)
}

func TestJoinDatasource_TimeFiltering(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	cpuData := []CPUMetrics{
		{Timestamp: baseTime, Usage: 10.5, Cores: 4},
		{Timestamp: baseTime.Add(1 * time.Hour), Usage: 20.3, Cores: 4},
		{Timestamp: baseTime.Add(2 * time.Hour), Usage: 30.1, Cores: 4},
		{Timestamp: baseTime.Add(3 * time.Hour), Usage: 40.2, Cores: 4},
	}

	memoryData := []MemoryMetrics{
		{Timestamp: baseTime, UsedMB: 1024.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(1 * time.Hour), UsedMB: 2048.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(2 * time.Hour), UsedMB: 3072.0, TotalMB: 8192.0},
		{Timestamp: baseTime.Add(3 * time.Hour), UsedMB: 4096.0, TotalMB: 8192.0},
	}

	diskData := []DiskMetrics{
		{Timestamp: baseTime, ReadMBps: 100.0, WriteMBps: 50.0},
		{Timestamp: baseTime.Add(1 * time.Hour), ReadMBps: 150.0, WriteMBps: 75.0},
		{Timestamp: baseTime.Add(2 * time.Hour), ReadMBps: 200.0, WriteMBps: 100.0},
		{Timestamp: baseTime.Add(3 * time.Hour), ReadMBps: 250.0, WriteMBps: 125.0},
	}

	cpuDS, err := NewStaticStructDatasource(stream.Just(cpuData...))
	require.NoError(t, err)

	memDS, err := NewStaticStructDatasource(stream.Just(memoryData...))
	require.NoError(t, err)

	diskDS, err := NewStaticStructDatasource(stream.Just(diskData...))
	require.NoError(t, err)

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS, diskDS).MustCollect()),
		InnerJoin,
	)

	ctx := context.Background()

	// Query for hours 1 to 3 (exclusive end)
	result, err := joinDS.Execute(ctx, baseTime.Add(1*time.Hour), baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	require.Len(t, records, 2) // Hours 1 and 2

	assert.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)
	assert.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
}

func TestJoinDatasource_LeftJoin_RequiredFields_MarkOptional(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create datasources with required=true fields to test the fix.
	// This simulates real datasources (e.g. aggregation) that produce required fields.
	cpuMeta, err := tsquery.NewFieldMeta("cpu:usage", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	memMeta, err := tsquery.NewFieldMeta("mem:used", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	cpuDS := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*cpuMeta},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{10.0}},
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{20.0}},
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{30.0}},
		),
	)}

	memDS := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*memMeta},
		stream.Just(
			// Only present at hour 1 — hour 0 and hour 2 will have nil
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{1024.0}},
		),
	)}

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS).MustCollect()),
		LeftJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Left-side field should remain required
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.True(t, fieldsMeta[0].Required(), "left-side field should stay required")
	assert.False(t, fieldsMeta[1].Required(), "right-side field should be marked optional after left join")

	records := result.Stream().MustCollect()
	require.Len(t, records, 3)

	// Hour 0: CPU only, memory is nil
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Nil(t, records[0].Value[1])

	// Hour 1: both present
	assert.Equal(t, 20.0, records[1].Value[0])
	assert.Equal(t, 1024.0, records[1].Value[1])

	// Hour 2: CPU only, memory is nil
	assert.Equal(t, 30.0, records[2].Value[0])
	assert.Nil(t, records[2].Value[1])
}

func TestJoinDatasource_LeftJoin_NumericExpressionWithNil_NoPanic(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Use required=true fields to reproduce the panic scenario from the bug report.
	cpuMeta, err := tsquery.NewFieldMeta("cpu:usage", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	memMeta, err := tsquery.NewFieldMeta("mem:used", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	cpuDS := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*cpuMeta},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{10.0}},
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{20.0}},
		),
	)}

	memDS := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*memMeta},
		stream.Just(
			// Only present at hour 1 — hour 0 will produce nil
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{1024.0}},
		),
	)}

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](cpuDS, memDS).MustCollect()),
		LeftJoin,
	)

	ctx := context.Background()
	joinResult, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Add a computed field: cpu:usage + mem:used
	// Before the fix, this would panic at hour 0 because mem:used is nil
	// but its metadata incorrectly says Required=true, skipping nil-wrapping.
	appendFilter := NewAppendFieldFilter(
		NewNumericExpressionFieldValue(
			NewRefFieldValue("cpu:usage"),
			tsquery.BinaryNumericOperatorAdd,
			NewRefFieldValue("mem:used"),
		),
		tsquery.AddFieldMeta{Urn: "total"},
	)

	filteredResult, err := appendFilter.Filter(ctx, joinResult)
	require.NoError(t, err)

	// Should not panic — this is the key assertion
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2)

	// Hour 0: mem:used is nil → computed field should be nil
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Nil(t, records[0].Value[1])
	assert.Nil(t, records[0].Value[2], "computed field should be nil when right-side operand is nil")

	// Hour 1: both present → computed field should be 20.0 + 1024.0 = 1044.0
	assert.Equal(t, 20.0, records[1].Value[0])
	assert.Equal(t, 1024.0, records[1].Value[1])
	assert.Equal(t, 1044.0, records[1].Value[2])
}

func TestJoinDatasource_FullJoin_NumericExpressionWithNil_NoPanic(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Use required=true fields to reproduce the panic scenario with full join.
	// Both sides can have nil values in a full join.
	aMeta, err := tsquery.NewFieldMeta("a:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	bMeta, err := tsquery.NewFieldMeta("b:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	ds1 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*aMeta},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{10.0}},
			// hour 1: absent from ds1
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{30.0}},
		),
	)}

	ds2 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*bMeta},
		stream.Just(
			// hour 0: absent from ds2
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{2.0}},
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{3.0}},
		),
	)}

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](ds1, ds2).MustCollect()),
		FullJoin,
	)

	ctx := context.Background()
	joinResult, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Add a computed field: a:val + b:val
	// Before the fix, this would panic when either side is nil
	// but its metadata incorrectly says Required=true, skipping nil-wrapping.
	appendFilter := NewAppendFieldFilter(
		NewNumericExpressionFieldValue(
			NewRefFieldValue("a:val"),
			tsquery.BinaryNumericOperatorAdd,
			NewRefFieldValue("b:val"),
		),
		tsquery.AddFieldMeta{Urn: "total"},
	)

	filteredResult, err := appendFilter.Filter(ctx, joinResult)
	require.NoError(t, err)

	// Should not panic — this is the key assertion
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3)

	// Hour 0: b:val is nil → computed field should be nil
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Nil(t, records[0].Value[1])
	assert.Nil(t, records[0].Value[2], "computed field should be nil when right-side operand is nil")

	// Hour 1: a:val is nil → computed field should be nil
	assert.Nil(t, records[1].Value[0])
	assert.Equal(t, 2.0, records[1].Value[1])
	assert.Nil(t, records[1].Value[2], "computed field should be nil when left-side operand is nil")

	// Hour 2: both present → computed field should be 30.0 + 3.0 = 33.0
	assert.Equal(t, 30.0, records[2].Value[0])
	assert.Equal(t, 3.0, records[2].Value[1])
	assert.Equal(t, 33.0, records[2].Value[2])
}

func TestJoinDatasource_FullJoin_RequiredFields_AllOptional(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Both datasources have required=true fields
	f1, err := tsquery.NewFieldMeta("a:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)
	f2, err := tsquery.NewFieldMeta("b:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	ds1 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*f1},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{1.0}},
		),
	)}
	ds2 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*f2},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{2.0}},
		),
	)}

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](ds1, ds2).MustCollect()),
		FullJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// Full join: ALL fields must be optional, even those originally required
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.False(t, fieldsMeta[0].Required(), "first field should be optional after full join")
	assert.False(t, fieldsMeta[1].Required(), "second field should be optional after full join")

	records := result.Stream().MustCollect()
	require.Len(t, records, 2)

	// Hour 0: only ds1 has data
	assert.Equal(t, 1.0, records[0].Value[0])
	assert.Nil(t, records[0].Value[1])

	// Hour 1: only ds2 has data
	assert.Nil(t, records[1].Value[0])
	assert.Equal(t, 2.0, records[1].Value[1])
}

func TestJoinDatasource_InnerJoin_RequiredFields_Unchanged(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Verify that InnerJoin does NOT change required status
	f1, err := tsquery.NewFieldMeta("a:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)
	f2, err := tsquery.NewFieldMeta("b:val", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	ds1 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*f1},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{1.0}},
		),
	)}
	ds2 := staticResultDatasource{result: NewResult(
		[]tsquery.FieldMeta{*f2},
		stream.Just(
			timeseries.TsRecord[[]any]{Timestamp: baseTime, Value: []any{2.0}},
		),
	)}

	joinDS := NewJoinDatasource(
		NewListMultiDatasource(stream.Just[DataSource](ds1, ds2).MustCollect()),
		InnerJoin,
	)

	ctx := context.Background()
	result, err := joinDS.Execute(ctx, baseTime, baseTime.Add(10*time.Hour))
	require.NoError(t, err)

	// InnerJoin guarantees all fields are present — required should be preserved
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.True(t, fieldsMeta[0].Required(), "inner join should preserve required=true")
	assert.True(t, fieldsMeta[1].Required(), "inner join should preserve required=true")
}

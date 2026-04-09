package tsquery

import "fmt"

// MetricKind describes the temporal semantics of a metric's values.
type MetricKind string

const (
	// MetricKindGauge represents an instantaneous measurement (e.g., temperature, CPU%).
	MetricKindGauge MetricKind = "gauge"

	// MetricKindDelta represents a change since the last data point (e.g., energy consumed in interval).
	MetricKindDelta MetricKind = "delta"

	// MetricKindCumulative represents a monotonically increasing total (e.g., lifetime energy, total request count).
	MetricKindCumulative MetricKind = "cumulative"

	// MetricKindRate represents a derived rate of change (e.g., power in kW, requests/second).
	MetricKindRate MetricKind = "rate"
)

func (mk MetricKind) Validate() error {
	switch mk {
	case "", MetricKindGauge, MetricKindDelta, MetricKindCumulative, MetricKindRate:
		return nil
	}
	return fmt.Errorf("invalid metric kind: %q", mk)
}

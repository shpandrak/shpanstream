package timeseries

type FillMode string

const (
	FillModeLinear      FillMode = "linear"
	FillModeForwardFill FillMode = "forwardFill"
)

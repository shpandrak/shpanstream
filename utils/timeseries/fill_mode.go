package timeseries

import "fmt"

type FillMode string

const (
	FillModeLinear      FillMode = "linear"
	FillModeForwardFill FillMode = "forwardFill"
)

// Validate reports whether the fill mode is one of the supported modes.
func (fm FillMode) Validate() error {
	switch fm {
	case FillModeLinear, FillModeForwardFill:
		return nil
	}
	return fmt.Errorf("invalid fill mode: %q", fm)
}

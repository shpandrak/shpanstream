package tsquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateData_Timestamp(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	require.NoError(t, DataTypeTimestamp.ValidateData(ts))
}

func TestValidateData_Timestamp_WrongType(t *testing.T) {
	err := DataTypeTimestamp.ValidateData("not a timestamp")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected timestamp")
}

func TestValidateData_Timestamp_Int(t *testing.T) {
	err := DataTypeTimestamp.ValidateData(int64(123))
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected timestamp")
}

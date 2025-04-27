package timeseries

import (
	"fmt"
	"github.com/shpandrak/shpanstream"
)

func DeltaStream[N Number](s shpanstream.Stream[TsRecord[N]]) shpanstream.Stream[TsRecord[N]] {
	var prevItem *TsRecord[N]
	return shpanstream.MapStreamWhileFilteringWithErr(
		s,
		func(item TsRecord[N]) (*TsRecord[N], error) {
			if prevItem == nil {
				prevItem = &item
				return nil, nil
			} else {
				if !item.Timestamp.After(prevItem.Timestamp) {
					return nil, fmt.Errorf("item timestamp %s is not after previous item timestamp %s", item.Timestamp, prevItem.Timestamp)
				}
				ret := &TsRecord[N]{
					Value:     item.Value - prevItem.Value,
					Timestamp: item.Timestamp,
				}
				prevItem = &item
				return ret, nil
			}
		},
	)
}

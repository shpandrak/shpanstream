package main

import (
	"fmt"
	"github.com/shpandrak/shpanstream/integrations/file"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"strconv"
	"strings"
	"time"
)

/*
This example shows how to use the shpanstream library to read a huge CSV file and calculate data with minimal memory usage.

To use this example, you need to download the hourly temperature data from:
https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/global/time-series
and save it as "examples/globalwarming/huge_time_series_csv.csv".
File expected format:
2020-01-01T00:00,0.1
2020-01-01T00:01,0.2
...
The example will read the file and print the average temperature for each year ascending.
*/
func main() {

	locationToUse := time.Local
	samplesStream := stream.MapStreamWithErr(
		file.StreamFromFile("examples/globalwarming/huge_time_series_csv.csv", false),
		parseCsvLine,
	).Filter(func(src timeseries.TsRecord[float64]) bool {
		// Filter current year, since it is not complete...
		return src.Timestamp.In(locationToUse).Year() < time.Now().In(locationToUse).Year()
	})

	for curr := range timeseries.AlignReduceStream(
		samplesStream,
		timeseries.NewYearAlignmentPeriod(locationToUse),
		timeseries.Avg[float64],
	).
		Iterator {
		fmt.Println(curr)
	}
}

func parseCsvLine(line []byte) (timeseries.TsRecord[float64], error) {
	const timeFieldLayout = "2006-01-02T15:04"

	// Split the line by comma
	parts := strings.Split(string(line), ",")
	if len(parts) != 2 {
		return util.DefaultValue[timeseries.TsRecord[float64]](), fmt.Errorf("invalid line: %s", string(line))
	}
	// Parse the time and value
	sampleTime, err := time.Parse(timeFieldLayout, parts[0])
	if err != nil {
		return util.DefaultValue[timeseries.TsRecord[float64]](), fmt.Errorf("invalid time: %s", parts[0])
	}
	sampleValue, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return util.DefaultValue[timeseries.TsRecord[float64]](), fmt.Errorf("invalid value: %s", parts[1])
	}
	// Return the TsRecord
	return timeseries.TsRecord[float64]{
		Timestamp: sampleTime,
		Value:     sampleValue,
	}, nil
}

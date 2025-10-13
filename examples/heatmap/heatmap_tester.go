package main

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/integrations/file"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/jsonstream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {

	f, err := os.Create("examples/heatmap/heatmap.html")
	if err != nil {
		log.Fatalf("failed to open output file: %v", err)
	}
	defer f.Close()

	err = writeHtmlPrefix(f)
	if err != nil {
		log.Fatalf("failed to write header to output file: %v", err)
	}

	locationToUse := time.Local
	err = jsonstream.StreamJsonToWriter(
		context.Background(),
		f,
		stream.ConcatStreams(

			// Adding a header to the output json to be used by the ECharts library
			stream.Just[[]any]([]any{"time", "temperature"}),
			stream.Map(

				// Aligning the data to the year period
				timeseries.AlignReduceStream(
					stream.MapWithErr(
						file.StreamFromFile("examples/globalwarming/huge_time_series_csv.csv", false),
						parseCsvLine,
					).
						// Filter current year, since it is not complete and yearly average is not relevant
						Filter(func(src timeseries.TsRecord[float64]) bool {
							return src.Timestamp.In(locationToUse).Year() < time.Now().In(locationToUse).Year()
						}),

					// Create a datapoint for each year
					timeseries.NewYearAlignmentPeriod(locationToUse),

					// Calculate the average temperature for each year
					timeseries.Avg[float64],
				),

				// Format the data to be used by ECharts
				func(src timeseries.TsRecord[float64]) []any {
					return []any{src.Timestamp, src.Value}
				},
			),
		),
	)

	err = writeHtmlSuffix(err, f)
	if err != nil {
		log.Fatalf("failed to write footer to output file: %v", err)
	}
}

func writeHtmlSuffix(err error, f *os.File) error {
	_, err = f.WriteString(`
        },
        tooltip: { trigger: 'axis' },
        legend: {},
        xAxis: { type: 'time' },  // or 'time' if you're using real timestamps
        yAxis: {},
		dataZoom: [
		  {
			type: 'inside'  // zoom with scroll wheel or pinch
		  },
		  {
			type: 'slider'  // zoom with visible slider
		  }
		],
        series: [
            { type: 'line', encode: { x: 'time', y: 'temperature' }, name: 'Temperature' },
        ]
    };

    chart.setOption(option);
</script>
</body>
</html>
`)
	return err
}

func writeHtmlPrefix(f *os.File) error {
	_, err := f.WriteString(
		`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>ECharts Dataset Example</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js"></script>
    <style>
        html, body, #chart {
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
        }
    </style>
</head>
<body>
<div id="chart"></div>
<script>
    const chart = echarts.init(document.getElementById('chart'));

    const option = {
        dataset: {
            source: 
				`)
	return err
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

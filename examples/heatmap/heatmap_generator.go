package main

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
)

type reportObject struct {
	Name string
}

type tsResult[N timeseries.Number] struct {
	Record       stream.Stream[timeseries.TsRecord[N]]
	reportObject reportObject
}

type tsDataset[N timeseries.Number] struct {
	Results stream.Stream[tsResult[N]]
}

func GenerateHeatmap[N timeseries.Number](dataset tsDataset[N]) {

}

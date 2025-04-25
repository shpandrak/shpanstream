package file

import (
	"fmt"
	"github.com/shpandrak/shpanstream"
	"strconv"
	"strings"
)

func ExampleStreamProvider() {

	type characterInfo struct {
		Name   string
		Height float64
	}
	getName := func(c characterInfo) string {
		return c.Name
	}

	lineParser := func(line []byte) (characterInfo, error) {
		var ret characterInfo
		// Split the line by comma
		parts := strings.Split(string(line), ",")
		if len(parts) != 2 {
			return ret, fmt.Errorf("invalid line: %s", string(line))
		}
		if parts[0] == "" {
			return ret, fmt.Errorf("invalid line: %s", string(line))
		}
		ret.Name = parts[0]

		// Parse height
		height, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return ret, fmt.Errorf("invalid height: %s", parts[1])
		}
		ret.Height = height
		return ret, nil
	}

	tallestXmenName := shpanstream.MapLazy(
		shpanstream.ReduceLazy(
			shpanstream.MapStreamWithErr(
				StreamFromFile("xmen-heights.csv", false).
					// Skip the header
					Skip(1),
				lineParser,
			),
			characterInfo{Name: "None", Height: 0},
			func(acc characterInfo, curr characterInfo) characterInfo {
				if curr.Height > acc.Height {
					return curr
				}
				return acc
			}),
		getName,
	)

	// Output: Colossus
	fmt.Println(
		tallestXmenName.MustGet(),
	)
}

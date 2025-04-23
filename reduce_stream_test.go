package shpanstream

import "fmt"

func ExampleReduce() {
	sum := MustReduce(
		Just(2, 4, 6),
		0,
		func(acc, v int) int {
			return acc + v
		},
	)

	// Output: 12
	fmt.Println(sum)
}

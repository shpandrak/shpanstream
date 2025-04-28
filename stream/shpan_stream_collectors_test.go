package stream

import (
	"context"
	"fmt"
)

func ExampleCollectToMap() {
	ctx := context.Background()
	result, err := CollectToMap(
		ctx,
		Just(1, 2, 3),
		func(v int) (int, string) {

			return v, fmt.Sprintf("value %d", v)
		},
	)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Result:", result)
	}
	// Output: Result: map[1:value 1 2:value 2 3:value 3]
}

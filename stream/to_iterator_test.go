package stream

import (
	"fmt"
)

func ExampleStream_Iterator() {
	msg := "using Iter:"
	for curr := range Just(1, 1, 2, 3, 5, 8, 13, 21, 34, 55).Iterator {
		if curr == 21 {
			break
		}
		msg += fmt.Sprintf(" %d", curr)
	}

	// Output: using Iter: 1 1 2 3 5 8 13
	fmt.Println(msg)

}

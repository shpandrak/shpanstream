package stream

import (
	"fmt"
)

func ExampleMustValues() {
	msg := "using MustValues:"
	for curr := range MustValues(Just(1, 1, 2, 3, 5, 8, 13, 21, 34, 55)) {
		if curr == 21 {
			break
		}
		msg += fmt.Sprintf(" %d", curr)
	}
	fmt.Println(msg)

	// Output: using MustValues: 1 1 2 3 5 8 13
}

func ExampleMustAll() {
	for i, name := range MustAll(Just("a", "b", "c")) {
		fmt.Println(i, name)
	}

	// Output:
	// 0 a
	// 1 b
	// 2 c
}

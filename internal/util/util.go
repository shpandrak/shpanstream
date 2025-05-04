package util

func DefaultValue[T any]() T {
	var ret T
	return ret
}

func Identity[T any]() func(v T) T {
	return func(v T) T {
		return v
	}
}

package util

import "fmt"

func DefaultValue[T any]() T {
	var ret T
	return ret
}

func Identity[T any]() func(v T) T {
	return func(v T) T {
		return v
	}
}

func Pointer[T any](v T) *T {
	return &v
}

func WrapAndReturn[T any](v T, err error) func(format string, a ...any) (T, error) {
	return func(format string, a ...any) (T, error) {
		if err != nil {
			return v, fmt.Errorf(fmt.Sprintf(format, a...)+": %w", err)
		}
		return v, nil
	}
}

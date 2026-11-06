package user

import "context"

// This file never names the stream package, so the rewrite has to import it.
func NoImport(ctx context.Context) (int, error) {
	return people().Count(ctx)
}

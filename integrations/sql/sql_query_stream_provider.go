package sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"io"
)

func StreamSqlQuery[T any](
	dbProvider func() (*sql.DB, error),
	query string,
	paramVals []any,
	scanner func(*sql.Rows) (T, error),
) stream.Stream[T] {
	db, err := dbProvider()
	if err != nil {
		return stream.Error[T](fmt.Errorf("failed to get db for sql query stream: %w", err))
	}
	return stream.NewStream(&sqlQueryStreamProvider[T]{
		db:        db,
		query:     query,
		paramVals: paramVals,
		scanner:   scanner,
	})

}

type sqlQueryStreamProvider[T any] struct {
	db        *sql.DB
	query     string
	paramVals []any
	rows      *sql.Rows
	scanner   func(*sql.Rows) (T, error)
}

func (s *sqlQueryStreamProvider[T]) Open(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		s.query,
		s.paramVals...,
	)
	if err != nil {
		return fmt.Errorf("failed opening sql query stream: %w", err)
	}
	s.rows = rows
	return nil

}

func (s *sqlQueryStreamProvider[T]) Close() {
	if s.rows != nil {
		_ = s.rows.Close()
	}
}

func (s *sqlQueryStreamProvider[T]) Emit(ctx context.Context) (T, error) {
	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	next := s.rows.Next()
	if !next {
		if err := s.rows.Err(); err != nil {
			return util.DefaultValue[T](), fmt.Errorf("error reading from sql query stream: %w", err)
		}
		return util.DefaultValue[T](), io.EOF
	}
	return s.scanner(s.rows)
}

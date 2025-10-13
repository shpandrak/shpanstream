package sql

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStreamSqlQuery(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS my_table (
			id TEXT PRIMARY KEY,
			name TEXT,
			timestamp TIMESTAMP
		)`)
	require.NoError(t, err)

	initialTime := time.Date(1981, 3, 4, 13, 30, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		_, err = db.Exec(`
			INSERT INTO my_table (id, name, timestamp)
			VALUES (?, ?, ?)`,
			fmt.Sprintf("%d", i),
			fmt.Sprintf("Name %d", i),
			initialTime.Add(time.Duration(i)*time.Minute),
		)
	}
	type myRow struct {
		id        string
		name      string
		timestamp time.Time
	}
	require.Equal(t, 100,
		StreamSqlQuery[myRow](func() (*sql.DB, error) {
			return db, nil
		}, "SELECT id, name, timestamp FROM my_table", nil, func(rows *sql.Rows) (myRow, error) {
			var row myRow
			err := rows.Scan(&row.id, &row.name, &row.timestamp)
			if err != nil {
				return util.DefaultValue[myRow](), fmt.Errorf("failed scanning row: %w", err)
			}
			return row, nil
		}).MustCount())

	require.Equal(t, 1, 1)
}

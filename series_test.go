package vtab_test

import (
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/augmentable-dev/vtab"
	_ "github.com/augmentable-dev/vtab/pkg/sqlite"
	_ "github.com/mattn/go-sqlite3"
	"go.riyazali.net/sqlite"
)

type seriesIter struct {
	current int
	start   int
	stop    int
	step    int
}

func (i *seriesIter) Column(c int) (interface{}, error) {
	switch c {
	case 0:
		return i.current, nil
	case 1:
		return i.start, nil
	case 2:
		return i.stop, nil
	case 3:
		return i.step, nil
	}

	return nil, fmt.Errorf("unknown column")
}

func (i *seriesIter) Next() (vtab.Row, error) {
	i.current += i.step
	if i.current > i.stop {
		return nil, io.EOF
	}
	return i, nil
}

func TestSeries(t *testing.T) {
	cols := []vtab.Column{
		{"value", sqlite.SQLITE_INTEGER, false, false, nil},
		{"start", sqlite.SQLITE_INTEGER, false, true, []sqlite.ConstraintOp{sqlite.INDEX_CONSTRAINT_EQ}},
		{"stop", sqlite.SQLITE_INTEGER, false, true, []sqlite.ConstraintOp{sqlite.INDEX_CONSTRAINT_EQ}},
		{"step", sqlite.SQLITE_INTEGER, false, true, []sqlite.ConstraintOp{sqlite.INDEX_CONSTRAINT_EQ}},
	}
	m := vtab.NewTableFunc("series", cols, func(constraints []vtab.Constraint) (vtab.Iterator, error) {
		// defaults
		start := 0
		stop := 100
		step := 1

		// override defaults based on any equality constraints (arguments to the table valued func)
		for _, constraint := range constraints {
			if constraint.Op == sqlite.INDEX_CONSTRAINT_EQ {
				switch constraint.ColIndex {
				case 1:
					start = constraint.Value.Int()
				case 2:
					stop = constraint.Value.Int()
				case 3:
					step = constraint.Value.Int()
				}
			}
		}

		return &seriesIter{start, start, stop, step}, nil
	})

	sqlite.Register(func(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
		if err := api.CreateModule("series", m,
			sqlite.EponymousOnly(true),
			sqlite.ReadOnly(true)); err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		return sqlite.SQLITE_OK, nil
	})

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// TODO edit this query to see different results
	rows, err := db.Query("select * from series(50, 200, 10)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	_, contents, err := GetContents(rows)
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range contents {
		fmt.Println(r)
	}

	err = rows.Err()
	if err != nil {
		t.Fatal(err)
	}
}

func GetContents(rows *sql.Rows) (int, [][]string, error) {
	count := 0
	columns, err := rows.Columns()
	if err != nil {
		return count, nil, err
	}

	pointers := make([]interface{}, len(columns))
	container := make([]sql.NullString, len(columns))
	var ret [][]string

	for i := range pointers {
		pointers[i] = &container[i]
	}

	for rows.Next() {
		err = rows.Scan(pointers...)
		if err != nil {
			return count, nil, err
		}

		r := make([]string, len(columns))
		for i, c := range container {
			if c.Valid {
				r[i] = c.String
			} else {
				r[i] = "NULL"
			}
		}
		ret = append(ret, r)
	}
	return count, ret, err

}

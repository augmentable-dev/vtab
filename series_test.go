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
	order   vtab.Orders
}

func (i *seriesIter) Column(ctx *sqlite.Context, c int) error {
	switch c {
	case 0:
		ctx.ResultInt(i.current)
	case 1:
		ctx.ResultInt(i.start)
	case 2:
		ctx.ResultInt(i.stop)
	case 3:
		ctx.ResultInt(i.step)
	default:
		return fmt.Errorf("unknown column")
	}

	return nil
}

func (i *seriesIter) Next() (vtab.Row, error) {
	switch i.order {
	case vtab.ASC:
		i.current += i.step
		if i.current > i.stop {
			return nil, io.EOF
		}
	case vtab.DESC:
		i.current -= i.step
		if i.current < i.start {
			return nil, io.EOF
		}
	}

	return i, nil
}

func TestSeries(t *testing.T) {
	cols := []vtab.Column{
		{Name: "value", Type: sqlite.SQLITE_INTEGER, OrderBy: vtab.ASC | vtab.DESC},
		{Name: "start", Type: sqlite.SQLITE_INTEGER, Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
		{Name: "stop", Type: sqlite.SQLITE_INTEGER, Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
		{Name: "step", Type: sqlite.SQLITE_INTEGER, Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
	}
	m := vtab.NewTableFunc("series", cols, func(constraints []*vtab.Constraint, order []*sqlite.OrderBy) (vtab.Iterator, error) {
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

		// by default, current is the starting value and the order is ascending
		current := start
		valueOrder := vtab.ASC

		// if the query wants the series ordered by value in reverse (desc)
		// tell the implementation to iterate in reverse
		if len(order) == 1 && order[0].ColumnIndex == 0 {
			if order[0].Desc {
				valueOrder = vtab.DESC
				current = stop
			}
		}

		return &seriesIter{current, start, stop, step, valueOrder}, nil
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
	rows, err := db.Query("select * from series(50, 200, 50) order by value desc")
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

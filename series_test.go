package vtab_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/augmentable-dev/vtab"
	_ "github.com/augmentable-dev/vtab/pkg/sqlite"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"go.riyazali.net/sqlite"
)

var totalIterations int

type seriesIter struct {
	current         int
	start           int
	stop            int
	step            int
	order           vtab.Orders
	totalIterations int
}

func (i *seriesIter) Column(ctx vtab.Context, c int) error {
	switch cols[c].Name {
	case "value":
		ctx.ResultInt(i.current)
	case "start":
		ctx.ResultInt(i.start)
	case "stop":
		ctx.ResultInt(i.stop)
	case "step":
		ctx.ResultInt(i.step)
	default:
		return fmt.Errorf("unknown column")
	}

	return nil
}

func (i *seriesIter) Next() (vtab.Row, error) {
	i.totalIterations++
	totalIterations++
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

var cols = []vtab.Column{
	{Name: "value", Type: "INTEGER", OrderBy: vtab.ASC | vtab.DESC, Filters: []*vtab.ColumnFilter{
		{Op: sqlite.INDEX_CONSTRAINT_GT}, {Op: sqlite.INDEX_CONSTRAINT_GE},
		{Op: sqlite.INDEX_CONSTRAINT_LT}, {Op: sqlite.INDEX_CONSTRAINT_LE},
	}},
	{Name: "start", Type: "INTEGER", Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
	{Name: "stop", Type: "INTEGER", Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
	{Name: "step", Type: "INTEGER", Hidden: true, Filters: []*vtab.ColumnFilter{{Op: sqlite.INDEX_CONSTRAINT_EQ}}},
}

func init() {
	m := vtab.NewTableFunc("series", cols, func(constraints []*vtab.Constraint, order []*sqlite.OrderBy) (vtab.Iterator, error) {
		// defaults
		start := 0
		stop := 100
		step := 1

		// override defaults based on any equality constraints (arguments to the table valued func)
		for _, constraint := range constraints {
			if constraint.Op == sqlite.INDEX_CONSTRAINT_EQ {
				switch cols[constraint.ColIndex].Name {
				case "start":
					start = constraint.Value.Int()
				case "stop":
					stop = constraint.Value.Int()
				case "step":
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

		return &seriesIter{current, start, stop, step, valueOrder, 0}, nil
	}, vtab.EarlyOrderByConstraintExit(true))

	sqlite.Register(func(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
		if err := api.CreateModule("series", m,
			sqlite.EponymousOnly(true),
			sqlite.ReadOnly(true)); err != nil {
			return sqlite.SQLITE_ERROR, err
		}
		return sqlite.SQLITE_OK, nil
	})
}

func TestSeriesSimple(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	db.Select(&contents, "select value from series(0, 100, 1)")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 100, len(contents))
	assert.Equal(t, 101, iterations)
	assert.Equal(t, "1", contents[0])
	assert.Equal(t, "2", contents[1])
	assert.Equal(t, "99", contents[98])
	assert.Equal(t, "100", contents[99])
}

func TestSeriesDescGT(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	db.Select(&contents, "select value from series(0, 100, 1) where value > 50 order by value desc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 49, len(contents))
	assert.Equal(t, 50, iterations)
	assert.Equal(t, "99", contents[0])
	assert.Equal(t, "98", contents[1])
	assert.Equal(t, "52", contents[47])
	assert.Equal(t, "51", contents[48])
}

func TestSeriesDescGTE(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	db.Select(&contents, "select value from series(0, 100, 1) where value >= 50 order by value desc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 50, len(contents))
	assert.Equal(t, 51, iterations)
	assert.Equal(t, "99", contents[0])
	assert.Equal(t, "98", contents[1])
	assert.Equal(t, "51", contents[48])
	assert.Equal(t, "50", contents[49])
}

func TestSeriesAscLT(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	db.Select(&contents, "select value from series(0, 100, 1) where value < 50 order by value asc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 49, len(contents))
	assert.Equal(t, 50, iterations)
	assert.Equal(t, "1", contents[0])
	assert.Equal(t, "2", contents[1])
	assert.Equal(t, "48", contents[47])
	assert.Equal(t, "49", contents[48])
}

func TestSeriesAscLE(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	db.Select(&contents, "select value from series(0, 100, 1) where value <= 50 order by value asc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 50, len(contents))
	assert.Equal(t, 51, iterations)
	assert.Equal(t, "1", contents[0])
	assert.Equal(t, "2", contents[1])
	assert.Equal(t, "49", contents[48])
	assert.Equal(t, "50", contents[49])
}

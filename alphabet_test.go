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

var alphabet = []rune("abcdefghijklmnopqrstuvwxyz")

type alphabetIter struct {
	current int
	desc    bool
}

func (i *alphabetIter) Column(ctx vtab.Context, c int) error {
	switch alphabetIterCols[c].Name {
	case "letter":
		ctx.ResultText(string(alphabet[i.current]))
	default:
		return fmt.Errorf("unknown column")
	}

	return nil
}

func (i *alphabetIter) Next() (vtab.Row, error) {
	totalIterations++
	if i.desc {
		i.current--
		if i.current <= -1 {
			return nil, io.EOF
		}
	} else {
		i.current++
		if i.current >= len(alphabet) {
			return nil, io.EOF
		}
	}

	return i, nil
}

var alphabetIterCols = []vtab.Column{
	{Name: "letter", Type: "TEXT", OrderBy: vtab.ASC | vtab.DESC, Filters: []*vtab.ColumnFilter{
		{Op: sqlite.INDEX_CONSTRAINT_GT}, {Op: sqlite.INDEX_CONSTRAINT_GE},
		{Op: sqlite.INDEX_CONSTRAINT_LT}, {Op: sqlite.INDEX_CONSTRAINT_LE},
	}},
}

var alphabetModule = vtab.NewTableFunc("alphabet", alphabetIterCols, func(constraints []*vtab.Constraint, order []*sqlite.OrderBy) (vtab.Iterator, error) {
	var desc bool
	current := -1
	if len(order) == 1 && order[0].ColumnIndex == 0 {
		if order[0].Desc {
			desc = true
			current = len(alphabet)
		}
	}

	return &alphabetIter{current, desc}, nil
}, vtab.EarlyOrderByConstraintExit(true))

func TestAlphabetSimple(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	err = db.Select(&contents, "select * from alphabet")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	assert.Equal(t, 26, len(contents))
	assert.Equal(t, 27, iterations)
	assert.Equal(t, "a", contents[0])
	assert.Equal(t, "b", contents[1])
	assert.Equal(t, "y", contents[24])
	assert.Equal(t, "z", contents[25])
}

func TestAlphabetDescGT(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	err = db.Select(&contents, "select * from alphabet where letter > 'm' order by letter desc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	t.Log(contents)

	assert.Equal(t, 13, len(contents))
	assert.Equal(t, 14, iterations)
	assert.Equal(t, "z", contents[0])
	assert.Equal(t, "y", contents[1])
	assert.Equal(t, "o", contents[11])
	assert.Equal(t, "n", contents[12])
}

func TestAlphabetDescGTE(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	err = db.Select(&contents, "select * from alphabet where letter >= 'm' order by letter desc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	t.Log(contents)

	assert.Equal(t, 14, len(contents))
	assert.Equal(t, 15, iterations)
	assert.Equal(t, "z", contents[0])
	assert.Equal(t, "y", contents[1])
	assert.Equal(t, "n", contents[12])
	assert.Equal(t, "m", contents[13])
}

func TestAlphabetAscLT(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	err = db.Select(&contents, "select * from alphabet where letter < 'm' order by letter asc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	t.Log(contents)

	assert.Equal(t, 12, len(contents))
	assert.Equal(t, 13, iterations)
	assert.Equal(t, "a", contents[0])
	assert.Equal(t, "b", contents[1])
	assert.Equal(t, "k", contents[10])
	assert.Equal(t, "l", contents[11])
}

func TestAlphabetAscLE(t *testing.T) {
	db, err := sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	totalBefore := totalIterations

	var contents []string
	err = db.Select(&contents, "select * from alphabet where letter <= 'm' order by letter asc")
	if err != nil {
		t.Fatal(err)
	}

	iterations := totalIterations - totalBefore

	t.Log((contents))

	assert.Equal(t, 13, len(contents))
	assert.Equal(t, 14, iterations)
	assert.Equal(t, "a", contents[0])
	assert.Equal(t, "b", contents[1])
	assert.Equal(t, "l", contents[11])
	assert.Equal(t, "m", contents[12])
}

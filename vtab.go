package vtab

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/template"

	"go.riyazali.net/sqlite"
)

type Orders uint8

const (
	NONE Orders = iota
	DESC
	ASC
)

type ColumnFilter struct {
	Op        sqlite.ConstraintOp
	OmitCheck bool
}

type Column struct {
	Name    string
	Type    string
	NotNull bool
	Hidden  bool
	Filters []*ColumnFilter
	OrderBy Orders
}

type Constraint struct {
	ColIndex int
	Op       sqlite.ConstraintOp
	Value    *sqlite.Value
}

type GetIteratorFunc func(constraints []*Constraint, order []*sqlite.OrderBy) (Iterator, error)

type options struct {
	earlyOrderByConstraintExit bool
}

type OptFunc func(*options)

// EarlyOrderByConstraintExit tells the table-func to end iteration early, if results are ordered by
// a field that is also in a WHERE clause with one of a >,>=,<,<= that would warrant an early exit.
// This assumes that the column in question has the GT, GE, LT, LE constraints registered.
func EarlyOrderByConstraintExit(value bool) OptFunc {
	return func(opts *options) { opts.earlyOrderByConstraintExit = value }
}

func NewTableFunc(name string, columns []Column, newIterator GetIteratorFunc, opts ...OptFunc) sqlite.Module {
	opt := &options{}
	for _, optFunc := range opts {
		optFunc(opt)
	}
	return &tableFuncModule{name, columns, newIterator, opt}
}

type tableFuncModule struct {
	name        string
	columns     []Column
	getIterator GetIteratorFunc
	options     *options
}

type tableFuncTable struct {
	*tableFuncModule
}

type tableFuncCursor struct {
	*tableFuncTable
	iterator    Iterator
	count       int
	current     Row
	order       []*sqlite.OrderBy
	constraints []*Constraint
}

type Iterator interface {
	Next() (Row, error)
}

type Context interface {
	ResultInt(v int)
	ResultInt64(v int64)
	ResultFloat(v float64)
	ResultNull()
	ResultValue(v sqlite.Value)
	ResultZeroBlob(n int64)
	ResultText(v string)
	ResultError(err error)
	ResultPointer(val interface{})
}

type Row interface {
	Column(ctx Context, col int) error
}

// createTableSQL produces the SQL to declare a new virtual table
func (m *tableFuncModule) createTableSQL() (string, error) {
	// TODO needs to support WITHOUT ROWID, PRIMARY KEY, NOT NULL
	const declare = `CREATE TABLE {{ .Name }} (
  {{- range $c, $col := .Columns }}
    {{ .Name }} {{ .Type }}{{ if .Hidden }} HIDDEN{{ end }}{{ if columnComma $c }},{{ end }}
  {{- end }}
)`

	// helper to determine whether we're on the last column (and therefore should avoid a comma ",") in the range
	fns := template.FuncMap{
		"columnComma": func(c int) bool {
			return c < len(m.columns)-1
		},
	}
	tmpl, err := template.New(fmt.Sprintf("declare_table_func_%s", m.name)).Funcs(fns).Parse(declare)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	err = tmpl.Execute(buf, struct {
		Name    string
		Columns []Column
	}{
		m.name,
		m.columns,
	})
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (m *tableFuncModule) Connect(_ *sqlite.Conn, _ []string, declare func(string) error) (sqlite.VirtualTable, error) {
	str, err := m.createTableSQL()
	if err != nil {
		return nil, err
	}

	err = declare(str)
	if err != nil {
		return nil, err
	}

	return &tableFuncTable{m}, nil
}

func (m *tableFuncModule) Destroy() error {
	return nil
}

func (t *tableFuncTable) Open() (sqlite.VirtualCursor, error) {
	return &tableFuncCursor{t, nil, 0, nil, nil, nil}, nil
}

type index struct {
	Constraints []*Constraint
	Orders      []*sqlite.OrderBy
}

func (t *tableFuncTable) BestIndex(input *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	// start with a relatively high cost
	cost := 1000.0
	usage := make([]*sqlite.ConstraintUsage, len(input.Constraints))
	idx := &index{
		Constraints: make([]*Constraint, 0, len(input.Constraints)),
		Orders:      make([]*sqlite.OrderBy, 0),
	}

	orderByUsed := true
	for _, order := range input.OrderBy {
		col := t.columns[order.ColumnIndex]
		if col.OrderBy&ASC != 0 && !order.Desc {
			idx.Orders = append(idx.Orders, order)
			continue
		}
		if col.OrderBy&DESC != 0 && order.Desc {
			idx.Orders = append(idx.Orders, order)
			continue
		}
		orderByUsed = false
	}

	// iterate over constraints
	for cst, constraint := range input.Constraints {
		usage[cst] = &sqlite.ConstraintUsage{}

		if !constraint.Usable {
			return nil, sqlite.SQLITE_CONSTRAINT
		}

		// iterate over the declared constraints the column supports
		col := t.columns[constraint.ColumnIndex]
		for _, filter := range col.Filters {
			// if there's a match, reduce the cost (to prefer usage of this constraint)
			if filter.Op == constraint.Op {
				cost -= 10
				usage[cst].ArgvIndex = len(idx.Constraints) + 1
				usage[cst].Omit = filter.OmitCheck
				idx.Constraints = append(idx.Constraints, &Constraint{
					ColIndex: constraint.ColumnIndex,
					Op:       filter.Op,
				})
			}
		}
	}

	idxStr, err := json.Marshal(idx)
	if err != nil {
		return nil, err
	}

	return &sqlite.IndexInfoOutput{
		EstimatedCost:   cost,
		IndexString:     string(idxStr),
		ConstraintUsage: usage,
		OrderByConsumed: orderByUsed,
	}, nil
}

func (t *tableFuncTable) Disconnect() error {
	return t.Destroy()
}

func (t *tableFuncTable) Destroy() error { return nil }

func (c *tableFuncCursor) Filter(idxNum int, idxName string, values ...sqlite.Value) error {
	var idx index
	err := json.Unmarshal([]byte(idxName), &idx)
	if err != nil {
		return err
	}

	for c := range idx.Constraints {
		idx.Constraints[c].Value = &values[c]
	}

	c.order = idx.Orders
	c.constraints = idx.Constraints

	iter, err := c.getIterator(idx.Constraints, idx.Orders)
	if err != nil {
		return err
	}
	c.iterator = iter

	row, err := iter.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.current = nil
			return nil
		}
		return err
	}

	c.current = row
	return nil
}

// earlyOrderByConstraintExit determines if there should be an early exit, based on supplied ORDER BYs
// and any of <, >=, <, or <= constraints on corresponding columns
func (c *tableFuncCursor) earlyOrderByConstraintExit() error {
outer:
	for _, order := range c.order {
		for _, constraint := range c.constraints {
			if order.ColumnIndex == constraint.ColIndex {
				// limit := constraint.Value.Blob()

				getter := &valueGetter{}
				err := c.current.Column(getter, constraint.ColIndex)
				if err != nil {
					return err
				}

				var comparison int
				switch v := getter.value.(type) {
				case int:
					limit := constraint.Value.Int()
					switch {
					case v == limit:
						comparison = 0
					case v < limit:
						comparison = -1
					case v > limit:
						comparison = 1
					}
				case int64:
					limit := constraint.Value.Int64()
					switch {
					case v == limit:
						comparison = 0
					case v < limit:
						comparison = -1
					case v > limit:
						comparison = 1
					}
				case string:
					limit := constraint.Value.Text()
					comparison = strings.Compare(v, limit)
					// value = []byte(fmt.Sprintf("%v", getter.value))
				case float64:
					limit := constraint.Value.Float()
					switch {
					case v == limit:
						comparison = 0
					case v < limit:
						comparison = -1
					case v > limit:
						comparison = 1
					}
				case []byte:
					limit := constraint.Value.Blob()
					comparison = bytes.Compare(v, limit)
				default:
					break outer
				}

				switch constraint.Op {
				case sqlite.INDEX_CONSTRAINT_GT:
					if order.Desc {
						if comparison <= 0 {
							c.current = nil
							return nil
						}
					}
				case sqlite.INDEX_CONSTRAINT_GE:
					if order.Desc {
						if comparison < 0 {
							c.current = nil
							return nil
						}
					}
				case sqlite.INDEX_CONSTRAINT_LT:
					if !order.Desc {
						if comparison >= 0 {
							c.current = nil
							return nil
						}
					}
				case sqlite.INDEX_CONSTRAINT_LE:
					if !order.Desc {
						if comparison > 0 {
							c.current = nil
							return nil
						}
					}
				}
			}
		}
	}
	return nil
}

func (c *tableFuncCursor) Next() error {
	defer func() { c.count++ }()
	row, err := c.iterator.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.current = nil
			return nil
		}
		return err
	}
	c.current = row

	if c.tableFuncModule.options.earlyOrderByConstraintExit {
		err := c.earlyOrderByConstraintExit()
		if errors.Is(err, io.EOF) {
			c.current = nil
			return nil
		}
		return err
	}

	return nil
}

func (c *tableFuncCursor) Column(ctx *sqlite.Context, col int) error {
	return c.current.Column(ctx, col)
}

func (c *tableFuncCursor) Eof() bool {
	return c.current == nil
}

func (c *tableFuncCursor) Rowid() (int64, error) {
	return int64(c.count), nil
}

func (c *tableFuncCursor) Close() error {
	return nil
}

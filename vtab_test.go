package vtab

import (
	"testing"
)

func TestCreateTableSQL(t *testing.T) {
	m := &tableFuncModule{
		name: "test_table",
		columns: []Column{
			{Name: "test_one", Type: "TEXT"},
			{Name: "test_two", Type: "INTEGER"},
			{Name: "test_three", Type: "BLOB"},
			{Name: "test_arg", Type: "TEXT", Hidden: true},
		},
	}

	str, err := m.createTableSQL()
	if err != nil {
		t.Fatal(err)
	}

	want := `CREATE TABLE test_table (
    test_one TEXT,
    test_two INTEGER,
    test_three BLOB,
    test_arg TEXT HIDDEN
)`

	if str != want {
		t.Fatalf("wanted: %s, got: %s", want, str)
	}
}

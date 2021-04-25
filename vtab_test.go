package vtab

import (
	"testing"

	"go.riyazali.net/sqlite"
)

func TestCreateTableSQL(t *testing.T) {
	m := &tableFuncModule{
		name: "test_table",
		columns: []Column{
			{Name: "test_one", Type: sqlite.SQLITE_TEXT},
			{Name: "test_two", Type: sqlite.SQLITE_INTEGER},
			{Name: "test_three", Type: sqlite.SQLITE_BLOB},
			{Name: "test_arg", Type: sqlite.SQLITE_TEXT, Hidden: true},
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

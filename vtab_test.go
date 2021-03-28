package vtab

import (
	"testing"

	"go.riyazali.net/sqlite"
)

func TestCreateTableSQL(t *testing.T) {
	m := &tableFuncModule{
		name: "test_table",
		columns: []Column{
			{"test_one", sqlite.SQLITE_TEXT, false, false, nil},
			{"test_two", sqlite.SQLITE_INTEGER, false, false, nil},
			{"test_three", sqlite.SQLITE_BLOB, false, false, nil},
			{"test_arg", sqlite.SQLITE_TEXT, false, true, nil},
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

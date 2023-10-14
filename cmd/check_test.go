package cmd

import (
	"github.com/jwbargsten/go-mssql-load/internal/testkit"
	"testing"
)

func TestLoadSql(t *testing.T) {
	conn := testkit.OpenTestDB(t, "master")
	defer conn.Close()

	args := []string{ "loadsql", "../sql/init.sql"}
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	if err != nil {
		t.Error("error", err)
	}


	row := conn.QueryRow("select hp from pokemon.pokemon where name = 'Wartortle'")
	var hp int
	if err = row.Scan(&hp); err != nil {
		t.Fatal("query failed", err)
	}
	if hp != 4 {
		t.Fatalf("got wrong hp, should be 4 but got %d", hp)
	}
}

func TestPortArg(t *testing.T) {
	args := []string{"--port", "abc", "check"}
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	if err == nil {
		t.Error("expected error for port, but did not get it")
	}
}

func TestCheck(t *testing.T) {
	args := []string{ "check"}
	rootCmd.SetArgs(args)
	err := rootCmd.Execute()
	if err != nil {
		t.Error("error", err)
	}

}

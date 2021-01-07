// +build ignore

// Command gen generates float test values for oracle number types.
package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"

	_ "github.com/sijms/go-ora"
)

var values = []struct {
	S string
	F float64
}{
	{"0", 0},
	{"1", 1},
	{"10", 10},
	{"100", 100},
	{"1000", 1000},
	{"10000000", 10000000},
	{"1E+30", 1e+30},
	{"1E+125", 1e+125},
	{"0.1", 0.1},
	{"0.01", 0.01},
	{"0.001", 0.001},
	{"0.0001", 0.0001},
	{"0.00001", 0.00001},
	{"0.000001", 0.000001},
	{"1E+125", 1e125},
	{"1E-125", 1e-125},
	{"-1E+125", -1e125},
	{"-1E-125", -1e-125},
	{"1.23456789e15", 1.23456789e+15},
	{"1.23456789e-15", 1.23456789e-15},
	{"1.234", 1.234},
	{"12.34", 12.34},
	{"123.4", 123.4},
	{"1234", 1234},
	{"12340", 12340},
	{"123400", 123400},
	{"1234000", 1234000},
	{"12340000", 12340000},
	{"0.1234", 0.1234},
	{"0.01234", 0.01234},
	{"0.001234", 0.001234},
	{"0.0001234", 0.0001234},
	{"0.00001234", 0.00001234},
	{"0.000001234", 0.000001234},
	{"-1.234", -1.234},
	{"-12.34", -12.34},
	{"-123.4", -123.4},
	{"-1234", -1234},
	{"-12340", -12340},
	{"-123400", -123400},
	{"-1234000", -1234000},
	{"-12340000", -12340000},
	{"-0.1234", -0.1234},
	{"-1.234", -1.234},
	{"-12.34", -12.34},
	{"-123.4", -123.4},
	{"-1234", -1234},
	{"-12340", -12340},
	{"-123400", -123400},
	{"-1234000", -1234000},
	{"9.8765", 9.8765},
	{"98.765", 98.765},
	{"987.65", 987.65},
	{"9876.5", 9876.5},
	{"98765", 98765},
	{"987650", 987650},
	{"9876500", 9876500},
	{"0.98765", 0.98765},
	{"0.098765", 0.098765},
	{"0.0098765", 0.0098765},
	{"0.00098765", 0.00098765},
	{"0.000098765", 0.000098765},
	{"0.0000098765", 0.0000098765},
	{"0.00000098765", 0.00000098765},
	{"-9.8765", -9.8765},
	{"-98.765", -98.765},
	{"-987.65", -987.65},
	{"-9876.5", -9876.5},
	{"-98765", -98765},
	{"-987650", -987650},
	{"-9876500", -9876500},
	{"-98765000", -98765000},
	{"-0.98765", -0.98765},
	{"-0.098765", -0.098765},
	{"-0.0098765", -0.0098765},
	{"-0.00098765", -0.00098765},
	{"-0.000098765", -0.000098765},
	{"-0.0000098765", -0.0000098765},
	{"-0.00000098765", -0.00000098765},
	{"2*asin(1)", 2. * math.Asin(1.0)},
	{"1/3", 1.0 / 3.0},
	{"-1/3", -1.0 / 3.0},
	{"9000000000000000000", 9000000000000000000},
	{"-9000000000000000000", -9000000000000000000},
	// {"9223372036854774784", 9223372036854774784},
	// {"-9223372036854774784", -9223372036854774784},
	// {"-1000000000000000000", -1000000000000000000},
	// {"9223372036854775805", 9223372036854775805},
	// {"9223372036854775807", 9223372036854775807},
	// {"-9223372036854775806", -9223372036854775806},
	// {"-9223372036854775808", -9223372036854775808},
}

const maxConvertibleInt = 9223372036854774784

func main() {
	dsn := flag.String("dsn", "oracle://system:P4ssw0rd@localhost/orasid", "dsn")
	dest := flag.String("dest", "values_test.go", "dest file")
	flag.Parse()
	if err := run(*dsn, *dest); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(dsn, dest string) error {
	if dsn == "" {
		dsn = os.Getenv("GOORA_TESTDB")
	}
	if dsn == "" {
		return errors.New("must provide -dsn or define $ENV{GOORA_TESTDB}")
	}
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	var tests []floatTest
	for _, v := range values {
		q := fmt.Sprintf("select N||'' S, N, dump(n) D from (select %s N from DUAL)", v.S)
		rows, err := db.Query(q)
		if err != nil {
			return err
		}
		if !rows.Next() {
			return fmt.Errorf("query %q did not return a row", q)
		}
		var row struct {
			S       string
			F       float64
			AsInt64 int64
			IsInt   bool
			dump    string
		}
		if err := rows.Scan(&row.S, &row.F, &row.dump); err != nil {
			return err
		}
		// Check oracle representation to test if number is Int
		if i := strings.Index(row.S, "."); i == -1 {
			row.IsInt = true
			var err error
			row.AsInt64, err = strconv.ParseInt(row.S, 10, 64)
			if err != nil || row.AsInt64 >= 9000000000000000000 || row.AsInt64 <= -9000000000000000000 {
				if err != nil && !errors.Is(err, strconv.ErrRange) {
					return err
				}
				row.IsInt, row.AsInt64 = false, 0

			}
		}
		if i := strings.Index(row.dump, ": "); i > 0 {
			row.dump = row.dump[i+2:]
		}
		tests = append(tests, floatTest{
			v.S,
			row.S,
			v.F,
			row.AsInt64,
			row.IsInt,
			row.dump,
		})
		if err := rows.Close(); err != nil {
			return err
		}
	}
	buf := new(bytes.Buffer)
	fmt.Fprintln(buf, hdr)
	fmt.Fprintln(buf, "func floatTests() []floatTest {")
	fmt.Fprintln(buf, "\treturn []floatTest{")
	for _, test := range tests {
		fmt.Fprintf(
			buf, "\t\t{%q, %q, %g, %d, %t, []byte{%s}}, // %e\n",
			test.SelectText, test.OracleText, test.Float, test.Int64, test.IsInt, test.Binary, test.Float,
		)
	}
	fmt.Fprintln(buf, "\t}\n}")
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}
	return ioutil.WriteFile(dest, out, 0o644)
}

type floatTest struct {
	SelectText string
	OracleText string
	Float      float64
	Int64      int64
	IsInt      bool
	Binary     string
}

const hdr = `package ora

// Code generated by gen.go. DO NOT EDIT.

type floatTest struct {    
    SelectText string    
    OracleText string    
    Float      float64    
    Int64      int64    
    IsInt      bool    
    Binary     []byte    
}    
`

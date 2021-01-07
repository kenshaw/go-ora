package ora

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var err error
	dsn := os.Getenv("GOORA_TESTDB")
	if dsn == "" {
		log.Fatal("Provide oracle server url in environment variable GOORA_TESTDB")
	}
	db, err = sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("cannot connect to db: %v", err)
	}
	defer db.Close()
	res := m.Run()
	os.Exit(res)
}

func TestSelectBindFloat(t *testing.T) {
	for _, test := range floatTests() {
		t.Run(test.SelectText, func(t *testing.T) {
			query := fmt.Sprintf("select :1 N from dual")
			stmt, err := db.Prepare(query)
			if err != nil {
				t.Fatalf("Query can't be prepared: %v", err)
			}
			defer stmt.Close()
			rows, err := stmt.Query(test.Float)
			if err != nil {
				t.Fatalf("Query can't be run: %v", err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatalf("Query returns no record")
			}
			var got float64
			if err = rows.Scan(&got); err != nil {
				t.Fatalf("Query can't scan row: %v", err)
			}
			var e float64
			if test.Float != 0.0 {
				e = math.Abs((got - test.Float) / test.Float)
			} else {
				e = math.Abs(got - test.Float)
			}
			if e > 1e-15 {
				t.Errorf("Query expecting: %v, got %v, error %g", test.Float, got, e)
			}
		})
	}
}

func TestSelectBindInt(t *testing.T) {
	for _, test := range floatTests() {
		if test.IsInt {
			t.Run(test.SelectText, func(t *testing.T) {
				query := fmt.Sprintf("select :1 N from dual")
				stmt, err := db.Prepare(query)
				if err != nil {
					t.Errorf("Query can't be prepared: %v", err)
					return
				}
				defer stmt.Close()
				rows, err := stmt.Query(test.Int64)
				if err != nil {
					t.Errorf("Query can't be run: %v", err)
					return
				}
				defer rows.Close()
				if !rows.Next() {
					t.Errorf("Query returns no record")
					return
				}
				var got int64
				err = rows.Scan(&got)
				if err != nil {
					t.Errorf("Query can't scan row: %v", err)
					return
				}
				if got != test.Int64 {
					t.Errorf("Query expecting: %v, got %v", test.Int64, got)
				}
			})
		}
	}
}

func TestSelectBindFloatAsInt(t *testing.T) {
	for _, test := range floatTests() {
		t.Run(test.SelectText, func(t *testing.T) {
			query := fmt.Sprintf("select :1 N from dual")
			stmt, err := db.Prepare(query)
			if err != nil {
				t.Errorf("Query can't be prepared: %v", err)
				return
			}
			defer stmt.Close()
			rows, err := stmt.Query(test.Float)
			if err != nil {
				t.Errorf("Query can't be run: %v", err)
				return
			}
			defer rows.Close()
			if !rows.Next() {
				t.Errorf("Query returns no record")
				return
			}
			var got int64
			err = rows.Scan(&got)
			if err == nil && !test.IsInt {
				t.Errorf("Expecting an error when scanning a real float(%v) into an int", test.Float)
				return
			}
			if err != nil && test.IsInt {
				t.Errorf("Un-expecting an error when scanning a int as a float(%v) into an int, err:%v", test.Float, err)
				return
			}
			if err != nil {
				return
			}
			if got != test.Int64 {
				t.Errorf("Expecting: int64(%v), got int64(%v),%v", int64(float64(test.Int64)), got, test.Float)
			}
		})
	}
}

func TestSelectBindIntAsFloat(t *testing.T) {
	for _, test := range floatTests() {
		if test.IsInt {
			t.Run(test.SelectText, func(t *testing.T) {
				query := fmt.Sprintf("select :1 N from dual")
				stmt, err := db.Prepare(query)
				if err != nil {
					t.Fatalf("Query can't be prepared: %v", err)
				}
				defer stmt.Close()
				rows, err := stmt.Query(test.Int64)
				if err != nil {
					t.Fatalf("Query can't be run: %v", err)
				}
				defer rows.Close()
				if !rows.Next() {
					t.Fatalf("Query returns no record")
				}
				var got float64
				if err = rows.Scan(&got); err == nil && !test.IsInt {
					t.Fatalf("Expecting an error when scanning a real float(%v) into an int", test.Float)
				}
				switch {
				case test.Float != 0.0:
					if e := math.Abs((got - test.Float) / test.Float); e > 1e-15 {
						t.Errorf("DecodeDouble(EncodeDouble(%g)) = %g,  Diff= %e", test.Float, got, e)
					}
				case got != 0.0:
					t.Errorf("DecodeDouble(EncodeDouble(%g)) = %g", test.Float, got)
				}
			})
		}
	}
}

package conv

import (
	"math"
	"reflect"
	"testing"
)

// Some documentation:
//	https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/
func TestDecodeDouble(t *testing.T) {
	for _, test := range TestFloatValue {
		t.Run(test.SelectText, func(t *testing.T) {
			got := DecodeDouble(test.Binary)
			e := math.Abs((got - test.Float) / test.Float)
			if e > 1e-15 {
				t.Errorf("DecodeDouble() = %v, want %v, Error= %e", got, test.Float, e)
			}
		})
	}
}

func TestDecodeInt(t *testing.T) {
	for _, test := range TestFloatValue {
		// Test only with interger values
		if test.IsInteger {
			t.Run(test.SelectText, func(t *testing.T) {
				got := DecodeInt(test.Binary)
				if got != test.Integer {
					t.Errorf("DecodeInt() = %v, want %v", got, test.Integer)
				}
			})
		}
	}
}

func TestTypeOfDecodeNumber(t *testing.T) {
	for _, test := range TestFloatValue {
		t.Run(test.SelectText, func(t *testing.T) {
			got := DecodeNumber(test.Binary)
			if i, ok := got.(int64); ok {
				if !test.IsInteger {
					t.Errorf("Expecting a float64(%g), got an int64(%d)", test.Float, i)
					return
				}
				if i != test.Integer {
					t.Errorf("Expecting an int64(%d), got %d", test.Integer, i)
				}
			} else if f, ok := got.(float64); ok {
				if test.IsInteger {
					t.Errorf("Expecting a int64(%d), got a float(%g)", test.Integer, f)
					return
				}
				e := math.Abs((f - test.Float) / test.Float)
				if e > 1e-15 {
					t.Errorf("Expecting an float64(%g), got %g", test.Float, f)
				}
			}
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	for _, test := range TestFloatValue {
		// Test integer values
		if test.IsInteger {
			t.Run(test.SelectText, func(t *testing.T) {
				got := EncodeInt64(test.Integer)
				n2 := DecodeInt(got)
				if n2 != test.Integer {
					t.Errorf("DecodeInt(EncodeInt64(%d)) = %v", test.Integer, n2)
				}
				if !reflect.DeepEqual(got, test.Binary) {
					t.Errorf("EncodeInt64() = %v, want %v", got, test.Binary)
				}
			})
		}
	}
}

func TestEncodeInt(t *testing.T) {
	for _, test := range TestFloatValue {
		// Test integer values
		if test.IsInteger && test.Float >= math.MinInt64 && test.Float <= math.MaxInt64 {
			t.Run(test.SelectText, func(t *testing.T) {
				i := int(test.Integer)
				got := EncodeInt(i)
				n2 := int(DecodeInt(got))
				if n2 != i {
					t.Errorf("DecodeInt(EncodeInt(%d)) = %v", i, n2)
				}
				if !reflect.DeepEqual(got, test.Binary) {
					t.Errorf("EncodeInt() = %v, want %v", got, test.Binary)
				}
			})
		}
	}
}

func TestEncodeDouble(t *testing.T) {
	for _, test := range TestFloatValue {
		t.Run(test.SelectText, func(t *testing.T) {
			got, err := EncodeDouble(test.Float)
			if err != nil {
				t.Errorf("Unexpected error: %s", err)
				return
			}
			f := DecodeDouble(got)
			if test.Float != 0.0 {
				e := math.Abs((f - test.Float) / test.Float)
				if e > 1e-15 {
					t.Errorf("DecodeDouble(EncodeDouble(%g)) = %g,  Error= %e", test.Float, f, e)
				}
			}
			if len(test.Binary) < 10 {
				if !reflect.DeepEqual(test.Binary, got) {
					t.Errorf("EncodeDouble(%g) = %v want %v", test.Float, got, test.Binary)
				}
			}
		})
	}
}

//
// func TestEncodeDate(t *testing.T) {
// 	ti := time.Date(2006, 01, 02, 15, 04, 06, 0, time.UTC)
// 	got := EncodeDate(ti)
// 	want := []byte{214, 7, 1, 2, 15, 4, 5, 0}
// 	if !reflect.DeepEqual(got, want) {
// 		t.Errorf("EncodeDate(%v) = %v, want %v", ti, got, want)
// 	}
// }

package stringtools

import (
	"testing"
)

type toUpperTest struct {
	arg, want string
}

var toUpperTests = []toUpperTest{
	toUpperTest{"hello world", "HELLO WORLD"},
	toUpperTest{"Some´++0+'```!~~Special", "SOME´++0+'```!~~SPECIAL"},
	toUpperTest{"üõäö", "ÜÕÄÖ"},
}

func TestToUpper(t *testing.T) {
	for _, test := range toUpperTests {
		got := ToUpper(test.arg)
		if test.want != got {
			t.Fatalf(`Result of ToUpper("%v") is %v, but wanted it to be %v`, test.arg, got, test.want)
		}
	}
}

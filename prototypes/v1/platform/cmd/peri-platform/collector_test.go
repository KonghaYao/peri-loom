package main

import "testing"

func TestParseCgroupLimitRejectsUnboundedValues(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want int64
		ok   bool
	}{
		"v2 number":       {"1073741824\n", 1073741824, true},
		"v2 unlimited":    {"max\n", 0, false},
		"v1 unlimited":    {"9223372036854771712\n", 0, false},
		"zero":            {"0\n", 0, false},
		"negative":        {"-1\n", 0, false},
		"garbage":         {"not-a-limit\n", 0, false},
		"empty":           {"", 0, false},
		"tebibyte header": {"1099511627776\n", 1099511627776, true},
		"piB upper bound": {"1125899906842624\n", 0, false},
	}
	for name, test := range cases {
		got, ok := parseCgroupLimit(test.raw)
		if got != test.want || ok != test.ok {
			t.Fatalf("%s: parseCgroupLimit(%q) = (%d, %t), want (%d, %t)", name, test.raw, got, ok, test.want, test.ok)
		}
	}
}

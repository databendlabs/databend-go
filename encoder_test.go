package godatabend

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestEmbedTuple struct {
	C       bool
	private int
}

type TestTuple struct {
	A int
	B string
	TestEmbedTuple
}

type TestNestedTuple struct {
	A *TestTuple
	D int
}

func TestTextEncoder(t *testing.T) {
	dt := time.Date(2011, 3, 6, 6, 20, 0, 0, time.UTC)
	d := time.Date(2012, 5, 31, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		value    interface{}
		expected string
	}{
		{true, "1"},
		{int8(1), "1"},
		{int16(1), "1"},
		{int32(1), "1"},
		{int64(1), "1"},
		{int(-1), "-1"},
		{uint8(1), "1"},
		{uint16(1), "1"},
		{uint32(1), "1"},
		{uint64(1), "1"},
		{uint(1), "1"},
		{float32(1), "1"},
		{float64(1), "1"},
		{dt, "'2011-03-06 06:20:00.000000+00:00'"},
		{d, "'2012-05-31 00:00:00.000000+00:00'"},
		{"hello", "'hello'"},
		{[]byte("hello"), "hello"},
		{`\\'hello`, `'\\\\\'hello'`},
		{[]byte(`\\'hello`), `\\'hello`},
		{[]int32{1, 2}, "[1,2]"},
		{[]int32{}, "[]"},
		{Array([]int8{1}), "[1]"},
		{Array([]interface{}{Array([]int8{1})}), "[[1]]"},
		{[][]int16{{1}}, "[[1]]"},
		{[]int16(nil), "[]"},
		{(*int16)(nil), "NULL"},
		{Tuple(TestTuple{A: 1, B: "2", TestEmbedTuple: TestEmbedTuple{C: true, private: 5}}), "(1,'2',1)"},
		{Tuple(TestNestedTuple{A: &TestTuple{A: 1, B: "2", TestEmbedTuple: TestEmbedTuple{C: true}}, D: 4}), "((1,'2',1),4)"},
		{[]TestTuple{{A: 1, B: "2", TestEmbedTuple: TestEmbedTuple{C: true, private: 5}}}, "[(1,'2',1)]"},
	}

	enc := new(textEncoder)
	for _, tc := range testCases {
		v, err := enc.Encode(tc.value)
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, string(v))
		}
	}
}

func TestTextEncoder_Map(t *testing.T) {
	testCases := []struct {
		value interface{}
		reg   *regexp.Regexp
	}{
		{value: Map(map[string]string{"KEY1": "Value1", "Key2": "Value2"}), reg: regexp.MustCompile(`map\(('KEY1','Value1','Key2','Value2'|'Key2','Value2','KEY1','Value1')\)`)},
		{value: Map(map[string]int{"KEY1": 1, "Key2": 2}), reg: regexp.MustCompile(`map\(('KEY1',1,'Key2',2|'Key2',2,'KEY1',1)\)`)},
		{value: Map(map[string]bool{"KEY1": true, "Key2": false}), reg: regexp.MustCompile(`map\(('KEY1',1,'Key2',0|'Key2',0,'KEY1',1)\)`)},
	}

	enc := new(textEncoder)
	for _, tc := range testCases {
		v, err := enc.Encode(tc.value)
		if assert.NoError(t, err) {
			assert.True(t, tc.reg.Match(v))
		}
	}
}

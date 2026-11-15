package godatabend

import (
	"math/big"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decimal128Array(t testing.TB, scale int32, values ...*big.Int) *arrowarray.Decimal128 {
	t.Helper()
	b := arrowarray.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 38, Scale: scale})
	defer b.Release()
	for _, v := range values {
		if v == nil {
			b.AppendNull()
			continue
		}
		b.Append(decimal128.FromBigInt(v))
	}
	return b.NewArray().(*arrowarray.Decimal128)
}

func bigInt(s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic(s)
	}
	return v
}

func TestFormatArrowDecimal128(t *testing.T) {
	for _, c := range []struct {
		value string
		scale int32
		want  string
		ok    bool
	}{
		{"0", 0, "0", true},
		{"0", 19, "0", true},
		{"123", 0, "123", true},
		{"100", 0, "100", true},
		{"-100", 0, "-100", true},
		{"718943000000000000000000", 19, "71894.3", true},
		{"718943000000000000000001", 19, "71894.3000000000000000001", true},
		{"-5", 1, "-0.5", true},
		{"5", 3, "0.005", true},
		{"1", 4, "0.0001", true},
		{"10", 5, "0.0001", true},
		{"1", 5, "", false}, // 1e-5: big.Float writes it as 1e-05
		{"-1", 5, "", false},
		{"123456789012345678901234567890123456", 0, "123456789012345678901234567890123456", true},
		{"99999999999999999999999999999999999999", 38, "0.99999999999999999999999999999999999999", true},
		{"-99999999999999999999999999999999999999", 0, "-99999999999999999999999999999999999999", true},
		{"10000000000000000000", 0, "10000000000000000000", true}, // exactly 10^19
		{"10000000000000000001", 0, "10000000000000000001", true}, // one above
		{"9999999999999999999", 0, "9999999999999999999", true},   // one below
		{"20000000000000000000000", 19, "2000", true},             // 2e22 at scale 19
	} {
		got, ok := formatArrowDecimal128(decimal128.FromBigInt(bigInt(c.value)), c.scale)
		assert.Equal(t, c.ok, ok, "%s at scale %d", c.value, c.scale)
		if c.ok {
			assert.Equal(t, c.want, got, "%s at scale %d", c.value, c.scale)
		}
	}
	// The lowest 128-bit value is its own negation: its magnitude is still right.
	lowest := decimal128.New(-1<<63, 0)
	got, ok := formatArrowDecimal128(lowest, 0)
	assert.True(t, ok)
	assert.Equal(t, "-170141183460469231731687303715884105728", got)
	highest := decimal128.New(1<<63-1, 1<<64-1)
	got, ok = formatArrowDecimal128(highest, 0)
	assert.True(t, ok)
	assert.Equal(t, "170141183460469231731687303715884105727", got)

	// A negative scale is left to the generic path.
	_, ok = formatArrowDecimal128(decimal128.FromI64(5), -2)
	assert.False(t, ok)
}

// The text is the one arrow-go's GetOneForMarshal gives, for random values of every
// magnitude and sign, and a value below 1e-4, which big.Float writes in exponent
// form, is the one case left to the generic path.
func TestFormatArrowDecimal128MatchesGetOneForMarshal(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil)
	for _, scale := range []int32{0, 1, 2, 4, 5, 9, 18, 19, 20, 30, 37, 38} {
		var values []*big.Int
		add := func(v *big.Int) {
			if v.CmpAbs(limit) < 0 {
				values = append(values, v)
			}
		}
		add(big.NewInt(0))
		add(big.NewInt(1))
		add(big.NewInt(-1))
		for k := 0; k < 38; k++ {
			p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(k)), nil)
			add(p)
			add(new(big.Int).Sub(p, big.NewInt(1)))
			add(new(big.Int).Add(p, big.NewInt(1)))
			add(new(big.Int).Neg(p))
		}
		for i := 0; i < 3000; i++ {
			digits := 1 + rng.Intn(38)
			v := new(big.Int).Rand(rng, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil))
			if rng.Intn(2) == 0 {
				v.Neg(v)
			}
			add(v)
		}
		arr := decimal128Array(t, scale, values...)
		fast, fallback := 0, 0
		for i := 0; i < arr.Len(); i++ {
			want := arr.GetOneForMarshal(i).(string)
			got, ok := formatArrowDecimal128(arr.Value(i), scale)
			if !ok {
				fallback++
				require.True(t, strings.ContainsAny(want, "eE"), "scale %d, %s: fell back for %q, which is not in exponent form", scale, values[i], want)
				continue
			}
			fast++
			require.Equal(t, want, got, "scale %d, value %s", scale, values[i])
		}
		arr.Release()
		assert.NotZero(t, fast, "scale %d", scale)
	}
}

func TestFormatArrowColumnValueDecimal(t *testing.T) {
	arr := decimal128Array(t, 19, bigInt("718943000000000000000000"), nil, bigInt("1"), bigInt("-25000000000000000000"))
	defer arr.Release()
	for _, c := range []struct {
		row  int
		want string
	}{
		{0, "71894.3"},
		{1, ""},      // null
		{2, "1e-19"}, // below 1e-4: the generic path
		{3, "-2.5"},
	} {
		got, err := formatArrowColumnValue(&TypeDesc{Name: "Decimal"}, arr, c.row, time.UTC)
		require.NoError(t, err)
		assert.Equal(t, c.want, got, "row %d", c.row)
		// With no type description it is the same.
		got, err = formatArrowColumnValue(nil, arr, c.row, time.UTC)
		require.NoError(t, err)
		assert.Equal(t, c.want, got, "row %d without a description", c.row)
	}
}

// BenchmarkDecimal128Format formats a column of DECIMAL(38,19) the way the driver
// does for every row it returns.
func BenchmarkDecimal128Format(b *testing.B) {
	values := make([]*big.Int, 100000)
	for i := range values {
		values[i] = new(big.Int).Mul(big.NewInt(718943000000+int64(i)), big.NewInt(100000000))
	}
	arr := decimal128Array(b, 19, values...)
	defer arr.Release()
	desc := &TypeDesc{Name: "Decimal"}
	b.ResetTimer()
	for b.Loop() {
		for i := 0; i < arr.Len(); i++ {
			if _, err := formatArrowColumnValue(desc, arr, i, time.UTC); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(arr.Len())*float64(b.N)/b.Elapsed().Seconds(), "values/s")
}

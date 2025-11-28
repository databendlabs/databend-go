package godatabend

import (
	"database/sql/driver"
	"github.com/pkg/errors"
	"reflect"
)

func buildQuery(query string, args []driver.Value, placeholders *[]int) (string, error) {
	var q string
	var err error
	if placeholders != nil {
		if len(*placeholders) != len(args) {
			return "", errors.Errorf("expect %v args, got %v", len(*placeholders), len(args))
		}
		q, err = interpolateParams2(query, args, *placeholders)
		if err != nil {
			return "", err
		}
		return q, nil
	} else {
		if len(args) > 0 && args[0] != nil {
			result, err := interpolateParams(query, args)
			if err != nil {
				return result, errors.Wrap(err, "buildRequest: failed to interpolate params")
			}
			return result, nil
		}
		return query, nil
	}
}

func placeholders(query string) []int {
	n := 0
	quote := false
	first := -1
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\\':
			i++
		case '\'':
			quote = !quote
		case '?':
			if !quote {
				n++
				if first == -1 {
					first = i
				}
			}
		}
	}
	if n == 0 {
		return nil
	}
	quote = false
	index := make([]int, n)
	n = 0
	for i, ch := range query[first:] {
		switch ch {
		case '\'':
			quote = !quote
		case '?':
			if !quote {
				index[n] = first + i
				n++
			}
		}
	}
	return index
}

func interpolateParams(query string, params []driver.Value) (string, error) {
	return interpolateParams2(query, params, placeholders(query))
}

func interpolateParams2(query string, params []driver.Value, index []int) (string, error) {
	if len(params) == 0 {
		return query, nil
	}
	if reflect.TypeOf(params[0]).Kind() == reflect.Slice {
		if reflect.ValueOf(params[0]).Len() == 0 {
			return query, nil
		}
	}
	if len(index) != len(params) {
		return "", ErrPlaceholderCount
	}

	var (
		queryRaw      = []byte(query)
		paramsEncoded = make([][]byte, len(params))
		n             = len(queryRaw) - len(index) // do not count number of placeholders
	)
	for i, v := range params {
		paramsEncoded[i], _ = textEncode.Encode(v)
		n += len(paramsEncoded[i])
	}
	buf := make([]byte, n)
	i := 0
	k := 0
	for j, idx := range index {
		copy(buf[k:], queryRaw[i:idx])
		k += idx - i
		copy(buf[k:], paramsEncoded[j])
		i = idx + 1
		k += len(paramsEncoded[j])
	}
	copy(buf[k:], query[i:])
	return string(buf), nil
}

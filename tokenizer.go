package godatabend

import (
	"bytes"
	"fmt"
	"io"
)

const (
	eof = rune(0)
)

func read(s io.RuneScanner) rune {
	r, _, err := s.ReadRune()
	if err != nil {
		return eof
	}
	return r
}

func readEscaped(s io.RuneScanner) (rune, error) {
	r := read(s)
	switch r {
	case eof:
		return 0, fmt.Errorf("unexpected eof in escaped char")
	case 'b':
		return '\b', nil
	case 'f':
		return '\f', nil
	case 'r':
		return '\r', nil
	case 'n':
		return '\n', nil
	case 't':
		return '\t', nil
	case '0':
		return '\x00', nil
	default:
		return r, nil
	}
}

func readRaw(s io.RuneScanner) *bytes.Buffer {
	var data bytes.Buffer

	for {
		r := read(s)

		if r == eof {
			break
		}

		data.WriteRune(r)
	}

	return &data
}

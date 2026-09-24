package jsoncodec

import (
	"encoding/json"
	"errors"
)

func Marshal(v any) ([]byte, error) {
	if v == nil {
		return nil, errors.New("json marshal nil message")
	}
	return json.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	if v == nil {
		return errors.New("json unmarshal nil message")
	}
	return json.Unmarshal(data, v)
}

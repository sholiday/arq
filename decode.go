package arq

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
)

var (
	ErrUnimplemented      = errors.New("Unimplemented")
	ErrUnknownSliceLength = errors.New("ErrUnknownSliceLength")
	ErrTooLong            = errors.New("value contains too many elements")
)

type ArqUnmarshaler interface {
	UnmarshalArq(io.Reader) error
}

func DecodeArq(r io.Reader, i interface{}) error {
	// If this type knows how to decode itself, let it.
	if um, ok := i.(ArqUnmarshaler); ok {
		return um.UnmarshalArq(r)
	}

	return decodeArqValue(r, reflect.ValueOf(i).Elem(), "")
}

func decodeArqValue(r io.Reader, v reflect.Value, tag string) error {
	switch v.Kind() {
	case reflect.Int8:
		var n int8
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetInt(int64(n))
		return nil
	case reflect.Int32:
		var n int32
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetInt(int64(n))
		return nil
	case reflect.Int64:
		var n int64
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetInt(n)
		return nil
	case reflect.Uint8:
		var n uint8
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetUint(uint64(n))
		return nil
	case reflect.Uint32:
		var n uint32
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetUint(uint64(n))
		return nil
	case reflect.Uint64:
		var n uint64
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetUint(n)
		return nil
	case reflect.Bool:
		var n uint8
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
		v.SetBool(n == 1)
		return nil
	case reflect.Array:
		// byte arrays are special cased because we use them so often.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return decodeByteArray(r, v)
		}
		// Otherwise, recurse for each element.
		for i := 0; i < v.Len(); i++ {
			err := decodeArqValue(r, v.Index(i), "")
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.String:
		return decodeString(r, v)
	case reflect.Struct:
		return decodeStruct(r, v, tag)
	case reflect.Slice:
		return decodeSlice(r, v, tag)
	}
	return fmt.Errorf("decoding '%s' %w", v.Type().String(), ErrUnimplemented)
}

func decodeByteArray(r io.Reader, v reflect.Value) error {
	buf := make([]byte, v.Len())
	_, err := io.ReadAtLeast(r, buf, v.Len())
	if err != nil {
		return err
	}
	reflect.Copy(v, reflect.ValueOf(buf))
	return nil
}

func decodeString(r io.Reader, v reflect.Value) error {
	notNull := byte(0)
	if err := binary.Read(r, binary.BigEndian, &notNull); err != nil {
		return err
	}
	if notNull != 1 {
		return nil
	}
	length := uint64(0)
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return err
	}
	if length > 4096 {
		return ErrTooLong
	}
	buf := make([]byte, length)
	if _, err := io.ReadAtLeast(r, buf, int(length)); err != nil {
		return err
	}
	v.SetString(string(buf))
	return nil
}

func decodeStruct(r io.Reader, v reflect.Value, tag string) error {
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).CanSet() {
			continue
		}
		err := decodeArqValue(r, v.Field(i), v.Type().Field(i).Tag.Get("arq"))
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeSlice(r io.Reader, v reflect.Value, tag string) error {
	// Determine the number of elements in the slice.
	var n uint64
	switch tag {
	case "len-uint32":
		var n32 uint32
		err := binary.Read(r, binary.BigEndian, &n32)
		if err != nil {
			return err
		}
		n = uint64(n32)
	case "len-uint64":
		err := binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return err
		}
	default:
		return ErrUnknownSliceLength
	}
	if n > 4096 {
		return ErrTooLong
	}
	// Fast path for bytes to avoid recursing.
	if v.Type().Elem().Kind() == reflect.Uint8 {
		buf := make([]byte, int(n))
		_, err := io.ReadAtLeast(r, buf, int(n))
		if err != nil {
			return err
		}
		v.Set(reflect.AppendSlice(v, reflect.ValueOf(buf)))
		return nil
	}
	for i := 0; i < int(n); i++ {
		elem := reflect.New(v.Type().Elem()).Elem()
		if err := decodeArqValue(r, elem, ""); err != nil {
			return err
		}
		v.Set(reflect.Append(v, elem))
	}
	return nil
}

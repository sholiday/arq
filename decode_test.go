package arq_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/sholiday/arq"
	"github.com/stretchr/testify/assert"
)

func TestDecodeBasic(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := int8(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual int8
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("int32", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := int32(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual int32
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("int64", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := int64(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual int64
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("uint8", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := uint8(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual uint8
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("uint32", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := uint32(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual uint32
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("uint64", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := uint64(42)
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual uint64
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("bool", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := true
		err := binary.Write(buf, binary.BigEndian, uint8(1))
		if !assert.Nil(t, err) {
			return
		}
		var actual bool
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
}

func TestDecodeArray(t *testing.T) {
	t.Run("byte-array", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := [4]byte{0xFE, 0xED, 0xFA, 0xCE}
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual [4]byte
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("uint32-array", func(t *testing.T) {
		buf := new(bytes.Buffer)
		expected := [4]uint32{1, 2, 3, 4}
		err := binary.Write(buf, binary.BigEndian, expected)
		if !assert.Nil(t, err) {
			return
		}
		var actual [4]uint32
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
}

func TestDecodeString(t *testing.T) {
	testCases := []struct {
		name          string
		notNull       uint8
		length        uint64
		values        []byte
		expected      string
		expectedError error
	}{
		{"Basic", 1, 4, []byte("arq!"), "arq!", nil},
		{"Null", 0, 0, []byte{}, "", nil},
		{"NullWithData", 0, 4, []byte("arq!"), "", nil},
		{"Length", 1, 3, []byte("arq!"), "arq", nil},

		// These should all return an error.
		{"Eof", 1, 100, []byte("arq!"), "", io.ErrUnexpectedEOF},
		{"TooLarge", 1, 5000, make([]byte, 5000), "", arq.ErrTooLong},
	}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			assert.Nil(t, binary.Write(buf, binary.BigEndian, tCase.notNull))
			if tCase.notNull == 1 {
				assert.Nil(t, binary.Write(buf, binary.BigEndian, tCase.length))
				assert.Nil(t, binary.Write(buf, binary.BigEndian, tCase.values))
			}
			var actual string
			err := arq.DecodeArq(buf, &actual)
			assert.Equal(t, err, tCase.expectedError)
			assert.Equal(t, tCase.expected, actual)
		})
	}
}

func TestDecodeStruct(t *testing.T) {
	type testStruct struct {
		B   bool
		By  byte
		U32 uint32
	}
	expected := testStruct{false, 0xFF, 94043}
	buf := new(bytes.Buffer)
	assert.Nil(t, binary.Write(buf, binary.BigEndian, expected))
	var actual testStruct
	assert.Nil(t, arq.DecodeArq(buf, &actual))
	assert.Equal(t, expected, actual)
}

func TestDecodeSlice(t *testing.T) {
	t.Run("BytesLen32", func(t *testing.T) {
		type testStruct struct {
			By []byte `arq:"len-uint32"`
		}
		expected := testStruct{[]byte("arq!")}
		buf := new(bytes.Buffer)
		assert.Nil(t, binary.Write(buf, binary.BigEndian, uint32(4)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, []byte("arq!")))

		var actual testStruct
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("BytesLen64", func(t *testing.T) {
		type testStruct struct {
			By []byte `arq:"len-uint64"`
		}
		expected := testStruct{[]byte("arq!")}
		buf := new(bytes.Buffer)
		assert.Nil(t, binary.Write(buf, binary.BigEndian, uint64(4)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, []byte("arq!")))

		var actual testStruct
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("Int32sLen32", func(t *testing.T) {
		type testStruct struct {
			By []int32 `arq:"len-uint32"`
		}
		expected := testStruct{[]int32{94040, 94043}}
		buf := new(bytes.Buffer)
		assert.Nil(t, binary.Write(buf, binary.BigEndian, uint32(2)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, int32(94040)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, int32(94043)))

		var actual testStruct
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
	t.Run("Int32sLen64", func(t *testing.T) {
		type testStruct struct {
			By []int32 `arq:"len-uint64"`
		}
		expected := testStruct{[]int32{94040, 94043}}
		buf := new(bytes.Buffer)
		assert.Nil(t, binary.Write(buf, binary.BigEndian, uint64(2)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, int32(94040)))
		assert.Nil(t, binary.Write(buf, binary.BigEndian, int32(94043)))

		var actual testStruct
		assert.Nil(t, arq.DecodeArq(buf, &actual))
		assert.Equal(t, expected, actual)
	})
}

type testUnmarshalable struct {
	MyValue string
}

func (ts *testUnmarshalable) UnmarshalArq(r io.Reader) error {
	ts.MyValue = "arq!"
	return nil
}

func TestDecodeArqUnmarshaler(t *testing.T) {
	expected := testUnmarshalable{"arq!"}
	buf := new(bytes.Buffer)
	var actual testUnmarshalable
	assert.Nil(t, arq.DecodeArq(buf, &actual))
	assert.Equal(t, expected, actual)
}

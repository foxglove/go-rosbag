package ros1msg

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
)

var (
	trueBytes  = []byte("true")
	falseBytes = []byte("false")
)

type converter func(io.Writer, io.Reader) error

type recordField struct {
	name      string
	converter converter
}

type JSONTranscoder struct {
	buf             []byte
	parentPackage   string
	converter       converter
	formattedNumber []byte
}

func (t *JSONTranscoder) Transcode(w io.Writer, r io.Reader) error {
	return t.converter(w, r)
}

func (t *JSONTranscoder) recordFromFields(fields []Field) (converter, error) {
	recordFields := []recordField{}
	for _, field := range fields {
		// record
		if field.Type.IsRecord {
			recordConverter, err := t.recordFromFields(field.Type.Fields)
			if err != nil {
				return nil, fmt.Errorf("failed to build dependent fields: %w", err)
			}
			recordFields = append(recordFields, recordField{
				name:      field.Name,
				converter: recordConverter,
			})
			continue
		}
		// complex array
		if field.Type.IsArray && field.Type.Items.IsRecord {
			recordConverter, err := t.recordFromFields(field.Type.Items.Fields)
			if err != nil {
				return nil, fmt.Errorf("failed to build dependent fields: %w", err)
			}
			recordFields = append(recordFields, recordField{
				name:      field.Name,
				converter: t.array(recordConverter, field.Type.FixedSize, false),
			})
			continue
		}
		converterType := field.Type.BaseType
		// if it's still an array, must be primitive
		if field.Type.IsArray {
			converterType = field.Type.Items.BaseType
		}
		var converter converter
		var isBytes bool
		if !field.Type.IsRecord {
			switch converterType {
			case "bool":
				converter = t.bool
			case "int8":
				converter = t.int8
			case "int16":
				converter = t.int16
			case "int32":
				converter = t.int32
			case "int64":
				converter = t.int64
			case "uint8":
				isBytes = true
				converter = t.uint8
			case "uint16":
				converter = t.uint16
			case "uint32":
				converter = t.uint32
			case "uint64":
				converter = t.uint64
			case "float32":
				converter = t.float32
			case "float64":
				converter = t.float64
			case "string":
				converter = t.string
			case "time":
				converter = t.time
			case "duration":
				converter = t.duration
			case "char":
				converter = t.uint8
			case "byte":
				converter = t.uint8
			default:
				return nil, ErrUnrecognizedPrimitive
			}
		}
		if field.Type.IsArray {
			converter = t.array(converter, field.Type.FixedSize, isBytes)
		}
		recordFields = append(recordFields, recordField{
			converter: converter,
			name:      field.Name,
		})
	}
	return t.record(recordFields), nil
}

func NewJSONTranscoder(parentPackage string, data []byte) (*JSONTranscoder, error) {
	fields, err := ParseMessageDefinition(parentPackage, data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message definition: %w", err)
	}
	t := &JSONTranscoder{
		buf:           make([]byte, 8),
		parentPackage: parentPackage,
	}
	converter, err := t.recordFromFields(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to build root converter: %w", err)
	}
	t.converter = converter
	return t, nil
}

func (t *JSONTranscoder) bool(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:1]); err != nil {
		return fmt.Errorf("unable to read byte: %w", err)
	}
	switch t.buf[0] {
	case 0:
		_, err := w.Write(falseBytes)
		if err != nil {
			return fmt.Errorf("unable to write bool: %w", err)
		}
	case 1:
		_, err := w.Write(trueBytes)
		if err != nil {
			return fmt.Errorf("unable to write bool: %w", err)
		}
	default:
		return ErrInvalidBool
	}
	return nil
}

func (t *JSONTranscoder) int8(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:1]); err != nil {
		return fmt.Errorf("failed to read int8: %w", err)
	}
	s := strconv.Itoa(int(t.buf[0]))
	if _, err := w.Write([]byte(s)); err != nil {
		return fmt.Errorf("failed to write int8: %w", err)
	}
	return nil
}

func (t *JSONTranscoder) uint8(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:1]); err != nil {
		return fmt.Errorf("failed to read uint8: %w", err)
	}
	s := strconv.Itoa(int(t.buf[0]))
	if _, err := w.Write([]byte(s)); err != nil {
		return fmt.Errorf("failed to write uint8: %w", err)
	}
	return nil
}

func (t *JSONTranscoder) int16(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:2]); err != nil {
		return fmt.Errorf("failed to read int16: %w", err)
	}
	x := int(binary.LittleEndian.Uint16(t.buf[:2]))
	s := strconv.Itoa(x)
	if _, err := w.Write([]byte(s)); err != nil {
		return fmt.Errorf("failed to write int16: %w", err)
	}
	return nil
}

func (t *JSONTranscoder) string(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:4]); err != nil {
		return fmt.Errorf("failed to read string length: %w", err)
	}
	length := binary.LittleEndian.Uint32(t.buf[:4])
	if uint32(len(t.buf)) < length {
		t.buf = make([]byte, length)
	}
	if _, err := io.ReadFull(r, t.buf[:length]); err != nil {
		return fmt.Errorf("failed to read string: %w", err)
	}
	if _, err := w.Write([]byte(strconv.QuoteToASCII(string(t.buf[:length])))); err != nil {
		return fmt.Errorf("failed to write string: %w", err)
	}
	return nil
}

func (t *JSONTranscoder) uint16(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:2]); err != nil {
		return fmt.Errorf("failed to read uint16: %w", err)
	}
	x := int(binary.LittleEndian.Uint16(t.buf[:2]))
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(x), 10)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted uint16: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) int32(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:4]); err != nil {
		return fmt.Errorf("failed to read int32: %w", err)
	}
	x := binary.LittleEndian.Uint32(t.buf[:4])
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(x), 10)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted int32: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) uint32(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:4]); err != nil {
		return fmt.Errorf("failed to read uint32: %w", err)
	}
	x := binary.LittleEndian.Uint32(t.buf[:4])
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(x), 10)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted uint32: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) int64(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:8]); err != nil {
		return fmt.Errorf("failed to read int64: %w", err)
	}
	x := binary.LittleEndian.Uint64(t.buf[:8])
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(x), 10)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted int64: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) uint64(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:8]); err != nil {
		return fmt.Errorf("failed to read uint64: %w", err)
	}
	x := binary.LittleEndian.Uint64(t.buf[:8])
	t.formattedNumber = strconv.AppendUint(t.formattedNumber, x, 10)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted uint64: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) float32(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:4]); err != nil {
		return fmt.Errorf("failed to read float32: %w", err)
	}
	x := binary.LittleEndian.Uint32(t.buf[:4])
	float := float64(math.Float32frombits(x))
	t.formattedNumber = formatFloat(t.formattedNumber, float, 32)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted float32: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

// formatFloat represents floating point numbers as JSON values. NaN and infinity are represented
// as strings consistent with `protojson` output.
// https://protobuf.dev/programming-guides/proto3/#json
func formatFloat(buf []byte, float float64, precision int) []byte {
	switch {
	case math.IsNaN(float):
		buf = append(buf, []byte(`"NaN"`)...)
	case math.IsInf(float, 1):
		buf = append(buf, []byte(`"Infinity"`)...)
	case math.IsInf(float, -1):
		buf = append(buf, []byte(`"-Infinity"`)...)
	default:
		buf = strconv.AppendFloat(buf, float, 'f', -1, precision)
	}
	return buf
}

func (t *JSONTranscoder) float64(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:8]); err != nil {
		return fmt.Errorf("failed to read float64: %w", err)
	}
	x := binary.LittleEndian.Uint64(t.buf[:8])
	float := math.Float64frombits(x)
	t.formattedNumber = formatFloat(t.formattedNumber, float, 64)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted float64: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func digits(n uint32) int {
	if n == 0 {
		return 1
	}
	count := 0
	for n != 0 {
		n /= 10
		count++
	}
	return count
}

func (t *JSONTranscoder) formatTime(secs uint32, nsecs uint32) {
	nanosecondsDigits := digits(nsecs)
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(secs), 10)
	t.formattedNumber = append(t.formattedNumber, '.')
	for i := 0; i < 9-nanosecondsDigits; i++ {
		t.formattedNumber = append(t.formattedNumber, '0')
	}
	t.formattedNumber = strconv.AppendInt(t.formattedNumber, int64(nsecs), 10)
}

func (t *JSONTranscoder) time(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:8]); err != nil {
		return fmt.Errorf("failed to read time: %w", err)
	}
	secs := binary.LittleEndian.Uint32(t.buf[:4])
	nsecs := binary.LittleEndian.Uint32(t.buf[4:])
	t.formatTime(secs, nsecs)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted time: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) duration(w io.Writer, r io.Reader) error {
	if _, err := io.ReadFull(r, t.buf[:8]); err != nil {
		return fmt.Errorf("failed to read duration: %w", err)
	}
	secs := binary.LittleEndian.Uint32(t.buf[:4])
	nsecs := binary.LittleEndian.Uint32(t.buf[4:])
	t.formatTime(secs, nsecs)
	if _, err := w.Write(t.formattedNumber); err != nil {
		return fmt.Errorf("failed to write formatted duration: %w", err)
	}
	t.formattedNumber = t.formattedNumber[:0]
	return nil
}

func (t *JSONTranscoder) array(items converter, fixedSize int, isBytes bool) converter {
	return func(w io.Writer, r io.Reader) error {
		var arrayLength uint32
		if fixedSize > 0 {
			arrayLength = uint32(fixedSize)
		} else {
			if _, err := io.ReadFull(r, t.buf[:4]); err != nil {
				return fmt.Errorf("failed to read array length: %w", err)
			}
			arrayLength = binary.LittleEndian.Uint32(t.buf[:4])
		}

		// if isBytes is set, we will base64 the content directly. Otherwise
		// transcode elements as a JSON array.
		if isBytes {
			_, err := w.Write([]byte("\""))
			if err != nil {
				return fmt.Errorf("error writing array start: %w", err)
			}
			encoder := base64.NewEncoder(base64.StdEncoding, w)
			_, err = io.CopyN(encoder, r, int64(arrayLength))
			if err != nil {
				return fmt.Errorf("failed to encode base64 array: %w", err)
			}
			err = encoder.Close()
			if err != nil {
				return fmt.Errorf("failed to close base64 encoder: %w", err)
			}
			_, err = w.Write([]byte("\""))
			if err != nil {
				return fmt.Errorf("error writing array end: %w", err)
			}
			return nil
		}

		if _, err := w.Write([]byte("[")); err != nil {
			return fmt.Errorf("failed to write array start: %w", err)
		}
		for i := uint32(0); i < arrayLength; i++ {
			if i > 0 {
				if _, err := w.Write([]byte(",")); err != nil {
					return fmt.Errorf("failed to write array separator: %w", err)
				}
			}
			if err := items(w, r); err != nil {
				return fmt.Errorf("failed to transcode array item: %w", err)
			}
		}
		if _, err := w.Write([]byte("]")); err != nil {
			return fmt.Errorf("failed to write array end: %w", err)
		}
		return nil
	}
}

func (t *JSONTranscoder) record(fields []recordField) converter {
	comma := []byte(",")
	leftBracket := []byte("{")
	rightBracket := []byte("}")
	buf := []byte{}
	return func(w io.Writer, r io.Reader) error {
		if _, err := w.Write(leftBracket); err != nil {
			return fmt.Errorf("failed to open record: %w", err)
		}
		for i, field := range fields {
			if i > 0 {
				_, err := w.Write(comma)
				if err != nil {
					return fmt.Errorf("failed to write comma: %w", err)
				}
			}
			if len(buf) < 3+len(field.name) {
				buf = make([]byte, 3+len(field.name))
			}
			buf[0] = '"'
			buf[1+len(field.name)] = '"'
			buf[2+len(field.name)] = ':'
			n := copy(buf[1:], field.name)
			_, err := w.Write(buf[:3+n])
			if err != nil {
				return fmt.Errorf("failed to write field %s name: %w", field.name, err)
			}
			err = field.converter(w, r)
			if err != nil {
				return fmt.Errorf("failed to convert field %s: %w", field.name, err)
			}
		}
		if _, err := w.Write(rightBracket); err != nil {
			return fmt.Errorf("failed to close record: %w", err)
		}
		return nil
	}
}

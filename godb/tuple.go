package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, tupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType DBType = iota
	StringType
	UnknownType //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	default:
		return "unknown"
	}
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (td *TupleDesc) equals(other *TupleDesc) bool {
	if len(td.Fields) != len(other.Fields) {
		return false
	}
	for i, f1 := range td.Fields {
		f2 := other.Fields[i]
		if f1.Fname != f2.Fname || f1.Ftype != f2.Ftype {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, Error{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, Error{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	return &TupleDesc{Fields: fields}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (td *TupleDesc) merge(other *TupleDesc) *TupleDesc {
	mergedFields := make([]FieldType, len(td.Fields)+len(other.Fields))
	copy(mergedFields, td.Fields)
	copy(mergedFields[len(td.Fields):], other.Fields)

	// Return a new tupleDesc with the merged fields
	return &TupleDesc{Fields: mergedFields}
}

func (td *TupleDesc) getTupleSizeInBytes() int {
	size := 0
	for _, field := range td.Fields {
		switch field.Ftype {
		case IntType:
			size += int(unsafe.Sizeof(int64(0)))
		case StringType:
			size += StringLength
		default:
			panic(fmt.Sprintf("unknown field type %s", field.Ftype))
		}
	}
	return size
}

// ================== Tuple Methods ======================

// DBValue Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
	writeTo(b *bytes.Buffer) error
}

// IntField Integer field value
type IntField struct {
	Value int64
}

func (f IntField) writeTo(b *bytes.Buffer) error {
	return binary.Write(b, binary.LittleEndian, f.Value)
}

// StringField String field value
type StringField struct {
	Value string
}

func (f StringField) writeTo(b *bytes.Buffer) error {
	padded := []byte(f.Value)
	if len(padded) < StringLength {
		pad := make([]byte, StringLength-len(padded))
		padded = append(padded, pad...)
	}

	if err := binary.Write(b, binary.LittleEndian, padded); err != nil {
		return err
	}

	return nil
}

func readDBValueFrom(b *bytes.Buffer, t DBType) (DBValue, error) {
	switch t {
	case IntType:
		var v int64
		if err := binary.Read(b, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return IntField{Value: v}, nil
	case StringType:
		buf := make([]byte, StringLength)
		if err := binary.Read(b, binary.LittleEndian, &buf); err != nil {
			return nil, err
		}

		for i := len(buf) - 1; i >= 0; i-- {
			if buf[i] != 0 {
				buf = buf[:i+1]
				break
			}
		}

		return StringField{Value: string(buf)}, nil
	default:
		return nil, Error{IncompatibleTypesError, fmt.Sprintf("unknown DB type %d", t)}
	}
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for _, field := range t.Fields {
		if err := field.writeTo(b); err != nil {
			return err
		}
	}

	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	var (
		err error
		t   Tuple
	)

	t.Desc = *desc
	for _, desc := range t.Desc.Fields {
		var dbVal DBValue
		if dbVal, err = readDBValueFrom(b, desc.Ftype); err != nil {
			return nil, err
		}
		t.Fields = append(t.Fields, dbVal)
	}

	return &t, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [tupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t *Tuple) equals(other *Tuple) bool {
	if !t.Desc.equals(&other.Desc) {
		return false
	}

	if len(t.Fields) != len(other.Fields) {
		return false
	}

	for i, f := range t.Fields {
		if !f.EvalPred(other.Fields[i], OpEq) {
			return false
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil {
		return t2
	}
	if t2 == nil {
		return t1
	}

	return &Tuple{
		Desc:   *t1.Desc.merge(&t2.Desc),
		Fields: append(t1.Fields, t2.Fields...),
	}
}

type orderByState int

const (
	OrderedLessThan orderByState = iota
	OrderedEqual
	OrderedGreaterThan
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(other *Tuple, field Expr) (order orderByState, err error) {
	tVal, err := field.EvalExpr(t)
	if err != nil {
		return
	}

	otherVal, err := field.EvalExpr(other)
	if err != nil {
		return
	}

	if !isCompatibleValueType(tVal, otherVal) {
		err = Error{IncompatibleTypesError, fmt.Sprintf("type %s is not compatible with type %s", tVal, otherVal)}
		return
	}

	if tVal.EvalPred(otherVal, OpEq) {
		return OrderedEqual, nil
	}
	if tVal.EvalPred(otherVal, OpGt) {
		return OrderedGreaterThan, nil
	}
	return OrderedLessThan, nil
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	result := &Tuple{}
	for _, field := range fields {
		fieldID, err := findFieldInTd(field, &t.Desc)
		if err != nil {
			return nil, err
		}
		result.Fields = append(result.Fields, t.Fields[fieldID])
		result.Desc.Fields = append(result.Desc.Fields, t.Desc.Fields[fieldID])
	}
	return result, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	_ = t.writeTo(&buf)
	return buf.String()
}

var winWidth = 120

func fmtCol(v string, nCols int) string {
	colWid := winWidth / nCols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// HeaderString Return a string representing the header of a table for a tuple with the supplied TupleDesc.
// Aligned indicates if the tuple should be formated in a tabular format
func (td *TupleDesc) HeaderString(aligned bool) string {
	outStr := ""
	for i, f := range td.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outStr = fmt.Sprintf("%s %s", outStr, fmtCol(tableName+f.Fname, len(td.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outStr = fmt.Sprintf("%s%s%s", outStr, sep, tableName+f.Fname)
		}
	}
	return outStr
}

// PrettyPrintString Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outStr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outStr = fmt.Sprintf("%s %s", outStr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outStr = fmt.Sprintf("%s%s%s", outStr, sep, str)
		}
	}
	return outStr
}

func PrintTuple(t *Tuple) string {
	if t == nil {
		return "Tuple{<nil>}"
	}
	return fmt.Sprintf("Tuple{%s}", t.PrettyPrintString(false))
}

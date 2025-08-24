package godb

import "fmt"

type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// NewInsertOp Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{insertFile: insertFile, child: child}
}

// Descriptor The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{Fields: []FieldType{{Fname: "count", Ftype: IntType}}}
}

// Iterator Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		var count int64
		for {
			tuple, err := childIter()
			if err != nil {
				return nil, fmt.Errorf("InsertOp.Iterator: %w", err)
			}
			if tuple == nil {
				break
			}

			err = iop.insertFile.insertTuple(tuple, tid)
			if err != nil {
				return nil, fmt.Errorf("InsertOp.Iterator: %w", err)
			}
			count++
		}

		return &Tuple{
			Desc:   *iop.Descriptor(),
			Fields: []DBValue{IntField{count}},
		}, nil
	}, nil
}

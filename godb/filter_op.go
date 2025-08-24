package godb

import (
	"fmt"
)

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// NewFilter Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Descriptor Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	return f.child.Descriptor()
}

// Iterator Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("Filter.Iterator: %w", err)
	}

	return func() (*Tuple, error) {
		for {
			tup, err := iter()
			if err != nil {
				return nil, fmt.Errorf("Filter.Iterator: %w", err)
			}
			if tup == nil {
				return nil, nil // No more tuples
			}

			leftVal, err := f.left.EvalExpr(tup)
			if err != nil {
				return nil, fmt.Errorf("Filter.Iterator: error evaluating left expression: %w", err)
			}
			rightVal, err := f.right.EvalExpr(tup)
			if err != nil {
				return nil, fmt.Errorf("Filter.Iterator: error evaluating right expression: %w", err)
			}

			if !leftVal.EvalPred(rightVal, f.op) {
				continue
			}
			return tup, nil
		}
	}, nil
}

package godb

import (
	"fmt"
)

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	limit     int64
	// Add additional fields here, if needed
}

// NewLimitOp Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	limField, _ := lim.EvalExpr(nil)
	return &LimitOp{child, lim, limField.(IntField).Value}
}

// Descriptor Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Iterator Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// Get the child iterator
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("failed to get child iterator: %v", err)
	}

	var count int64
	return func() (*Tuple, error) {
		if count >= l.limit {
			return nil, nil // Reached the limit
		}
		tup, err := childIter()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple from child: %v", err)
		}
		if tup == nil {
			return nil, nil
		}
		count++
		return tup, nil
	}, nil
}

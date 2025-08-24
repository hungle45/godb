package godb

import (
	"fmt"
)

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// NewJoin Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, left, right, maxBufferSize}, nil
}

// Descriptor Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [tupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	leftDesc := hj.left.Descriptor()
	rightDesc := hj.right.Descriptor()
	return leftDesc.merge(rightDesc)
}

// Iterator Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: optimize to pass the TestJoinBigOptional test
	var err error
	leftIter, err := joinOp.left.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("Join.Iterator: %w", err)
	}
	rightIter, err := joinOp.right.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("Join.Iterator: %w", err)
	}

	leftTupBuf := make([]*Tuple, joinOp.maxBufferSize)
	isInitLeftTupBuf := false
	leftTupBufIndex := joinOp.maxBufferSize
	var rightTup *Tuple

	return func() (*Tuple, error) {
		if !isInitLeftTupBuf {
			for i := 0; i < joinOp.maxBufferSize; i++ {
				leftTupBuf[i], err = leftIter()
				if err != nil {
					return nil, fmt.Errorf("Join.Iterator: %w", err)
				}
				if leftTupBuf[i] == nil {
					leftTupBuf[i] = nil
					break
				}
			}
			leftTupBufIndex = 0
			isInitLeftTupBuf = true

			rightTup, err = rightIter()
			if err != nil {
				return nil, fmt.Errorf("Join.Iterator: %w", err)
			}
		}
		for {
			if leftTupBufIndex >= joinOp.maxBufferSize || leftTupBuf[leftTupBufIndex] == nil {
				leftTupBufIndex = 0

				rightTup, err = rightIter()
				if err != nil {
					return nil, fmt.Errorf("Join.Iterator: %w", err)
				}
				if rightTup == nil {
					rightIter, err = joinOp.right.Iterator(tid)
					if err != nil {
						return nil, fmt.Errorf("Join.Iterator: %w", err)
					}
					rightTup, err = rightIter()
					if err != nil {
						return nil, fmt.Errorf("Join.Iterator: %w", err)
					}

					for i := 0; i < joinOp.maxBufferSize; i++ {
						leftTupBuf[i], err = leftIter()
						if err != nil {
							return nil, fmt.Errorf("Join.Iterator: %w", err)
						}
						if leftTupBuf[i] == nil {
							leftTupBuf[i] = nil
							break
						}
					}
				}
			}

			if rightTup == nil {
				return nil, nil
			}

			leftTup := leftTupBuf[leftTupBufIndex]
			if leftTup == nil {
				return nil, nil
			}
			leftTupBufIndex++

			leftVal, err := joinOp.leftField.EvalExpr(leftTup)
			if err != nil {
				return nil, fmt.Errorf("Join.Iterator: error evaluating left expression: %w", err)
			}

			rightVal, err := joinOp.rightField.EvalExpr(rightTup)
			if err != nil {
				return nil, fmt.Errorf("Join.Iterator: error evaluating right expression: %w", err)
			}

			if leftVal.EvalPred(rightVal, OpEq) {
				return joinTuples(leftTup, rightTup), nil
			}
		}
	}, nil
}

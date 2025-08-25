package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	asc     []bool // true for ascending, false for descending
}

// NewOrderBy Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	if len(orderByFields) == 0 {
		return nil, fmt.Errorf("empty order by fields")
	}
	if len(orderByFields) != len(ascending) {
		return nil, fmt.Errorf("mismatched lengths for order by fields and ascending list")
	}
	return &OrderBy{
		orderBy: orderByFields,
		child:   child,
		asc:     ascending,
	}, nil
}

// Descriptor Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Iterator Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	var (
		tuples []*Tuple
		index  int
		init   bool
	)

	return func() (*Tuple, error) {
		if !init {
			for {
				tuple, err := childIter()
				if err != nil {
					return nil, err
				}
				if tuple == nil {
					break
				}
				tuples = append(tuples, tuple)
			}

			sorter, err := newTupleSorter(tuples, o.orderBy, o.asc)
			if err != nil {
				return nil, err
			}
			sort.Sort(sorter)
			tuples = sorter.tuples
			init = true
		}

		if index >= len(tuples) {
			return nil, nil
		}
		tuple := tuples[index]
		index++
		return tuple, nil
	}, nil
}

type tupleSorter struct {
	tuples    []*Tuple
	orderBy   []Expr
	cacheExpr [][]DBValue
	asc       []bool
}

func newTupleSorter(tuples []*Tuple, orderBy []Expr, asc []bool) (*tupleSorter, error) {
	if len(orderBy) == 0 {
		return nil, fmt.Errorf("empty order by fields")
	}
	if len(orderBy) != len(asc) {
		return nil, fmt.Errorf("mismatched lengths for order by fields and ascending list")
	}

	cacheExpr := make([][]DBValue, len(tuples))
	for i, tuple := range tuples {
		cacheExpr[i] = make([]DBValue, len(orderBy))
		for j, expr := range orderBy {
			val, err := expr.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}
			cacheExpr[i][j] = val
		}
	}

	return &tupleSorter{
		tuples:    tuples,
		orderBy:   orderBy,
		cacheExpr: cacheExpr,
		asc:       asc,
	}, nil
}

func (ts *tupleSorter) Len() int {
	return len(ts.tuples)
}

func (ts *tupleSorter) Swap(i, j int) {
	ts.tuples[i], ts.tuples[j] = ts.tuples[j], ts.tuples[i]
	ts.cacheExpr[i], ts.cacheExpr[j] = ts.cacheExpr[j], ts.cacheExpr[i]
}

func (ts *tupleSorter) Less(i, j int) bool {
	for k := 0; k < len(ts.orderBy); k++ {
		valI := ts.cacheExpr[i][k]
		valJ := ts.cacheExpr[j][k]

		if valI.EvalPred(valJ, OpLt) {
			return ts.asc[k]
		} else if valI.EvalPred(valJ, OpGt) {
			return !ts.asc[k]
		}
	}
	return false
}

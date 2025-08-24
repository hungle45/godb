package godb

import (
	"fmt"
)

// AggState interface for an aggregation state
type AggState interface {
	// Init Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Copy Makes a copy of the aggregation state.
	Copy() AggState

	// AddTuple Adds a tuple to the aggregation state.
	AddTuple(*Tuple)

	// Finalize Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// GetTupleDesc Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// CountAggState Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(_ *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// SumAggState Implements the aggregation state for SUM
type SumAggState struct {
	alias string
	sum   int64
	expr  Expr
}

func (a *SumAggState) Copy() AggState {
	return &SumAggState{a.alias, a.sum, a.expr}
}

func intAggGetter(v DBValue) any {
	if intField, ok := v.(IntField); ok {
		return intField.Value
	}
	return 0
}

func stringAggGetter(v DBValue) any {
	if strField, ok := v.(StringField); ok {
		return strField.Value
	}
	return ""
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	if expr.GetExprType().Ftype != IntType {
		return fmt.Errorf("CountAggState.Init: expected IntType, got %v", expr.GetExprType().Ftype)
	}

	a.sum = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	intVal := intAggGetter(v)
	a.sum += intVal.(int64)
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.sum}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// AvgAggState Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	alias string
	sum   int64
	count int64
	expr  Expr
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{a.alias, a.sum, a.count, a.expr}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	if expr.GetExprType().Ftype != IntType {
		return fmt.Errorf("AvgAggState.Init: expected IntType, got %v", expr.GetExprType().Ftype)
	}

	a.sum = 0
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	intVal := intAggGetter(v)
	a.sum += intVal.(int64)
	a.count++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.sum / a.count}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// MaxAggState Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	alias string
	max   any
	expr  Expr
	vType DBType
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{a.alias, a.max, a.expr, a.vType}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	if expr.GetExprType().Ftype != IntType {
		return fmt.Errorf("MaxAggState.Init: expected IntType, got %v", expr.GetExprType().Ftype)
	}

	a.max = nil
	a.expr = expr
	a.alias = alias
	a.vType = expr.GetExprType().Ftype
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	switch a.vType {
	case IntType:
		f, ok := intAggGetter(v).(int64)
		if !ok {
			return
		}
		if a.max == nil {
			a.max = f
			return
		}
		if f > a.max.(int64) {
			a.max = f
		}
	case StringType:
		f, ok := stringAggGetter(v).(string)
		if !ok {
			return
		}
		if a.max == nil {
			a.max = f
			return
		}
		if f > a.max.(string) {
			a.max = f
		}
	default:
		return
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", a.expr.GetExprType().Ftype}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()

	var f DBValue
	switch a.vType {
	case IntType:
		f = IntField{a.max.(int64)}
	case StringType:
		f = StringField{a.max.(string)}
	default:
		return nil
	}

	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// MinAggState Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	alias string
	min   any
	expr  Expr
	vType DBType
}

func (a *MinAggState) Copy() AggState {
	return &MinAggState{a.alias, a.min, a.expr, a.vType}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.min = nil
	a.expr = expr
	a.alias = alias
	a.vType = expr.GetExprType().Ftype
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	v, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	switch a.vType {
	case IntType:
		f, ok := intAggGetter(v).(int64)
		if !ok {
			return
		}
		if a.min == nil {
			a.min = f
			return
		}
		if f < a.min.(int64) {
			a.min = f
		}
	case StringType:
		f, ok := stringAggGetter(v).(string)
		if !ok {
			return
		}
		if a.min == nil {
			a.min = f
			return
		}
		if f < a.min.(string) {
			a.min = f
		}
	default:
		return
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", a.expr.GetExprType().Ftype}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()

	var f DBValue
	switch a.vType {
	case IntType:
		f = IntField{a.min.(int64)}
	case StringType:
		f = StringField{a.min.(string)}
	default:
		return nil
	}

	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

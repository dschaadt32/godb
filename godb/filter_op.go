package godb

import "golang.org/x/exp/constraints"

type Filter[T constraints.Ordered] struct {
	op     BoolOp
	left   Expr
	right  Expr
	child  Operator
	getter func(DBValue) T
}

func intFilterGetter(v DBValue) int64 {
	intV := v.(IntField)
	return intV.Value
}

func stringFilterGetter(v DBValue) string {
	stringV := v.(StringField)
	return stringV.Value
}

// Constructor for a filter operator on ints
func NewIntFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[int64], error) {
	if constExpr.GetExprType().Ftype != IntType || field.GetExprType().Ftype != IntType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply int filter to non int-types"}
	}
	f, err := newFilter[int64](constExpr, op, field, child, intFilterGetter)
	return f, err
}

// Constructor for a filter operator on strings
func NewStringFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[string], error) {
	if constExpr.GetExprType().Ftype != StringType || field.GetExprType().Ftype != StringType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply string filter to non string-types"}
	}
	f, err := newFilter[string](constExpr, op, field, child, stringFilterGetter)
	return f, err
}

// Getter is a function that reads a value of the desired type
// from a field of a tuple
// This allows us to have a generic interface for filters that work
// with any ordered type
func newFilter[T constraints.Ordered](constExpr Expr, op BoolOp, field Expr, child Operator, getter func(DBValue) T) (*Filter[T], error) {
	return &Filter[T]{op, field, constExpr, child, getter}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter[T]) Descriptor() *TupleDesc {
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over
// the results of the child iterator and return a tuple if it satisfies
// the predicate.
// HINT: you can use the evalPred function defined in types.go to compare two values
func (f *Filter[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	child, err := f.childIter(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			tup, err := child()
			if err != nil {
				return nil, err
			}
			if tup == nil { // out of tuples
				return nil, nil
			}
			left, err := f.left.EvalExpr(tup)
			if err != nil {
				return nil, err
			}
			right, err := f.right.EvalExpr(tup)
			if err != nil {
				return nil, err
			}
			if evalPred[T](f.getter(left), f.getter(right), f.op) {
				return tup, nil
			}
		}
	}, nil
}

func (f *Filter[T]) childIter(tid TransactionID) (func() (*Tuple, error), error) {
	return f.child.Iterator(tid)
}

// // Filter operator implementation. This function should iterate over
// // the results of the child iterator and return a tuple if it satisfies
// // the predicate.
// // HINT: you can use the evalPred function defined in types.go to compare two values
// func (f *Filter[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
// 	mu := new(sync.Mutex) // Mutex for synchronization
// 	buffer := []*Tuple{}

// 	child, err := f.childIter(tid)
// 	if err != nil {
// 		return nil, err
// 	}

// 	go func() error {
// 		for {
// 			tup, err := child()
// 			if err != nil {
// 				return err
// 			}
// 			if tup == nil { // out of tuples
// 				mu.Lock()
// 				buffer = append(buffer, nil)
// 				mu.Unlock()
// 				return nil
// 			}
// 			left, err := f.left.EvalExpr(tup)
// 			if err != nil {
// 				return err
// 			}
// 			right, err := f.right.EvalExpr(tup)
// 			if err != nil {
// 				return err
// 			}
// 			if evalPred[T](f.getter(left), f.getter(right), f.op) {
// 				mu.Lock()
// 				buffer = append(buffer, tup)
// 				mu.Unlock()
// 			}
// 		}
// 	}()
// 	pos2 := 0
// 	return func() (*Tuple, error) {
// 		for {

// 			if pos2 < len(buffer) {
// 				mu.Lock()
// 				tup := buffer[pos2]
// 				mu.Unlock()
// 				// possibly remove from the end for time complexity, have a pointer to the position in the buffer to return
// 				pos2++
// 				//

// 				return tup, nil
// 			}

// 		}

// 	}, nil
// }

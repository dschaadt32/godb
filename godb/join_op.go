package godb

import "sync"

type EqualityJoin[T comparable] struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator //operators for the two inputs of the join

	// Function that when applied to a DBValue returns the join value; will be
	// one of intFilterGetter or stringFilterGetter
	getter func(DBValue) T

	// The maximum number of records of intermediate state that the join should use
	// (only required for optional exercise)
	maxBufferSize int
}

// Constructor for a  join of integer expressions
// Returns an error if either the left or right expression is not an integer
func NewIntJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[int64], error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return nil, GoDBError{TypeMismatchError, "join field is not an int"}
	case IntType:
		return &EqualityJoin[int64]{leftField, rightField, &left, &right, intFilterGetter, maxBufferSize}, nil
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Constructor for a  join of string expressions
// Returns an error if either the left or right expression is not a string
func NewStringJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin[string], error) {

	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, GoDBError{TypeMismatchError, "can't join fields of different types"}
	}
	switch leftField.GetExprType().Ftype {
	case StringType:
		return &EqualityJoin[string]{leftField, rightField, &left, &right, stringFilterGetter, maxBufferSize}, nil
	case IntType:
		return nil, GoDBError{TypeMismatchError, "join field is not a string"}
	}
	return nil, GoDBError{TypeMismatchError, "unknown type"}
}

// Return a TupleDescriptor for this join. The returned descriptor should contain
// the union of the fields in the descriptors of the left and right operators.
// HINT: use the merge function you implemented for TupleDesc in lab1
func (hj *EqualityJoin[T]) Descriptor() *TupleDesc {
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())

}

// Join operator implementation.  This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
// HINT: When implementing the simple nested loop join, you should keep in mind that
// you only iterate through the left iterator once (outer loop) but iterate through the right iterator
// once for every tuple in the the left iterator (inner loop).
// HINT: You can use joinTuples function you implemented in lab1 to join two tuples.
//
// OPTIONAL EXERCISE:  the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out.  To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	left, err := joinOp.leftIter(tid)
	if err != nil {
		return nil, err
	}
	right, err := joinOp.rightIter(tid)
	if err != nil {
		return nil, err
	}

	mu := new(sync.Mutex) // Mutex for synchronization
	buffer := []*Tuple{}
	// pos := 0
	go func() error {
		for {
			leftTup, err := left()
			if err != nil {
				return err
			}
			if leftTup == nil {
				// mu.Lock()
				// buffer[pos] = nil
				mu.Lock()
				buffer = append(buffer, nil)
				mu.Unlock()
				// pos++
				// mu.Unlock()
				return nil
			}

			func(leftTup *Tuple) error {
				for {
					rightTup, err := right()
					if err != nil || rightTup == nil {
						right, err = joinOp.rightIter(tid)
						if err != nil {
							return err
						}
						break
					}

					leftVal, err := joinOp.leftField.EvalExpr(leftTup)
					if err != nil {
						return err
					}
					leftFieldVal := joinOp.getter(leftVal)

					rightVal, err := joinOp.rightField.EvalExpr(rightTup)
					if err != nil {
						return err
					}
					rightFieldVal := joinOp.getter(rightVal)

					if leftFieldVal == rightFieldVal {
						mu.Lock()
						// buffer[pos] = joinTuples(leftTup, rightTup)
						// pos++
						buffer = append(buffer, joinTuples(leftTup, rightTup))
						mu.Unlock()
					}
				}
				return nil
			}(leftTup)

		}
		return nil
	}()

	pos2 := 0
	return func() (*Tuple, error) {
		for {

			if pos2 < len(buffer) {
				mu.Lock()
				tup := buffer[pos2]
				mu.Unlock()
				// possibly remove from the end for time complexity, have a pointer to the position in the buffer to return
				pos2++
				//

				return tup, nil
			}

		}

	}, nil
}

func (joinOp *EqualityJoin[T]) leftIter(tid TransactionID) (func() (*Tuple, error), error) {
	return (*joinOp.left).Iterator(tid)
}
func (joinOp *EqualityJoin[T]) rightIter(tid TransactionID) (func() (*Tuple, error), error) {
	return (*joinOp.right).Iterator(tid)
}

// right, err := joinOp.rightIter(tid)
// if err != nil {
// 	return nil, err
// }
// left, err := joinOp.leftIter(tid)

// if err != nil {
// 	return nil, err
// }
// leftTup, err := left()
// if err != nil {
// 	//fmt.Printf("in err under left iter")
// 	return nil, err
// }

// buffer := []*Tuple{}
// for {
// 	//fmt.Printf("in left")

// 	if leftTup == nil { // out of tuples
// 		buffer = append(buffer, nil)
// 		//return nil, nil
// 		break
// 	}
// 	for {
// 		rightTup, err := right()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if rightTup == nil { // out of tuples
// 			right, err = joinOp.rightIter(tid) // reset

// 			if err != nil {
// 				return nil, err
// 			}
// 			leftTup, err = left()

// 			if err != nil {
// 				return nil, err
// 			}
// 			break // get next tuple from left
// 		}
// 		// else actually do the join
// 		leftVal, err := joinOp.leftField.EvalExpr(leftTup)
// 		if err != nil {
// 			return nil, err
// 		}
// 		leftFieldVal := joinOp.getter(leftVal)

// 		rightVal, err := joinOp.rightField.EvalExpr(rightTup)
// 		if err != nil {
// 			return nil, err
// 		}
// 		rightFieldVal := joinOp.getter(rightVal)
// 		//fmt.Printf("left = %T, right = %T\n", (leftFieldVal), (rightFieldVal))
// 		if leftFieldVal == rightFieldVal {
// 			buffer = append(buffer, joinTuples(leftTup, rightTup))

// 		}
// 	}
// }

// return func() (*Tuple, error) {
// 	for {
// 		if len(buffer) != 0 {
// 			tup := buffer[0]
// 			remove(buffer, 0)
// 			return tup, nil
// 		}
// 	}
// }, nil

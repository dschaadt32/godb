package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor() // not sure about this

}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		//for {
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		if tup == nil { // out of tuples
			return nil, nil
		}

		val, _ := l.limitTups.EvalExpr(tup)
		if count == int(val.(IntField).Value) {
			return nil, nil
		}
		count += 1

		return tup, nil
	}, nil
}

package godb

type DeleteOp struct {
	deleteFile DBFile
	child      Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{deleteFile: deleteFile, child: child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	fields := []FieldType{{"count", "", IntType}}
	return &TupleDesc{Fields: fields}

}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	iter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			tup, err := iter()
			if err != nil {
				return nil, err
			}
			if tup == nil { // out of tuples
				//println("count = ", count)
				return &Tuple{*dop.Descriptor(), []DBValue{IntField{int64(count)}}, nil}, nil
			}
			err = dop.deleteFile.deleteTuple(tup, tid)
			if err != nil {
				return nil, err
			}
			count += 1
		}

	}, nil

}

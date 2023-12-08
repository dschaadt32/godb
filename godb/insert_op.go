package godb

// TODO: some code goes here
type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {

	return &InsertOp{insertFile: insertFile, child: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	fields := []FieldType{{"count", "", IntType}}
	return &TupleDesc{Fields: fields}

}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	iter, err := iop.child.Iterator(tid)
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
				return &Tuple{*iop.Descriptor(), []DBValue{IntField{int64(count)}}, nil}, nil
			}
			iop.insertFile.insertTuple(tup, tid)
			//println("tuple inserted", *tid, "val =", tup.Fields[1].(IntField).Value)
			count += 1
		}

	}, nil

}

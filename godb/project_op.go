package godb

import "sync"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator

	// TODO: some code goes here
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, GoDBError{0, "select fields and outputNames should be same length"}
	}
	return &Project{selectFields: selectFields, outputNames: outputNames, child: child}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fields := []FieldType{}
	for i := range p.selectFields {
		fields = append(fields, p.selectFields[i].GetExprType())
		//println(p.outputNames[i])
		fields[i].Fname = p.outputNames[i]
	}
	return &TupleDesc{fields}

}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	muIter := new(sync.Mutex) // Mutex for synchronization
	buffer := []*Tuple{}

	iter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	go func() error {
		for {
			tup, err := iter()
			if err != nil {
				return err
			}
			if tup == nil {
				muIter.Lock()
				buffer = append(buffer, nil)
				muIter.Unlock()
				return nil
			}

			fields := make([]DBValue, len(p.selectFields))
			for i, expr := range p.selectFields {
				val, _ := expr.EvalExpr(tup)
				fields[i] = val
			}

			muIter.Lock()
			buffer = append(buffer, &Tuple{*p.Descriptor(), fields, nil})
			muIter.Unlock()

		}
	}()

	pos2 := 0
	return func() (*Tuple, error) {
		for {

			if pos2 < len(buffer) {
				muIter.Lock()
				tup := buffer[pos2]
				muIter.Unlock()
				// possibly remove from the end for time complexity, have a pointer to the position in the buffer to return
				pos2++
				//

				return tup, nil
			}

		}

	}, nil
}

// return func() (*Tuple, error) {
// 	//for {
// 	tup, err := iter()
// 	if err != nil {
// 		return nil, err
// 	}
// 	if tup == nil { // out of tuples
// 		return nil, nil
// 	}
// 	fields := []DBValue{}
// 	for i := range p.selectFields {
// 		val, _ := p.selectFields[i].EvalExpr(tup)
// 		fields = append(fields, val)
// 	}
// 	return &Tuple{*p.Descriptor(), fields, nil}, nil

// 	//return *Tuple{*p.Descriptor(), fields}, nil
// 	//}

// }, nil

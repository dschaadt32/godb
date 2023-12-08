package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil

}

func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

type lessFunc func(p1, p2 DBValue) bool

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	index := -1
	vals := []Tuple{}
	iter, _ := o.child.Iterator(tid)
	tup, _ := iter()
	for tup != nil {
		vals = append(vals, *tup) // get all the tuples
		tup, _ = iter()
	}
	lessFuncs := []lessFunc{}
	for range o.ascending {

		lessThan := func(c1, c2 DBValue) bool {

			switch c1.(type) {
			case IntField:
				return (c1.(IntField).Value < c2.(IntField).Value)
			default:
				return (c1.(StringField).Value < c2.(StringField).Value)

			}
		}
		lessFuncs = append(lessFuncs, lessThan)
	}

	ms := multiSorter{tuples: vals, exprs: o.orderBy, less: lessFuncs, ascending: o.ascending}

	ms.Sort(vals)

	return func() (*Tuple, error) {
		if index == len(vals)-1 {
			return nil, nil
		}
		index++
		return &vals[index], nil
	}, nil
}

// multiSorter implements the Sort interface, sorting the changes within.
type multiSorter struct {
	tuples    []Tuple
	exprs     []Expr
	less      []lessFunc
	ascending []bool
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ms *multiSorter) Sort(tuples []Tuple) {
	ms.tuples = tuples
	sort.Sort(ms)
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(less []lessFunc, exprs []Expr) *multiSorter {
	return &multiSorter{
		less:  less,
		exprs: exprs,
	}
}

// Len is part of sort.Interface.
func (ms *multiSorter) Len() int {
	return len(ms.tuples)
}

// Swap is part of sort.Interface.
func (ms *multiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *multiSorter) Less(i, j int) bool {
	p, q := &ms.tuples[i], &ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		evalp, _ := ms.exprs[k].EvalExpr(p)
		evalq, _ := ms.exprs[k].EvalExpr(q)
		switch {
		case less(evalp, evalq):
			// p < q, so we have a decision.
			if ms.ascending[k] {
				return true
			} else {
				return false
			}

		case less(evalq, evalp):
			// p > q, so we have a decision.
			if ms.ascending[k] {
				return false
			} else {
				return true
			}
		}
		//fmt.Println(p, q)
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	//fmt.Println(p, q)
	// evalp, _ := ms.exprs[k].EvalExpr(p)
	// evalq, _ := ms.exprs[k].EvalExpr(q)
	return !ms.ascending[k] // dont want it to switch places
	//return ms.less[k](evalp, evalq)
}

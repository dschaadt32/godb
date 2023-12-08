package godb

import (
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	//Create buffer pool
	bp := NewBufferPool(10)

	os.Remove("myfilediff.dat")
	hf, err := NewHeapFile("myfilediff.dat", &td, bp)
	if err != nil {
		return -1, err
	}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return -1, err
	}
	err = hf.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return -1, err
	}
	//find the column
	fieldNo, err := findFieldInTd(FieldType{sumField, "", IntType}, &td)
	if err != nil {
		return -1, err
	}
	if td.Fields[fieldNo].Ftype != IntType {
		return -1, GoDBError{0, "field type not int"}
	}
	//Start a transaction -> we will do the implementation in another lab
	tid := NewTID()
	bp.BeginTransaction(tid)
	iter, err := hf.Iterator(tid)
	if err != nil {
		return -1, err
	}

	//Iterate through the tuples and sum them up.
	sum := 0
	for {
		tup, err := iter()
		if err != nil {
			return -1, err
		}
		if tup == nil {
			break
		}
		f := tup.Fields[fieldNo].(IntField)
		sum += int(f.Value)
	}
	//bp.CommitTransaction() //commit transaction
	return sum, nil //return the value
}

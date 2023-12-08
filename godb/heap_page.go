package godb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	pageNo     int
	heapFile   *HeapFile
	tupleSlice []*Tuple
	numSlots   int
	dirty      bool
	TupleDesc  *TupleDesc
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	var remPageSize = PageSize - 8
	var bytesPerTuple = 0
	//var tupleSlice []Tuple

	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Ftype == StringType {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		} else { // IntType
			bytesPerTuple += ((int)(unsafe.Sizeof(int64(0))))
		}
	}
	var numSlots = remPageSize / bytesPerTuple
	var tupleSlice = make([]*Tuple, 0)
	//var tupleSlice = make(*[]Tuple, numSlots)
	return &heapPage{pageNo: pageNo, heapFile: f, tupleSlice: tupleSlice, numSlots: numSlots, dirty: false, TupleDesc: desc}
}

func (h *heapPage) getNumSlots() int {
	return h.numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	var index = len(h.tupleSlice)

	if index >= h.numSlots { // no free slots
		return Actual_rid{slotIndex: index, pageNum: h.pageNo}, GoDBError{3, "no free slots to insert"}
	}
	// else insert the tuple at the next index
	(h.tupleSlice) = append((h.tupleSlice), t)
	t.Rid = Actual_rid{slotIndex: index, pageNum: h.pageNo}
	h.setDirty(true)
	return Actual_rid{slotIndex: index, pageNum: h.pageNo}, nil //return the slice index of the tuple
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {

	// else insert the tuple at the next index
	var tupleSlice = h.tupleSlice
	actual_rid1 := rid.(Actual_rid)
	var return_tuple []*Tuple
	var flag = false
	for i := 0; i < len(tupleSlice); i++ {
		tup_rid := tupleSlice[i].Rid.(Actual_rid)
		if tup_rid.slotIndex != actual_rid1.slotIndex { // if this one is not rid
			return_tuple = append(return_tuple, tupleSlice[i])
		} else {
			flag = true // this one is rid
		}

	}
	if !flag {
		return GoDBError{0, "rid not in tupleslice"}
	}

	h.tupleSlice = return_tuple
	h.setDirty(true)
	//return GoDBError{0, fmt.Sprint(len(return_tuple))}
	return nil

}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var f DBFile = p.heapFile
	return &f //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.

// In addition, all pages are PageSize bytes.  They begin with a header with a 32
// bit integer with the number of slots (tuples), and a second 32 bit integer with
// the number of used slots.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	//return nil, GoDBError{}
	b := new(bytes.Buffer)
	var err = binary.Write(b, binary.LittleEndian, int32(h.numSlots)) // write num of slots
	if err != nil {
		return b, err
	}
	var err2 = binary.Write(b, binary.LittleEndian, int32(len(h.tupleSlice))) // write num of used slots
	if err2 != nil {
		return b, err2
	}
	for i := 0; i < len(h.tupleSlice); i++ {
		var tupleSlice = h.tupleSlice
		tupleSlice[i].writeTo(b)
	}

	//fmt.Println("Serialized page ", h.pageNo, " with ", len(h.tupleSlice), " tuples")
	return b, nil

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var uVarnumSlots int32
	var used int32
	binary.Read(buf, binary.LittleEndian, &uVarnumSlots)
	binary.Read(buf, binary.LittleEndian, &used)
	//fmt.Println("Init from buffer page ", h.pageNo, " has ", used)

	//var uVarnumSlots, _ = binary.Uvarint(header[0:4])

	h.numSlots = int(uVarnumSlots)
	// can't I just calculate the slots used from the len of the array?
	// h., _ = binary.Uvarint(header[4:8])
	tups := make([]*Tuple, used)
	var err error
	for i := 0; i < int(used); i++ {
		tups[i], err = readTupleFrom(buf, h.TupleDesc)
		if err != nil {
			return err
		}
		tup_rid := tups[i].Rid.(Actual_rid)
		tup_rid.slotIndex = i
		tup_rid.pageNum = h.pageNo
	}
	h.tupleSlice = tups
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	index := -1 // dont love this
	return func() (*Tuple, error) {
		index += 1
		if index >= len(p.tupleSlice) { // at the end
			return nil, nil
		}
		var tupleSlice = p.tupleSlice
		tupleSlice[index].Rid = Actual_rid{slotIndex: index, pageNum: p.pageNo} // set rid
		return tupleSlice[index], nil

	}

}

package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	file      *os.File
	heapPages []heapPage
	tupleDesc *TupleDesc

	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool            *BufferPool
	heapFileMutex      sync.Mutex
	is_full_continuous bool
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	var hp = make([]heapPage, 0)
	file, _ := os.OpenFile(fromFile, os.O_CREATE|os.O_RDWR, 0644)

	return &HeapFile{file: file, heapPages: hp, tupleDesc: td, bufPool: bp, is_full_continuous: false}, nil
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// should use File.Stat()
	s, _ := f.file.Stat()
	return int(s.Size()) / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, Actual_rid{pageNum: f.NumPages(), slotIndex: 0}}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {

	var b = make([]byte, PageSize) // need size of page here
	//fmt.Println("Reading page ", pageNo, "from ", int64(PageSize*pageNo))
	_, err := f.file.ReadAt(b, int64(PageSize*pageNo))
	//fmt.Println("Page header: ", b[0:8])
	if err != nil { // page doesnt exist
		return nil, err
	}

	var h *heapPage = newHeapPage(f.tupleDesc, pageNo, f)
	var buf *bytes.Buffer = bytes.NewBuffer(b)
	h.initFromBuffer(buf)
	var p Page = h
	return &p, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	f.heapFileMutex.Lock()
	start_pageno := 0
	if f.is_full_continuous {
		start_pageno = f.NumPages() - 1
	}
	f.heapFileMutex.Unlock()
	//fmt.Println("Insert Tuple. Num pages: ", f.NumPages())
	for pageno := start_pageno; pageno < f.NumPages(); pageno++ {
		pagepointer, err := f.bufPool.GetPage(f, pageno, tid, WritePerm) // error is in here
		//fmt.Println("Get page error: ", err)
		if err == nil {

			page := (*pagepointer).(*heapPage)
			_, err = page.insertTuple(t)
			page.setDirty(true)
			//fmt.Println("InsertTuple error: ", err)
			if err == nil {
				return nil // actually got inserted
			}
		} else {
			return err
		}

	}
	f.heapFileMutex.Lock()
	newPageIndex := f.NumPages()
	var newPage Page = newHeapPage(f.tupleDesc, newPageIndex, f)

	f.flushPage(&newPage)
	f.heapFileMutex.Unlock()
	//fmt.Println("Creating new page because full", newPageIndex)

	page, err := f.bufPool.GetPage(f, newPageIndex, tid, WritePerm)

	if err != nil {
		//fmt.Println("Error inserting tuple into fresh page", err)
		return err
	}

	heap_page := (*page).(*heapPage)
	_, err = heap_page.insertTuple(t)
	heap_page.setDirty(true)
	if err != nil {
		//fmt.Println("Error inserting tuple into fresh page", err)
		return err
	}
	f.is_full_continuous = true

	return nil

}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	f.heapFileMutex.Lock()
	defer f.heapFileMutex.Unlock()

	t_rid := t.Rid.(Actual_rid)
	pagepointer, err := f.bufPool.GetPage(f, t_rid.pageNum, tid, WritePerm)

	if err != nil {
		return err
	}

	page := (*pagepointer).(*heapPage)
	err = page.deleteTuple(t_rid)
	if err != nil {
		page.setDirty(true)
		return err
	}
	f.is_full_continuous = false
	page.setDirty(true)
	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	// make sure the page is pagesize bits longs (pad with extra zeros), easy to find how many pages you have

	page := (*p).(*heapPage)

	b, err := page.toBuffer()
	if err != nil {
		return GoDBError{0, "page to buffer failed in flushPage"}
	}

	bSlice := b.Bytes()
	for i := 0; len(bSlice) < PageSize; i++ {
		bSlice = append(bSlice, 0)
	}

	//fmt.Println("Length of buffer: ", len(bSlice))

	if err != nil {
		return err
	}
	//fmt.Println("at flush ", page.pageNo, " to pos ", int64(PageSize*page.pageNo))
	//fmt.Println("Page header: ", bSlice[0:8])
	_, err = f.file.WriteAt(bSlice, int64(PageSize*page.pageNo))
	_, _ = f.file.Stat()
	//fmt.Println("Wrote to file: ", n, err, a.Size())
	if err != nil {
		return err
	}

	page.setDirty(false)

	return nil //replace me
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {

	return f.tupleDesc // aint no way

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// f.heapFileMutex.Lock()
	// defer f.heapFileMutex.Unlock()
	pageNo := 0
	//println("page in iterator-", *tid)
	//println("page set in iterator", *tid)
	var pagepointer, err = f.bufPool.GetPage(f, pageNo, tid, ReadPerm)

	//println("page in iterator+", *tid)
	if err != nil {
		//println("in error iterator")

		return nil, err
	}

	page := (*pagepointer).(*heapPage)

	iter := f.iteratorHelper(page)
	return func() (*Tuple, error) {
		for {
			if pageNo >= f.NumPages() { // done
				return nil, nil
			}
			tup, err := iter()
			//fmt.Println(tup, err)
			if tup != nil || err != nil { // not at end of page
				//fmt.Println("Iterator returning: ", tup)
				return tup, err
			}
			pageNo += 1
			if pageNo >= f.NumPages() {
				return nil, nil
			}
			pagepointer, err = f.bufPool.GetPage(f, pageNo, tid, ReadPerm) // note sure about perm here
			if err != nil {
				return nil, err
			}
			page = (*pagepointer).(*heapPage)
			iter = f.iteratorHelper(page)

		}
	}, nil
}

func (f *HeapFile) iteratorHelper(page *heapPage) func() (*Tuple, error) {
	iter := page.tupleIter()
	return iter
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) heapHash {
	return heapHash{FileName: f.file.Name(), PageNo: pgNo}
}

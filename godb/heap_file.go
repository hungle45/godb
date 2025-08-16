package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	backingFile string
	bufPool     *BufferPool
	tupDesc     *TupleDesc
}

// NewHeapFile Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pool read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	_, err := os.Stat(fromFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("NewHeapFile: could not open file %s: %v", fromFile, err)
		}
		file, err := os.Create(fromFile)
		defer file.Close()
		if err != nil {
			return nil, fmt.Errorf("NewHeapFile: could not create file %s: %v", fromFile, err)
		}
	}

	return &HeapFile{
		backingFile: fromFile,
		bufPool:     bp,
		tupDesc:     td,
	}, nil
}

// BackingFile Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.backingFile
}

// NumPages Return the number of pool in the heap file
func (f *HeapFile) NumPages() int {
	stat, _ := os.Stat(f.backingFile)
	return int(math.Floor(float64(stat.Size() / int64(PageSize))))
}

// LoadFromCSV Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
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
			return Error{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return Error{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
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
					return Error{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
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
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pool to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	file, err := os.Open(f.backingFile)
	if err != nil {
		return nil, fmt.Errorf("readPage: could not open file %s: %v", f.backingFile, err)
	}
	defer file.Close()

	offset := int64(pageNo * PageSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("readPage: could not seek to offset %d in file %s: %v", offset, f.backingFile, err)
	}

	buf := make([]byte, PageSize)
	_, err = file.Read(buf)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("readPage: reached end of file %s before reading full page %d", f.backingFile, pageNo)
		}
		return nil, fmt.Errorf("readPage: could not read page %d from file %s: %v", pageNo, f.backingFile, err)
	}

	page, err := newHeapPage(f.tupDesc, pageNo, f)
	if err != nil {
		return nil, fmt.Errorf("readPage: could not create new heapPage for page %d: %v", pageNo, err)
	}
	if err = page.initFromBuffer(bytes.NewBuffer(buf)); err != nil {
		return nil, fmt.Errorf("readPage: could not initialize heapPage from buffer for page %d: %v", pageNo, err)
	}

	return page, nil
}

// Add the tuple to the HeapFile. This method should search through pool in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pool, it should use the [BufferPool.GetPage method]
// rather than directly reading pool itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	for i := 0; i < f.NumPages(); i++ {
		page, err := f.bufPool.GetPage(f, i, tid, WritePerm)
		if err != nil {
			return fmt.Errorf("insertTuple: could not get page %d: %v", i, err)
		}

		hp, ok := page.(*heapPage)
		if !ok {
			return fmt.Errorf("insertTuple: page %d is not a heapPage", i)
		}

		if !hp.isFull() {
			_, err := hp.insertTuple(t)
			return err
		}
	}

	newPage, err := newHeapPage(f.tupDesc, f.NumPages(), f)
	if err != nil {
		return fmt.Errorf("insertTuple: could not create new heapPage: %v", err)
	}
	if _, err := newPage.insertTuple(t); err != nil {
		return fmt.Errorf("insertTuple: could not insert tuple into new heapPage: %v", err)
	}

	if err := f.flushPage(newPage); err != nil {
		return fmt.Errorf("insertTuple: could not flush new heapPage to file: %v", err)
	}

	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid, ok := t.Rid.(heapRecordID)
	if !ok {
		return fmt.Errorf("deleteTuple: expected heapRecordID, got %T", t.Rid)
	}

	page, err := f.bufPool.GetPage(f, rid.PageNo, tid, WritePerm)
	if err != nil {
		return fmt.Errorf("deleteTuple: could not get page %d: %v", rid.PageNo, err)
	}

	hp, ok := page.(*heapPage)
	if !ok {
		return fmt.Errorf("deleteTuple: page %d is not a heapPage", rid.PageNo)
	}
	if err := hp.deleteTuple(rid); err != nil {
		return fmt.Errorf("deleteTuple: could not delete tuple with rid %v from page %d: %v", rid, rid.PageNo, err)
	}

	if err := f.flushPage(page); err != nil {
		return fmt.Errorf("deleteTuple: could not flush page %d after deleting tuple: %v", rid.PageNo, err)
	}
	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	hp, ok := p.(*heapPage)
	if !ok {
		return fmt.Errorf("flushPage: expected heapPage, got %T", p)
	}

	file, err := os.OpenFile(f.backingFile, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("flushPage: could not open file %s for writing: %v", f.backingFile, err)
	}
	defer file.Close()

	offset := int64(hp.pageNo * PageSize)
	if _, err = file.Seek(offset, 0); err != nil {
		return fmt.Errorf("flushPage: could not seek to offset %d in file %s: %v", offset, f.backingFile, err)
	}

	buf, err := hp.toBuffer()
	if err != nil {
		return fmt.Errorf("flushPage: could not write heapPage to buffer: %v", err)
	}

	if _, err = file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("flushPage: could not write heapPage to file %s at offset %d: %v", f.backingFile, offset, err)
	}

	hp.setDirty(0, false)
	return nil
}

// Descriptor [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.tupDesc

}

// Iterator [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pool from the HeapFile using the
// BufferPool method GetPage, rather than reading pool directly,
// since the BufferPool caches pool and manages page-level locking state for
// transactions
// You should ensure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	numPages := f.NumPages()
	curPageNo := 0
	var tupleIter func() (*Tuple, error)

	return func() (*Tuple, error) {
		if curPageNo >= numPages {
			return nil, nil
		}

		for curPageNo < numPages {
			if tupleIter != nil {
				tuple, err := tupleIter()
				if err != nil {
					return nil, fmt.Errorf("Iterator: error iterating tuples on page %d: %v", curPageNo, err)
				}
				if tuple != nil {
					return tuple, nil
				}
				curPageNo++
				tupleIter = nil
				continue
			}

			page, err := f.bufPool.GetPage(f, curPageNo, tid, ReadPerm)
			if err != nil {
				return nil, fmt.Errorf("Iterator: could not get page %d: %v", curPageNo, err)
			}
			hp, ok := page.(*heapPage)
			if !ok {
				return nil, fmt.Errorf("Iterator: page %d is not a heapPage", curPageNo)
			}
			tupleIter = hp.tupleIter()
		}

		return nil, nil
	}, nil
}

// internal structure to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{
		FileName: f.backingFile,
		PageNo:   pgNo,
	}
}

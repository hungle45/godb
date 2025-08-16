package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/* HeapPage implements the Page interface for pool of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a tupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pool are PageSize bytes.  They begin with a header with a 32
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

You will follow the inverse process to read pool from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	numSlots     int
	numUsedSlots int
	pageNo       int
	tupleDesc    *TupleDesc
	tuples       []*Tuple
	file         *HeapFile
	isDirtyFlag  bool
}

// heapRecordID is a record ID for a tuple in a heap page.
type heapRecordID struct {
	PageNo int // No of the page this tuple is on
	SlotNo int // Slot No of the tuple on the page
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	tupleSize := desc.getTupleSizeInBytes()
	remPageSize := PageSize - 8
	numSlots := remPageSize / tupleSize
	if numSlots <= 0 {
		return nil, fmt.Errorf("heapPage: tuple size %d is too large for page size %d", tupleSize, PageSize)
	}

	return &heapPage{
		numSlots:     numSlots,
		numUsedSlots: 0,
		pageNo:       pageNo,
		tupleDesc:    desc,
		tuples:       make([]*Tuple, numSlots),
		file:         f,
	}, nil
}

func (h *heapPage) getNumSlots() int {
	return h.numSlots
}

func (h *heapPage) isFull() bool {
	return h.numUsedSlots >= h.numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.isFull() {
		return heapRecordID{}, Error{PageFullError, fmt.Sprintf("insertTuple: no free slots available on page")}
	}

	for i := 0; i < h.numSlots; i++ {
		if h.tuples[i] == nil {
			h.tuples[i] = t
			h.numUsedSlots++
			t.Rid = heapRecordID{PageNo: h.pageNo, SlotNo: i}
			break
		}

	}

	h.isDirtyFlag = true
	return t.Rid, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	hRid, ok := rid.(heapRecordID)
	if !ok {
		return Error{TypeMismatchError, fmt.Sprintf("deleteTuple: expected heapRecordID, got %T", hRid)}
	}

	if hRid.PageNo != h.pageNo || hRid.SlotNo < 0 || hRid.SlotNo >= h.numSlots {
		return Error{TupleNotFoundError, fmt.Sprintf("deleteTuple: invalid record ID %v", hRid)}
	}

	if h.tuples[hRid.SlotNo] == nil {
		return Error{TupleNotFoundError, fmt.Sprintf("deleteTuple: no tuple found at record ID %v", hRid)}
	}

	h.tuples[hRid.SlotNo] = nil
	h.numUsedSlots--
	h.isDirtyFlag = true
	return nil
}

// Page method - return whether the page is dirty
func (h *heapPage) isDirty() bool {
	return h.isDirtyFlag
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.isDirtyFlag = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, int32(h.numSlots)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(h.numUsedSlots)); err != nil {
		return nil, err
	}

	for _, t := range h.tuples {
		if t == nil {
			continue
		}
		if err := t.writeTo(buf); err != nil {
			return nil, err
		}
	}

	if buf.Len() < PageSize {
		padding := make([]byte, PageSize-buf.Len())
		if _, err := buf.Write(padding); err != nil {
			return nil, err
		}
	}

	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var (
		err          error
		numSlots     int32
		numUsedSlots int32
	)
	if err = binary.Read(buf, binary.LittleEndian, &numSlots); err != nil {
		return err
	}
	h.numSlots = int(numSlots)
	if err = binary.Read(buf, binary.LittleEndian, &numUsedSlots); err != nil {
		return err
	}
	h.numUsedSlots = int(numUsedSlots)

	h.tuples = make([]*Tuple, h.numSlots)
	for i := 0; i < h.numUsedSlots; i++ {
		tuple := &Tuple{}
		if tuple, err = readTupleFrom(buf, h.tupleDesc); err != nil {
			return err
		}
		h.tuples[i] = tuple
	}

	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (h *heapPage) tupleIter() func() (*Tuple, error) {
	currentSlot := 0
	return func() (*Tuple, error) {
		if currentSlot >= h.numSlots {
			return nil, nil
		}

		for ; currentSlot < h.numSlots; currentSlot++ {
			if h.tuples[currentSlot] == nil {
				continue
			}
			tuple := h.tuples[currentSlot]
			tuple.Rid = heapRecordID{PageNo: h.pageNo, SlotNo: currentSlot}
			currentSlot++
			return tuple, nil
		}

		return nil, nil
	}
}

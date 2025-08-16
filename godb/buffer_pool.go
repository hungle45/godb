package godb

//BufferPool provides methods to cache pool that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// Permissions used to when reading / locking pool
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	length int
	cap    int
	pool   map[any]Page
}

// NewBufferPool Create a new BufferPool with the specified number of pool
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		cap:  numPages,
		pool: make(map[any]Page),
	}, nil
}

// FlushAllPages Testing method -- iterate through all pool in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pool as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.pool {
		_ = page.getFile().flushPage(page)
		page.setDirty(0, false)
	}
}

// AbortTransaction Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pool tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// CommitTransaction Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pool tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pool and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// BeginTransaction Begin a new transaction. You do not need to implement this for lab 1.
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// GetPage Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk using [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pool), a page should be evicted.  Should not evict
// pool that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pool, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pool in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (page Page, err error) {
	if page, exists := bp.pool[file.pageKey(pageNo)]; exists {
		return page, nil
	}

	if bp.isFull() {
		if err = bp.evictPage(); err != nil {
			return nil, err
		}
	}

	if page, err = file.readPage(pageNo); err != nil {
		return nil, err
	}

	bp.pool[file.pageKey(pageNo)] = page
	bp.length++

	return page, nil
}

func (bp *BufferPool) isFull() bool {
	return bp.length == bp.cap
}

func (bp *BufferPool) evictPage() error {
	for key, page := range bp.pool {
		if !page.isDirty() {
			delete(bp.pool, key)
			bp.length--
			return nil
		}
	}
	return Error{BufferPoolFullError, fmt.Sprintf("GetPage: page size %d is full, cannot evict dirty pages", bp.cap)}
}

package godb

//BufferPool provides methods to cache pool that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
	"sync"
)

// Permissions used to when reading / locking pool
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type LockType int

const (
	SharedLockType LockType = iota
	ExclusiveLockType
)

type BufferPool struct {
	length           int
	cap              int
	pool             map[any]Page
	runningTIDs      sync.Map
	poolLock         sync.Mutex
	transLockManager *transLockManager
}

// NewBufferPool Create a new BufferPool with the specified number of pool
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		cap:              numPages,
		pool:             make(map[any]Page),
		runningTIDs:      sync.Map{},
		transLockManager: newTransLockManager(),
		poolLock:         sync.Mutex{},
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
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	for _, pageKey := range bp.transLockManager.GetLockedPages(tid) {
		if page, exists := bp.pool[pageKey]; exists && page.isDirty() {
			bp.evictPage(pageKey)
		}
	}

	bp.transLockManager.UnlockAllPages(tid)
	bp.runningTIDs.Delete(tid)
}

// CommitTransaction Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pool tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pool and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	for _, pageKey := range bp.transLockManager.GetLockedPages(tid) {
		if page, exists := bp.pool[pageKey]; exists && page.isDirty() {
			_ = page.getFile().flushPage(page)
		}
	}

	bp.transLockManager.UnlockAllPages(tid)
	bp.runningTIDs.Delete(tid)
}

// BeginTransaction Begin a new transaction. You do not need to implement this for lab 1.
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	if _, exists := bp.runningTIDs.LoadOrStore(tid, struct{}{}); exists {
		return Error{IllegalTransactionError, fmt.Sprintf("BeginTransaction: transaction %d is already running", tid)}
	}
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
	for {
		bp.poolLock.Lock()
		if err = bp.transLockManager.LockPage(tid, file.pageKey(pageNo), perm); err == nil {
			bp.poolLock.Unlock()
			break
		}
		bp.poolLock.Unlock()
	}

	print("GetPage: transaction ", tid, " acquired ", perm, " lock on page ", pageNo, "\n")
	if page, exists := bp.pool[file.pageKey(pageNo)]; exists {
		return page, nil
	}

	if bp.isFull() {
		if err = bp.freePage(); err != nil {
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

func (bp *BufferPool) freePage() error {
	for key, page := range bp.pool {
		if !page.isDirty() {
			bp.evictPage(key)
			return nil
		}
	}
	return Error{BufferPoolFullError, fmt.Sprintf("GetPage: page size %d is full, cannot evict dirty pages", bp.cap)}
}

func (bp *BufferPool) evictPage(pageKey any) {
	page, exists := bp.pool[pageKey]
	if !exists {
		return
	}
	if !page.isDirty() {
		return
	}
	delete(bp.pool, pageKey)
	bp.length--
}

type transLockManager struct {
	// map of transaction id to map of page key to lock type: tranID -> (pageKey -> lockType)
	tranPageLocks map[TransactionID]map[any]LockType
	// map of page key to lock: pageKey -> lock
	pageLocks map[any]*sync.RWMutex
}

func newTransLockManager() *transLockManager {
	return &transLockManager{
		tranPageLocks: make(map[TransactionID]map[any]LockType),
		pageLocks:     make(map[any]*sync.RWMutex),
	}
}

func (m *transLockManager) LockPage(tid TransactionID, pageKey any, perm RWPerm) error {
	if _, exists := m.tranPageLocks[tid]; !exists {
		m.tranPageLocks[tid] = make(map[any]LockType)
	}

	if lockType, locked := m.tranPageLocks[tid][pageKey]; locked {
		// already have the lock
		if lockType == ExclusiveLockType || (lockType == SharedLockType && perm == ReadPerm) {
			return nil
		}
		// upgrade from shared to exclusive (lockType is SharedLockType and perm is WritePerm)
		lock, exists := m.pageLocks[pageKey]
		if !exists {
			return fmt.Errorf("LockPage: inconsistent state, page %d has no lock", pageKey)
		}
		lock.RUnlock()
		lock.Lock()
		m.tranPageLocks[tid][pageKey] = ExclusiveLockType
		return nil
	}

	lock, exists := m.pageLocks[pageKey]
	if !exists {
		lock = &sync.RWMutex{}
		m.pageLocks[pageKey] = lock
	}

	switch perm {
	case ReadPerm:
		lock.RLock()
		m.tranPageLocks[tid][pageKey] = SharedLockType
	case WritePerm:
		lock.Lock()
		m.tranPageLocks[tid][pageKey] = ExclusiveLockType
	default:
		return fmt.Errorf("LockPage: unknown permission %d", perm)
	}

	return nil
}

func (m *transLockManager) UnlockAllPages(tid TransactionID) {
	pages, exists := m.tranPageLocks[tid]
	if !exists {
		return
	}

	for pageKey, lockType := range pages {
		lock, exists := m.pageLocks[pageKey]
		if !exists {
			continue
		}

		switch lockType {
		case ExclusiveLockType:
			lock.Unlock()
		case SharedLockType:
			lock.RUnlock()
		}
	}
	delete(m.tranPageLocks, tid)
}

func (m *transLockManager) GetLockedPages(tid TransactionID) []any {
	pages, exists := m.tranPageLocks[tid]
	if !exists {
		return []any{}
	}

	pageKeys := make([]any, 0, len(pages))
	for pageKey := range pages {
		pageKeys = append(pageKeys, pageKey)
	}
	return pageKeys
}

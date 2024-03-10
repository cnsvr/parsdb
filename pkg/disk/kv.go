package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"parsdb/pkg"
	c "parsdb/pkg/constants"
	"syscall"
)

type KV struct {
	Path string

	// internals
	fp   *os.File
	tree pkg.BTree
	mmap struct {
		file   int      // file size, can be larger than database size
		total  int      // mmap size, can be larger than file size
		chunks [][]byte // multiple mmap chunks, can be non-contiguous
	}

	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

func NewKV(path string) *KV {
	return &KV{Path: path}
}

func (db *KV) pageGet(ptr uint64) pkg.BNode {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/c.BTREE_PAGE_SIZE
		if ptr < end {
			offset := c.BTREE_PAGE_SIZE * (ptr - start)
			return pkg.NewBNode(chunk[offset : offset+c.BTREE_PAGE_SIZE])
		}
		start = end
	}
	panic("pageGet: invalid pointer")
}

func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write
		db.page.flushed = 1 // reserved for the master page
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])

	// verify the page
	if !bytes.Equal([]byte(c.DB_SIG), data[:16]) {
		return errors.New("masterLoad: bad signature")
	}

	bad := !(1 <= used && used <= uint64(db.mmap.file/c.BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("masterLoad: bad master page")
	}

	db.tree.SetRoot(root)
	db.page.flushed = used
	return nil
}

// update the master page. it must be atomic.
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(c.DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.GetRoot())
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// Note: updating the page via mmap is not atomic
	// use the `pwrite` syscall to update the master page atomically

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %v", err)
	}

	return nil
}

// callback for BTree, allocate a new page
func (db *KV) pageNew(node pkg.BNode) uint64 {
	// TODO: reuse deallocated pages
	assert(node.Size() <= c.BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.GetData())
	return ptr
}

// callback for BTree, deallocate a page
func (db *KV) pageDel(ptr uint64) {
	// TODO: implement page deallocation
}

// extend the file to at least `npages` pages
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / c.BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}

	for filePages < npages {
		// the file size increased exponentially
		// so that we don't need to extend the file for every update
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}

		filePages += inc
	}

	fileSize := filePages * c.BTREE_PAGE_SIZE
	err := syscall.Ftruncate(int(db.fp.Fd()), int64(fileSize)) // TODO: use fallocate() instead, but it's not supported on macOS
	if err != nil {
		return fmt.Errorf("fallocate: %v", err)
	}

	db.mmap.file = fileSize
	return nil
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open: %v", err)
	}

	db.fp = fp

	// create initial mmap
	sz, chunk, err := mmapInit(fp)
	if err != nil {
		goto fail
	}

	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	/// btree callbacks
	db.tree.AssignGet(db.pageGet)
	db.tree.AssignNew(db.pageNew)
	db.tree.AssignDel(db.pageDel)

	err = masterLoad(db)
	if err != nil {
		goto fail
	}

	// done
	return nil

fail:
	db.Close()
	return fmt.Errorf("open: %v", err)

}

func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		assert(err == nil)
	}

	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) []byte {
	return db.tree.Get(key)
}

// update the db
func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flashPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flashPages(db)
}

func flashPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func syncPages(db *KV) error {
	// flush data to the disk. must be done before updating the master page
	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("sync: %v", err)
	}

	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]

	// update & flush the master page
	if err := masterStore(db); err != nil {
		return err
	}

	if err := db.fp.Sync(); err != nil {
		return fmt.Errorf("sync: %v", err)
	}

	fmt.Println("syncPages: flushed", db.page.flushed)
	return nil
}

func writePages(db *KV) error {
	// extend the file % mmap if necessary
	npages := int(db.page.flushed) + len(db.page.temp)
	if err := extendFile(db, npages); err != nil {
		return err
	}

	if err := extendMmap(db, npages); err != nil {
		return err
	}

	// copy data to the file
	for i, page := range db.page.temp {
		ptr := db.page.flushed + uint64(i)
		copy(db.pageGet(ptr).GetData(), page)
	}

	fmt.Println("writePages: flushed", db.page.flushed)
	return nil
}

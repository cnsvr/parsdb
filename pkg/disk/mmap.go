package disk

import (
	"fmt"
	"os"
	c "parsdb/pkg/constants"
	"syscall"
)

func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat file error: %v", err)
	}

	if fi.Size()%c.BTREE_MAX_KEY_SIZE != 0 {
		return 0, nil, fmt.Errorf("file size is not a multiple of %d", c.BTREE_MAX_KEY_SIZE)
	}

	mmapSize := 64 << 20 // 64MB
	assert(mmapSize%c.BTREE_PAGE_SIZE == 0)
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	// mmapSize can be lagrer than file size

	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap error: %v", err)
	}

	return int(fi.Size()), chunk, nil
}

// extend the mmap by adding new mmapings
func extendMmap(db *KV, npages int) error {
	if db.mmap.total >= npages*c.BTREE_PAGE_SIZE {
		return nil
	}

	// double the address space
	chunk, err := syscall.Mmap(int(db.fp.Fd()),
		int64(db.mmap.total),
		npages*c.BTREE_PAGE_SIZE,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)

	if err != nil {
		return fmt.Errorf("mmap error: %v", err)
	}

	db.mmap.total += db.mmap.total
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func assert(b bool) {
	if !b {
		panic("assertion failed")
	}
}

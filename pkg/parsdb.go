package pkg

import (
	c "parsdb/pkg/constants"
	"unsafe"
)

type ParsDB struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func NewParsDB() *ParsDB {
	pages := map[uint64]BNode{}
	return &ParsDB{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok)
				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() <= c.BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data == nil)
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (db *ParsDB) Add(key string, val string) {
	db.tree.Insert([]byte(key), []byte(val))
	db.ref[key] = val
}

func (db *ParsDB) Get(key string) string {
	return db.ref[key]
}

func (db *ParsDB) Delete(key string) bool {
	delete(db.ref, key)
	return db.tree.Delete([]byte(key))
}

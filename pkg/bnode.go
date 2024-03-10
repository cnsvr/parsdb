package pkg

import (
	"encoding/binary"
	c "parsdb/pkg/constants"
)

/*
	| type | nkeys | pointers | offsets  | key-values
	| 2B   | 2B    | nkeys*8B | nkeys*2B | ...

	| klen | vlen | key | val |
	| 2B   | 2B   |...  |...  |
*/

type BNode struct {
	data []byte // can be dumped to disk
}

func NewBNode(data []byte) BNode {
	return BNode{data}
}

func init() {
	node1max := c.HEADER + 8 + 2 + 4 + c.BTREE_MAX_KEY_SIZE + c.BTREE_MAX_VAL_SIZE
	if node1max > c.BTREE_PAGE_SIZE {
		panic("node1max > BTREE_PAGE_SIZE")
	}
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := c.HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := c.HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

func (node BNode) setKey(idx uint16, key []byte) {
	assert(len(key) <= c.BTREE_MAX_KEY_SIZE)
	pos := node.kvPos(idx)
	binary.LittleEndian.PutUint16(node.data[pos:], uint16(len(key)))
	copy(node.data[pos+4:], key)
}

func (node BNode) setVal(idx uint16, val []byte) {
	assert(len(val) <= c.BTREE_MAX_VAL_SIZE)
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	binary.LittleEndian.PutUint16(node.data[pos+2:], uint16(len(val)))
	copy(node.data[pos+4+klen:], val)

}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return c.HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

// The offset list is used to locate the nth KV pair quickly.
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return c.HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func (node BNode) Size() int {
	return len(node.data)
}

func (node BNode) GetData() []byte {
	return node.data
}

func assert(ok bool) {
	if !ok {
		panic("assertion failed")
	}
}

package pkg

type BTree struct {
	// pointer (a nonzero page number)
	root uint64 // the root page

	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

func (tree *BTree) SetRoot(root uint64) {
	tree.root = root
}

func (tree *BTree) GetRoot() uint64 {
	return tree.root
}

func (tree *BTree) AssignGet(get func(uint64) BNode) {
    tree.get = get
}

func (tree *BTree) AssignNew(new func(BNode) uint64) {
    tree.new = new
}

func (tree *BTree) AssignDel(del func(uint64)) {
    tree.del = del
}
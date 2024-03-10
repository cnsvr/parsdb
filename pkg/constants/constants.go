package constants

const (
	BNODE_NODE = 1 // internal nodes without data
	BNODE_LEAF = 2 // leaf nodes with data

	HEADER = 4 // Page header size -> 4 bytes =3 2B type + 2B nkeys

	BTREE_PAGE_SIZE    = 4096 // Page size -> 4KB
	BTREE_MAX_KEY_SIZE = 1000 // Max key size -> 1000 bytes
	BTREE_MAX_VAL_SIZE = 3000 // Max value size -> 3000 bytes
	DB_SIG             = "PARSDB"
)

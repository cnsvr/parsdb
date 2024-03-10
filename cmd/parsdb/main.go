package main

import (
	. "parsdb/pkg/disk"
)

func main() {
	println("Welcome to ParsDB")

	/*
		db := NewParsDB()
		// get pages from the tree
		// insert some data
		db.Add("key1", "val1")
		db.Add("key2", "val2")

		// get the value
		key1 := db.Get("key1")
		// print the value
		println(key1)

		key2 := db.Get("key2")
		println(key2)

		deletedKey1 := db.Delete("key1")
		println(deletedKey1)
		// delete the value
		deletedKey1Again := db.Delete("key1")
		println(deletedKey1Again)

	*/

	kv := NewKV("test.db")
	println("Opening the database...")
	err := kv.Open()
	if err != nil {
		panic(err)
	}

	defer kv.Close()

	err = kv.Set([]byte("key1"), []byte("naber kanka"))
	if err != nil {
		panic(err)
	}

	val := kv.Get([]byte("key1"))
	println(string(val))

	del, err := kv.Del([]byte("key1"))
	if err != nil {
		panic(err)
	}
	println(del)

	val = kv.Get([]byte("key1"))
	if val == nil {
		println("Key1 is not found.")
	} else {
		println(string(val))
	}
}

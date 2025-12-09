package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/MartinSimango/simple-db/pkg/db"
)

func main() {

	n, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	// TODO: add support for args

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			// TODO: allow client to use longer lasting connections AS database is one connection per operation for
			client, err := db.NewSimpleDbClient(":5050")
			if err != nil {
				panic(err)
			}
			defer client.Close()
			if r, err := client.Put(fmt.Sprintf("key%d", id), fmt.Sprintf("value%d", id)); err != nil {
				fmt.Printf("Error putting key%d: %v\n", id, err)
			} else {
				fmt.Println(r)
			}
		}(i)
	}
	wg.Wait()
}

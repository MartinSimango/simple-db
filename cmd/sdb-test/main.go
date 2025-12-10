package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MartinSimango/simple-db/pkg/db"
)

func main() {

	cn, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	rpc, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(cn)
	s := time.Now()
	for i := 0; i < cn; i++ { // cn tcp connections
		go func(id int) {
			defer wg.Done()
			var iwg sync.WaitGroup
			iwg.Add(rpc) // rpc - requests per connection
			client, err := db.NewSimpleDbClient(":5050")
			if err != nil {
				fmt.Println("ERROR: failed to connect to server:", err)
				return
			}
			defer client.Close()
			for j := 0; j < rpc; j++ {
				go func() {
					defer iwg.Done()
					send(client, fmt.Sprintf("key%d-%d", id, j), fmt.Sprintf("value%d-%d", id, j))

				}()
			}
			iwg.Wait()
		}(i)
	}
	wg.Wait()
	fmt.Println("All done in", time.Since(s).Seconds(), "seconds")
}

func send(client *db.SimpleDbClient, key, value string) {

	if r, err := client.Put(key, value); err != nil {
		fmt.Printf("Error putting key %v: %v\n", key, err)
		fmt.Println("Reconnecting...")
		client.Reconnect(":5050")

	} else {
		// fmt.Println(r)
		if strings.HasPrefix(r, "ERROR") {
			fmt.Println("Reconnecting...")
			client.Reconnect(":5050")
		}
	}
}

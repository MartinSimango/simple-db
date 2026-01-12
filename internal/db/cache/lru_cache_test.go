package cache_test

import (
	"fmt"
	"testing"

	"github.com/MartinSimango/simple-db/internal/db/cache"
)

// TODO: add benchmarks and Fuzz tests
func TestLRUCache_Get(t *testing.T) {

	c := cache.NewLRUCache(2)
	c.Put("a", []byte("Hello"))
	c.Put("c", []byte("Hello"))
	value, found := c.Get("a")

	c.Put("b", []byte("Hello"))

	fmt.Println(string(value), found)
	tests := []struct {
		name string
	}{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// c := cache.NewLRUCache(100)
			// c.Get("asd")

		})
	}

}

func TestLRUCache_Put(t *testing.T) {

}

func TestLRUCache_Delete(t *testing.T) {

}

func TestLRUCache_Capacity(t *testing.T) {

}

func TestLRUCache_Size(t *testing.T) {

}

func TestLRUCache_SizeInBytes(t *testing.T) {

}

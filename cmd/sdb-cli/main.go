package main

import (
	"github.com/MartinSimango/simple-db/internal/cmd"
	del "github.com/MartinSimango/simple-db/internal/cmd/delete"
	"github.com/MartinSimango/simple-db/internal/cmd/get"
	"github.com/MartinSimango/simple-db/internal/cmd/put"
)

func main() {
	simpleDbCmd := cmd.NewSimpleDbCmd()
	putCmd := put.NewPutCmd()
	getCmd := get.NewGetCmd()
	deleteCmd := del.NewDeleteCmd()

	simpleDbCmd.AddCommand(putCmd, getCmd, deleteCmd)
	simpleDbCmd.Execute()
}

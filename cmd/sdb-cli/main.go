package main

import (
	"github.com/MartinSimango/simple-db/pkg/cmd"
	del "github.com/MartinSimango/simple-db/pkg/cmd/delete"
	"github.com/MartinSimango/simple-db/pkg/cmd/get"
	"github.com/MartinSimango/simple-db/pkg/cmd/put"
)

func main() {
	simpleDbCmd := cmd.NewSimpleDbCmd()
	putCmd := put.NewPutCmd()
	getCmd := get.NewGetCmd()
	deleteCmd := del.NewDeleteCmd()

	simpleDbCmd.AddCommand(putCmd, getCmd, deleteCmd)
	simpleDbCmd.Execute()
}

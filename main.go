package main

import (
	"fmt"
	"github.com/chaoyangnz/breezedb"
)

func main() {
	fileManager := breezedb.NewFileManager("./dbdata")
	fileManager.Append("a.bdb")
	fmt.Printf("file length %d", fileManager.Length("a.bdb"))
}

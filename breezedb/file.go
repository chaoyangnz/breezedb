package breezedb

import (
	"fmt"
	"os"
)

type Block struct {
	fileName string
	number int64
}

type Page struct {
	data []byte
}

func (this *Block) FileName() string {
	return this.fileName
}

func (this *Block) Number() int64 {
	return this.number
}

func (this *Block) Offset(blockSize int64) int64 {
	return this.number * blockSize
}

func (this *Page) ReadInt() int {
	return int(this.data[4])
}

func (this *Page) WriteInt() int {
	return int(this.data[4])
}

type FileManager struct {
	dbDir string
	blockSize int64
	openFiles map[string]*os.File
}

func NewFileManager(dbDir string) *FileManager {
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		os.Mkdir(dbDir, 0777)
	}
	return &FileManager{
		dbDir:     dbDir,
		blockSize: 4 * 1024,
		openFiles: make(map[string]*os.File),
	}
}

func (this *FileManager) Read(block *Block, page *Page) {
	f := this.getFile(block.fileName)
	f.ReadAt(page.data, block.Offset(this.blockSize))
}

func (this *FileManager) Write(block *Block, page *Page) {
	f := this.getFile(block.fileName)
	f.WriteAt(page.data, block.Offset(this.blockSize))
}

func (this *FileManager) Append(fileName string) *Block {
	f := this.getFile(fileName)
	length := this.Length(fileName)
	count := length / this.blockSize
	block := &Block{fileName: fileName, number: count}
	f.Seek(block.Offset(this.blockSize), 0)
	f.Write(make([]byte, this.blockSize))
	return block
}

func (this *FileManager) IsNew() bool {
	return true
}

func (this *FileManager) Length(fileName string) int64 {
	f := this.getFile(fileName)
	stat, err := f.Stat()
	if err != nil {
		fmt.Printf("cannot stat file %v", fileName)
		os.Exit(-1)
	}
	return stat.Size()
}

func (this *FileManager) BlockSize() int64 {
	return this.blockSize
}

func (this *FileManager) Close() {
	for _, f := range this.openFiles {
		f.Close()
	}
	this.openFiles = make(map[string]*os.File)
}

func (this *FileManager) getFile(fileName string) *os.File {
	file := this.openFiles[fileName]
	if file == nil {
		f, err := os.OpenFile(this.dbDir + "/" + fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("cannot open file %v (%v)", fileName, err)
			os.Exit(-1)
		}
		this.openFiles[fileName] = f
		file = f
	}
	return file
}

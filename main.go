package main

import (
	_ "github.com/eventials/go-tus"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"qianxunke-fastdfs/modules/file"
)


func main() {
	//https://github.com/perfree/go-fastdfs-web
	file.Init()
}

package model

import (
	"github.com/jinzhu/gorm"
	"qianxunke-fastdfs/common/api"
	"qianxunke-fastdfs/modules/file/config"
	"sync"
)

var (
	d *dao
	m sync.Mutex
)

type dao struct {
}

type FileDao interface {


	InsertFileInfo(fileInfo *FileInfo) (err error)

	UpdateFileInfo(fileInfo *FileInfo) (err error)

	DeleteFileInfoById(id uint) (err error)

	SearchFileInfoList(limit int64, pages int64, key string, startTime string, endTime string, order string) (data *api.ListResponseEntity, err error)

}

func GetDB() *gorm.DB {
	return config.MasterEngine()
}

func Init() {
		m.Lock()
		defer m.Unlock()
		if d != nil {
			return
		}
		d = &dao{}
		if !GetDB().HasTable(&FileInfo{}){
			GetDB().CreateTable(&FileInfo{})
		}
}

func GetDao() (FileDao, error) {
	if d == nil {
		Init()
	}
	return d, nil
}

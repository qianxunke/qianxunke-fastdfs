package model

import (
	"net/http"
)

type FileInfo struct {
	ID        uint `gorm:"primary_key"`
	CreatedAt int64
	UpdatedAt int64
	Name      string   `json:"name"`
	ReName    string   `json:"rename"`
	Path      string   `json:"path"`
	Md5       string   `json:"md5"`
	Size      int64    `json:"size"`
	PeerStr   string
	Scene     string `json:"scene"`
	TimeStamp int64  `json:"timeStamp"`
	OffSet    int64  `json:"offset"`
	Status    int64  `json:"status"` //同步到第三方状态
	Retry     int    `gorm:"-"`      // 忽略这个字段
	Op        string `gorm:"-"`      // 忽略这个字段
}

type FileLog struct {
	FileInfo *FileInfo
	FileName string
}

type WrapReqResp struct {
	W    *http.ResponseWriter
	R    *http.Request
	Done chan bool
}

type JsonResult struct {
	Message string      `json:"message"`
	Status  string      `json:"status"`
	Data    interface{} `json:"data"`
}

type FileResult struct {
	Url     string `json:"url"`
	Md5     string `json:"md5"`
	Path    string `json:"path"`
	Domain  string `json:"domain"`
	Scene   string `json:"scene"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime"`
	Scenes  string `json:"scenes"`
	Retmsg  string `json:"retmsg"`
	Retcode int    `json:"retcode"`
	Src     string `json:"src"`
}

type StatDateFileInfo struct {
	Date      string `json:"date"`
	TotalSize int64  `json:"totalSize"`
	FileCount int64  `json:"fileCount"`
}

type FileInfoResult struct {
	Name    string `json:"name"`
	Md5     string `json:"md5"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mtime"`
	IsDir   bool   `json:"is_dir"`
}

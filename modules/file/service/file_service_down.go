package service

import (
	"errors"
	"github.com/astaxie/beego/httplib"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb/util"
	"io"
	"net/http"
	"os"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"strconv"
	"strings"
	"time"
)

func (this *server) DownloadSmallFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err        error
		data       []byte
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
		notFound   bool
	)
	r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = config.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			log.Error(err)
		}
	}
	data, notFound, err = this.GetSmallFileByURI(w, r)
	_ = notFound
	if data != nil && string(data[0]) == "1" {
		if isDownload {
			this.SetDownloadHeader(w, r)
		}
		if imgWidth != 0 || imgHeight != 0 {
			this.ResizeImageByBytes(w, data[1:], uint(imgWidth), uint(imgHeight))
			return true, nil
		}
		w.Write(data[1:])
		return true, nil
	}
	return false, errors.New("not found")
}

func (this *server) DownloadNormalFileByURI(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err        error
		isDownload bool
		imgWidth   int
		imgHeight  int
		width      string
		height     string
	)
	_ = r.ParseForm()
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = config.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	width = r.FormValue("width")
	height = r.FormValue("height")
	if width != "" {
		imgWidth, err = strconv.Atoi(width)
		if err != nil {
			_ = log.Error(err)
		}
	}
	if height != "" {
		imgHeight, err = strconv.Atoi(height)
		if err != nil {
			_ = log.Error(err)
		}
	}
	if isDownload {
		this.SetDownloadHeader(w, r)
	}
	fullpath, _ := this.GetFilePathFromRequest(w, r)
	if imgWidth != 0 || imgHeight != 0 {
		this.ResizeImage(w, fullpath, uint(imgWidth), uint(imgHeight))
		return true, nil
	}
	staticHandler.ServeHTTP(w, r)
	return true, nil
}


func (this *server) DownloadNotFound(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		fullpath   string
		smallPath  string
		isDownload bool
		pathMd5    string
		peer       string
		fileInfo   *model.FileInfo
	)
	fullpath, smallPath = this.GetFilePathFromRequest(w, r)
	isDownload = true
	if r.FormValue("download") == "" {
		isDownload = config.Config().DefaultDownload
	}
	if r.FormValue("download") == "0" {
		isDownload = false
	}
	if smallPath != "" {
		pathMd5 = this.util.MD5(smallPath)
	} else {
		pathMd5 = this.util.MD5(fullpath)
	}
	for _, peer = range config.Config().Peers {
		if fileInfo, err = this.checkPeerFileExist(peer, pathMd5, fullpath); err != nil {
			_ = log.Error(err)
			continue
		}
		if fileInfo.Md5 != "" {
			go this.DownloadFromPeer(peer, fileInfo)
			//http.Redirect(w, r, peer+r.RequestURI, 302)
			if isDownload {
				this.SetDownloadHeader(w, r)
			}
			this.DownloadFileToResponse(peer+r.RequestURI, w, r)
			return
		}
	}
	w.WriteHeader(404)
	return
}

func (this *server) DownloadFileToResponse(url string, w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		req  *httplib.BeegoHTTPRequest
		resp *http.Response
	)
	req = httplib.Get(url)
	req.SetTimeout(time.Second*20, time.Second*600)
	resp, err = req.DoRequest()
	if err != nil {
		_ = log.Error(err)
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		_ = log.Error(err)
	}
}

func (this *server) ConsumerDownLoad() {
	ConsumerFunc := func() {
		for {
			fileInfo := <-this.queueFromPeers
			if len(fileInfo.PeerStr) <= 0 {
				_ = log.Warn("Peer is null", fileInfo)
				continue
			}
			_peers:=strings.Split(fileInfo.PeerStr,",")
			for _, peer := range _peers {
				if strings.Contains(peer, "127.0.0.1") {
					_ = log.Warn("sync error with 127.0.0.1", fileInfo)
					continue
				}
				if peer != this.host {
					this.DownloadFromPeer(peer, &fileInfo)
					break
				}
			}
		}
	}
	for i := 0; i < config.Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}

func (this *server) RemoveDownloading() {
	RemoveDownloadFunc := func() {
		for {
			iter := this.ldb.NewIterator(util.BytesPrefix([]byte("downloading_")), nil)
			for iter.Next() {
				key := iter.Key()
				keys := strings.Split(string(key), "_")
				if len(keys) == 3 {
					if t, err := strconv.ParseInt(keys[1], 10, 64); err == nil && time.Now().Unix()-t > 60*10 {
						_ = os.Remove(config.DOCKER_DIR + keys[2])
					}
				}
			}
			iter.Release()
			time.Sleep(time.Minute * 3)
		}
	}
	go RemoveDownloadFunc()
}
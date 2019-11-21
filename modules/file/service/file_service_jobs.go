package service

import (
	"fmt"
	"github.com/radovskyb/watcher"
	log "github.com/sjqzhang/seelog"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"regexp"
	"runtime"
	"strings"
	"time"
)

func (this *server) WatchfileschangeJob() {
	var (
		w        *watcher.Watcher
		fileInfo model.FileInfo
		curDir   string
		err      error
		qchan    chan *model.FileInfo
		isLink   bool
	)
	qchan = make(chan *model.FileInfo, 10000)
	w = watcher.New()
	w.FilterOps(watcher.Create)
	//w.FilterOps(watcher.Create, watcher.Remove)
	curDir, err = filepath.Abs(filepath.Dir(config.STORE_DIR_NAME))
	if err != nil {
		_ = log.Error(err)
	}
	go func() {
		for {
			select {
			case event := <-w.Event:
				if event.IsDir() {
					continue
				}

				fpath := strings.Replace(event.Path, curDir+string(os.PathSeparator), "", 1)
				if isLink {
					fpath = strings.Replace(event.Path, curDir, config.STORE_DIR_NAME, 1)
				}
				fpath = strings.Replace(fpath, string(os.PathSeparator), "/", -1)
				sum := this.util.MD5(fpath)
				fileInfo = model.FileInfo{
					Size:      event.Size(),
					Name:      event.Name(),
					Path:      strings.TrimSuffix(fpath, "/"+event.Name()), // files/default/20190927/xxx
					Md5:       sum,
					TimeStamp: event.ModTime().Unix(),
					PeerStr:   this.host,
					OffSet:    -2,
					Op:        event.Op.String(),
				}
				log.Info(fmt.Sprintf("WatchFilesChange op:%s path:%s", event.Op.String(), fpath))
				qchan <- &fileInfo
				//this.PeerAppendToQueue(&fileInfo)
			case err := <-w.Error:
				_ = log.Error(err)
			case <-w.Closed:
				return
			}
		}
	}()
	go func() {
		for {
			c := <-qchan
			if time.Now().Unix()-c.TimeStamp < 3 {
				qchan <- c
				time.Sleep(time.Second * 1)
				continue
			} else {
				if c.Op == watcher.Create.String() {
					log.Info(fmt.Sprintf("Syncfile Add to Queue path:%s", fileInfo.Path+"/"+fileInfo.Name))
					this.PeerAppendToQueue(c)
					_, _ = this.SaveFileInfoToLevelDB(c.Md5, c, this.ldb)
				}
			}
		}
	}()
	if dir, err := os.Readlink(config.STORE_DIR_NAME); err == nil {

		if strings.HasSuffix(dir, string(os.PathSeparator)) {
			dir = strings.TrimSuffix(dir, string(os.PathSeparator))
		}
		curDir = dir
		isLink = true
		if err := w.AddRecursive(dir); err != nil {
			_ = log.Error(err)
		}
		_ = w.Ignore(dir + "/_tmp/")
		_ = w.Ignore(dir + "/" + config.LARGE_DIR_NAME + "/")
	}
	if err := w.AddRecursive("./" + config.STORE_DIR_NAME); err != nil {
		_ = log.Error(err)
	}
	_ = w.Ignore("./" + config.STORE_DIR_NAME + "/_tmp/")
	_ = w.Ignore("./" + config.STORE_DIR_NAME + "/" + config.LARGE_DIR_NAME + "/")
	if err := w.Start(time.Millisecond * 100); err != nil {
		_ = log.Error(err)
	}
}


func (this *server) Reload(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		data    []byte
		cfg     config.GloablConfig
		action  string
		cfgjson string
		result  model.JsonResult
	)
	result.Status = "fail"
	_ = r.ParseForm()
	if !this.IsPeer(r) {
		_, _ = w.Write([]byte(this.GetClusterNotPermitMessage(r)))
		return
	}
	cfgjson = r.FormValue("cfg")
	action = r.FormValue("action")
	_ = cfgjson
	if action == "get" {
		result.Data = config.Config()
		result.Status = "ok"
		_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
		return
	}
	if action == "set" {
		if cfgjson == "" {
			result.Message = "(error)parameter cfg(json) require"
			_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
			return
		}
		if err = json.Unmarshal([]byte(cfgjson), &cfg); err != nil {
			_ = log.Error(err)
			result.Message = err.Error()
			_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
			return
		}
		result.Status = "ok"
		cfgjson = this.util.JsonEncodePretty(cfg)
		this.util.WriteFile(config.CONST_CONF_FILE_NAME, cfgjson)
		_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
		return
	}
	if action == "reload" {
		if data, err = ioutil.ReadFile(config.CONST_CONF_FILE_NAME); err != nil {
			result.Message = err.Error()
			_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
			return
		}
		if err = json.Unmarshal(data, &cfg); err != nil {
			result.Message = err.Error()
			_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
			return
		}
		config.ParseConfig(config.CONST_CONF_FILE_NAME)
		this.initComponent(true)
		result.Status = "ok"
		_, _ = w.Write([]byte(this.util.JsonEncodePretty(result)))
		return
	}
	if action == "" {
		_, _ = w.Write([]byte("(error)action support set(json) get reload"))
	}
}



func (this *server) initComponent(isReload bool) {
	var (
		ip string
	)
	if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
		ip = this.util.GetPulicIP()
	}
	if config.Config().Host == "" {
		if len(strings.Split(config.Config().Addr, ":")) == 2 {
			this.host = fmt.Sprintf("http://%s:%s", ip, strings.Split(config.Config().Addr, ":")[1])
			config.Config().Host = this.host
		}
	} else {
		if strings.HasPrefix(config.Config().Host, "http") {
			this.host = config.Config().Host
		} else {
			this.host = "http://" + config.Config().Host
		}
	}
	ex, _ := regexp.Compile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	var peers []string
	for _, peer := range config.Config().Peers {
		if this.util.Contains(ip, ex.FindAllString(peer, -1)) ||
			this.util.Contains("127.0.0.1", ex.FindAllString(peer, -1)) {
			continue
		}
		if strings.HasPrefix(peer, "http") {
			peers = append(peers, peer)
		} else {
			peers = append(peers, "http://"+peer)
		}
	}
	config.Config().Peers = peers
	if !isReload {
		this.FormatStatInfo()
		if config.Config().EnableTus {
			this.initTus()
		}
	}
	for _, s := range config.Config().Scenes {
		kv := strings.Split(s, ":")
		if len(kv) == 2 {
			this.sceneMap.Put(kv[0], kv[1])
		}
	}
	if config.Config().ReadTimeout == 0 {
		config.Config().ReadTimeout = 60 * 10
	}
	if config.Config().WriteTimeout == 0 {
		config.Config().WriteTimeout = 60 * 10
	}
	if config.Config().SyncWorker == 0 {
		config.Config().SyncWorker = 200
	}
	if config.Config().UploadWorker == 0 {
		config.Config().UploadWorker = runtime.NumCPU() + 4
		if runtime.NumCPU() < 4 {
			config.Config().UploadWorker = 8
		}
	}
	if config.Config().UploadQueueSize == 0 {
		config.Config().UploadQueueSize = 200
	}
	if config.Config().RetryCount == 0 {
		config.Config().RetryCount = 3
	}
}
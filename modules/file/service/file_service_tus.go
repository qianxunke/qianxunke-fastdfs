package service

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	log "github.com/sjqzhang/seelog"
	"github.com/sjqzhang/tusd"
	"github.com/sjqzhang/tusd/filestore"
	"io"
	slog "log"
	"net/http"
	"os"
	"path"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"strconv"
	"strings"
	"time"
)

type hookDataStore struct {
	tusd.DataStore
}

func (s *server) initTus() {
	var (
		err     error
		fileLog *os.File
		bigDir  string
	)
	BIG_DIR := config.STORE_DIR + "/_big/" + config.Config().PeerId
	_ = os.MkdirAll(BIG_DIR, 0775)
	_ = os.MkdirAll(config.LOG_DIR, 0775)
	store := filestore.FileStore{
		Path: BIG_DIR,
	}
	if fileLog, err = os.OpenFile(config.LOG_DIR+"/tusd.log", os.O_CREATE|os.O_RDWR, 0666); err != nil {
		log.Error(err)
		panic("initTus")
	}
	go func() {
		for {
			if fi, err := fileLog.Stat(); err != nil {
				log.Error(err)
			} else {
				if fi.Size() > 1024*1024*500 {
					//500M
					_, _ = s.util.CopyFile(config.LOG_DIR+"/tusd.log", config.LOG_DIR+"/tusd.log.2")
					_, _ = fileLog.Seek(0, 0)
					_ = fileLog.Truncate(0)
					_, _ = fileLog.Seek(0, 2)
				}
			}
			time.Sleep(time.Second * 30)
		}
	}()
	l := slog.New(fileLog, "[tusd] ", slog.LstdFlags)
	bigDir = config.CONST_BIG_UPLOAD_PATH_SUFFIX
	if config.Config().SupportGroupManage {
		bigDir = fmt.Sprintf("/%s%s", config.Config().Group, config.CONST_BIG_UPLOAD_PATH_SUFFIX)
	}
	composer := tusd.NewStoreComposer()
	// support raw tus upload and download
	store.GetReaderExt = func(id string) (io.Reader, error) {
		var (
			offset int64
			err    error
			length int
			buffer []byte
			fi     *model.FileInfo
			fn     string
		)
		if fi, err = s.GetFileInfoFromLevelDB(id); err != nil {
			_ = log.Error(err)
			return nil, err
		} else {
			fn = fi.Name
			if fi.ReName != "" {
				fn = fi.ReName
			}
			fp := config.DOCKER_DIR + fi.Path + "/" + fn
			if s.util.FileExists(fp) {
				log.Info(fmt.Sprintf("download:%s", fp))
				return os.Open(fp)
			}
			ps := strings.Split(fp, ",")
			if len(ps) > 2 && s.util.FileExists(ps[0]) {
				if length, err = strconv.Atoi(ps[2]); err != nil {
					return nil, err
				}
				if offset, err = strconv.ParseInt(ps[1], 10, 64); err != nil {
					return nil, err
				}
				if buffer, err = s.util.ReadFileByOffSet(ps[0], offset, length); err != nil {
					return nil, err
				}
				if buffer[0] == '1' {
					bufferReader := bytes.NewBuffer(buffer[1:])
					return bufferReader, nil
				} else {
					msg := "data no sync"
					_ = log.Error(msg)
					return nil, errors.New(msg)
				}
			}
			return nil, errors.New(fmt.Sprintf("%s not found", fp))
		}
	}
	store.UseIn(composer)
	SetupPreHooks := func(composer *tusd.StoreComposer) {
		composer.UseCore(hookDataStore{
			DataStore: composer.Core,
		})
	}
	SetupPreHooks(composer)
	handler, err := tusd.NewHandler(tusd.Config{
		Logger:                  l,
		BasePath:                bigDir,
		StoreComposer:           composer,
		NotifyCompleteUploads:   true,
		RespectForwardedHeaders: true,
	})
	notify := func(handler *tusd.Handler) {
		for {
			select {
			case info := <-handler.CompleteUploads:
				log.Info("CompleteUploads", info)
				name := ""
				if v, ok := info.MetaData["filename"]; ok {
					name = v
				}
				var err error
				md5sum := ""
				oldFullPath := BIG_DIR + "/" + info.ID + ".bin"
				infoFullPath := BIG_DIR + "/" + info.ID + ".info"
				if md5sum, err = s.util.GetFileSumByName(oldFullPath, config.Config().FileSumArithmetic); err != nil {
					_ = log.Error(err)
					continue
				}
				ext := path.Ext(name)
				filename := md5sum + ext
				timeStamp := time.Now().Unix()
				fpath := time.Now().Format("/20060102/15/04/")
				newFullPath := config.STORE_DIR + "/" + config.Config().DefaultScene + fpath + config.Config().PeerId + "/" + filename
				if fi, err := s.GetFileInfoFromLevelDB(md5sum); err != nil {
					_ = log.Error(err)
				} else {
					if fi.Md5 != "" {
						if _, err := s.SaveFileInfoToLevelDB(info.ID, fi, s.ldb); err != nil {
							_ = log.Error(err)
						}
						log.Info(fmt.Sprintf("file is found md5:%s", fi.Md5))
						log.Info("remove file:", oldFullPath)
						log.Info("remove file:", infoFullPath)
						_ = os.Remove(oldFullPath)
						_ = os.Remove(infoFullPath)
						continue
					}
				}
				fpath = config.STORE_DIR_NAME + "/" + config.Config().DefaultScene + fpath + config.Config().PeerId
				_ = os.MkdirAll(config.DOCKER_DIR+fpath, 0775)
				fileInfo := &model.FileInfo{
					Name:      name,
					Path:      fpath,
					ReName:    filename,
					Size:      info.Size,
					TimeStamp: timeStamp,
					Md5:       md5sum,
					PeerStr:   s.host,
					OffSet:    -1,
				}
				if err = os.Rename(oldFullPath, newFullPath); err != nil {
					_ = log.Error(err)
					continue
				}
				log.Info(fileInfo)
				_ = os.Remove(infoFullPath)
				if _, err = s.SaveFileInfoToLevelDB(info.ID, fileInfo, s.ldb); err != nil {
					//assosiate file id
					_ = log.Error(err)
				}
				s.SaveFileMd5Log(fileInfo, config.CONST_FILE_Md5_FILE_NAME)
				go s.postFileToPeer(fileInfo)
				callBack := func(info tusd.FileInfo, fileInfo *model.FileInfo) {
					if callback_url, ok := info.MetaData["callback_url"]; ok {
						req := httplib.Post(callback_url)
						req.SetTimeout(time.Second*10, time.Second*10)
						req.Param("info", s.util.JsonEncodePretty(fileInfo))
						req.Param("id", info.ID)
						if _, err := req.String(); err != nil {
							_ = log.Error(err)
						}
					}
				}
				go callBack(info, fileInfo)
			}
		}
	}
	go notify(handler)
	if err != nil {
		_ = log.Error(err)
	}
	http.Handle(bigDir, http.StripPrefix(bigDir, handler))
}

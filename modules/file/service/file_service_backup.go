package service

import (
	"fmt"
	"github.com/astaxie/beego/httplib"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb/util"
	"net/http"
	"os"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
	"time"
)

func (s *server) CleanAndBackUp() {
	Clean := func() {
		var (
			filenames []string
			yesterday string
		)
		if s.curDate != s.util.GetToDay() {
			filenames = []string{config.CONST_Md5_QUEUE_FILE_NAME, config.CONST_Md5_ERROR_FILE_NAME, config.CONST_REMOME_Md5_FILE_NAME}
			yesterday = s.util.GetDayFromTimeStamp(time.Now().AddDate(0, 0, -1).Unix())
			for _, filename := range filenames {
				s.CleanLogLevelDBByDate(yesterday, filename)
			}
			s.BackUpMetaDataByDate(yesterday)
			s.curDate = s.util.GetToDay()
		}
	}
	go func() {
		for {
			time.Sleep(time.Hour * 6)
			Clean()
		}
	}()
}


func (s *server) BackUp(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		date   string
		result model.JsonResult
		inner  string
		url    string
	)
	result.Status = "ok"
	_ = r.ParseForm()
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if date == "" {
		date = s.util.GetToDay()
	}
	if s.IsPeer(r) {
		if inner != "1" {
			for _, peer := range config.Config().Peers {
				backUp := func(peer string, date string) {
					url = fmt.Sprintf("%s%s", peer, s.getRequestURI("backup"))
					req := httplib.Post(url)
					req.Param("date", date)
					req.Param("inner", "1")
					req.SetTimeout(time.Second*5, time.Second*600)
					if _, err = req.String(); err != nil {
						_ = log.Error(err)
					}
				}
				go backUp(peer, date)
			}
		}
		go s.BackUpMetaDataByDate(date)
		result.Message = "back job start..."
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	} else {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	}
}

func (s *server) BackUpMetaDataByDate(date string) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("BackUpMetaDataByDate")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	var (
		err          error
		keyPrefix    string
		msg          string
		name         string
		fileInfo     model.FileInfo
		logFileName  string
		fileLog      *os.File
		fileMeta     *os.File
		metaFileName string
		fi           os.FileInfo
	)
	logFileName = config.DATA_DIR + "/" + date + "/" + config.CONST_FILE_Md5_FILE_NAME
	s.lockMap.LockKey(logFileName)
	defer s.lockMap.UnLockKey(logFileName)
	metaFileName = config.DATA_DIR + "/" + date + "/" + "meta.data"
	_ = os.MkdirAll(config.DATA_DIR+"/"+date, 0775)
	if s.util.IsExist(logFileName) {
		_ = os.Remove(logFileName)
	}
	if s.util.IsExist(metaFileName) {
		_ = os.Remove(metaFileName)
	}
	fileLog, err = os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		_ = log.Error(err)
		return
	}
	defer fileLog.Close()
	fileMeta, err = os.OpenFile(metaFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		_ = log.Error(err)
		return
	}
	defer fileMeta.Close()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, config.CONST_FILE_Md5_FILE_NAME)
	iter := s.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		name = fileInfo.Name
		if fileInfo.ReName != "" {
			name = fileInfo.ReName
		}
		msg = fmt.Sprintf("%s\t%s\n", fileInfo.Md5, string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			_ = log.Error(err)
		}
		msg = fmt.Sprintf("%s\t%s\n", s.util.MD5(fileInfo.Path+"/"+name), string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			_ = log.Error(err)
		}
		msg = fmt.Sprintf("%s|%d|%d|%s\n", fileInfo.Md5, fileInfo.Size, fileInfo.TimeStamp, fileInfo.Path+"/"+name)
		if _, err = fileLog.WriteString(msg); err != nil {
			_ = log.Error(err)
		}
	}
	if fi, err = fileLog.Stat(); err != nil {
		_ = log.Error(err)
	} else if fi.Size() == 0 {
		_ = fileLog.Close()
		_ = os.Remove(logFileName)
	}
	if fi, err = fileMeta.Stat(); err != nil {
		_ = log.Error(err)
	} else if fi.Size() == 0 {
		_ = fileMeta.Close()
		_ = os.Remove(metaFileName)
	}
}

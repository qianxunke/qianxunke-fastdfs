package service

import (
	"errors"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
)

func (s *server) RemoveKeyFromLevelDB(key string, db *leveldb.DB) error {
	var (
		err error
	)
	err = db.Delete([]byte(key), nil)
	return err
}

func (s *server) SaveFileInfoToLevelDB(key string, fileInfo *model.FileInfo, db *leveldb.DB) (*model.FileInfo, error) {
	var (
		err  error
		data []byte
	)
	if fileInfo == nil || db == nil {
		err=errors.New("fileInfo is null or db is null")
		return nil, err
	}
	dao,err:=model.GetDao()
	if dao!=nil{
		err = dao.UpdateFileInfo(fileInfo)
	}
	if db == s.ldb { //search slow ,write fast, double write logDB
		logDate := s.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
		logKey := fmt.Sprintf("%s_%s_%s", logDate, config.CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		_ = s.logDB.Put([]byte(logKey), data, nil)
	}
	return fileInfo, nil
}


func (s *server) IsExistFromLevelDB(key string, db *leveldb.DB) (bool, error) {
	return db.Has([]byte(key), nil)
}

func (s *server) GetFileInfoFromLevelDB(key string) (*model.FileInfo, error) {
	var (
		err      error
		data     []byte
		fileInfo model.FileInfo
	)
	if data, err = s.ldb.Get([]byte(key), nil); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, &fileInfo); err != nil {
		return nil, err
	}
	return &fileInfo, nil
}

func (s *server) CleanLogLevelDBByDate(date string, filename string) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("CleanLogLevelDBByDate")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		keys      mapset.Set
	)
	keys = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := s.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys.Add(string(iter.Value()))
	}
	iter.Release()
	for key := range keys.Iter() {
		err = s.RemoveKeyFromLevelDB(key.(string), s.logDB)
		if err != nil {
			_ = log.Error(err)
		}
	}
}


package service

import (
	"bufio"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func (s *server) Download(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		ok        bool
		fullpath  string
		smallPath string
		fi        os.FileInfo
	)
	// redirect to upload
	if r.RequestURI == "/" || r.RequestURI == "" ||
		r.RequestURI == "/"+config.Config().Group ||
		r.RequestURI == "/"+config.Config().Group+"/" {
		s.Index(w, r)
		return
	}
	if ok, err = s.CheckDownloadAuth(w, r); !ok {
		_ = log.Error(err)
		s.NotPermit(w, r)
		return
	}

	if config.Config().EnableCrossOrigin {
		s.CrossOrigin(w, r)
	}
	fullpath, smallPath = s.GetFilePathFromRequest(w, r)
	if smallPath == "" {
		if fi, err = os.Stat(fullpath); err != nil {
			s.DownloadNotFound(w, r)
			return
		}
		if !config.Config().ShowDir && fi.IsDir() {
			_, _ = w.Write([]byte("list dir deny"))
			return
		}
		//staticHandler.ServeHTTP(w, r)
		_, _ = s.DownloadNormalFileByURI(w, r)
		return
	}
	if smallPath != "" {
		if ok, err = s.DownloadSmallFileByURI(w, r); !ok {
			s.DownloadNotFound(w, r)
			return
		}
		return
	}

}

func (s *server) CheckFileExist(w http.ResponseWriter, r *http.Request) {
	var (
		data     []byte
		err      error
		fileInfo *model.FileInfo
		fpath    string
		fi       os.FileInfo
	)
	_ = r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	if fileInfo, err = s.GetFileInfoFromLevelDB(md5sum); fileInfo != nil {
		if fileInfo.OffSet != -1 {
			if data, err = json.Marshal(fileInfo); err != nil {
				_ = log.Error(err)
			}
			_, _ = w.Write(data)
			return
		}
		fpath = config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
		if fileInfo.ReName != "" {
			fpath = config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
		}
		if s.util.IsExist(fpath) {
			if data, err = json.Marshal(fileInfo); err == nil {
				_, _ = w.Write(data)
				return
			} else {
				_ = log.Error(err)
			}
		} else {
			if fileInfo.OffSet == -1 {
				_ = s.RemoveKeyFromLevelDB(md5sum, s.ldb) // when file delete,delete from leveldb
			}
		}
	} else {
		if fpath != "" {
			fi, err = os.Stat(fpath)
			if err == nil {
				sum := s.util.MD5(fpath)
				fileInfo = &model.FileInfo{
					Path:      path.Dir(fpath),
					Name:      path.Base(fpath),
					Size:      fi.Size(),
					Md5:       sum,
					PeerStr:    config.Config().Host,
					OffSet:    -1, //very important
					TimeStamp: fi.ModTime().Unix(),
				}
				data, err = json.Marshal(fileInfo)
				_, _ = w.Write(data)
				return
			}
		}
	}
	data, _ = json.Marshal(model.FileInfo{})
	_, _ = w.Write(data)
	return
}

func (s *server) CheckFilesExist(w http.ResponseWriter, r *http.Request) {
	var (
		data      []byte
		err       error
		fileInfo  *model.FileInfo
		fileInfos []*model.FileInfo
		fpath     string
		result    model.JsonResult
	)
	_ = r.ParseForm()
	md5sum := ""
	md5sum = r.FormValue("md5s")
	md5s := strings.Split(md5sum, ",")
	for _, m := range md5s {
		if fileInfo, err = s.GetFileInfoFromLevelDB(m); fileInfo != nil {
			if fileInfo.OffSet != -1 {
				if data, err = json.Marshal(fileInfo); err != nil {
					_ = log.Error(err)
				}
				//w.Write(data)
				//return
				fileInfos = append(fileInfos, fileInfo)
				continue
			}
			fpath = config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name
			if fileInfo.ReName != "" {
				fpath = config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName
			}
			if s.util.IsExist(fpath) {
				if data, err = json.Marshal(fileInfo); err == nil {
					fileInfos = append(fileInfos, fileInfo)
					//w.Write(data)
					//return
					continue
				} else {
					_ = log.Error(err)
				}
			} else {
				if fileInfo.OffSet == -1 {
					_ = s.RemoveKeyFromLevelDB(md5sum, s.ldb) // when file delete,delete from leveldb
				}
			}
		}
	}
	result.Data = fileInfos
	data, _ = json.Marshal(result)
	_, _ = w.Write(data)
	return
}

func (s *server) SaveStat() {
	SaveStatFunc := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				_ = log.Error("SaveStatFunc")
				_ = log.Error(re)
				_ = log.Error(string(buffer))
			}
		}()
		stat := s.statMap.Get()
		if v, ok := stat[config.CONST_STAT_FILE_COUNT_KEY]; ok {
			switch v.(type) {
			case int64, int32, int, float64, float32:
				if v.(int64) >= 0 {
					if data, err := json.Marshal(stat); err != nil {
						log.Error(err)
					} else {
						s.util.WriteBinFile(config.CONST_STAT_FILE_NAME, data)
					}
				}
			}
		}
	}
	SaveStatFunc()
}

//Mds
func (s *server) GetMd5sForWeb(w http.ResponseWriter, r *http.Request) {
	var (
		date   string
		err    error
		result mapset.Set
		lines  []string
		md5s   []interface{}
	)
	if !s.IsPeer(r) {
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
		return
	}
	date = r.FormValue("date")
	if result, err = s.GetMd5sByDate(date, config.CONST_FILE_Md5_FILE_NAME); err != nil {
		_ = log.Error(err)
		return
	}
	md5s = result.ToSlice()
	for _, line := range md5s {
		if line != nil && line != "" {
			lines = append(lines, line.(string))
		}
	}
	_, _ = w.Write([]byte(strings.Join(lines, ",")))
}

func (s *server) GetMd5File(w http.ResponseWriter, r *http.Request) {
	var (
		date  string
		fpath string
		data  []byte
		err   error
	)
	if !s.IsPeer(r) {
		return
	}
	fpath = config.DATA_DIR + "/" + date + "/" + config.CONST_FILE_Md5_FILE_NAME
	if !s.util.FileExists(fpath) {
		w.WriteHeader(404)
		return
	}
	if data, err = ioutil.ReadFile(fpath); err != nil {
		w.WriteHeader(500)
		return
	}
	_, _ = w.Write(data)
}

func (s *server) ReceiveMd5s(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5str   string
		fileInfo *model.FileInfo
		md5s     []string
	)
	if !s.IsPeer(r) {
		_ = log.Warn(fmt.Sprintf("ReceiveMd5s %s", s.util.GetClientIp(r)))
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
		return
	}
	_ = r.ParseForm()
	md5str = r.FormValue("md5s")
	md5s = strings.Split(md5str, ",")
	AppendFunc := func(md5s []string) {
		for _, m := range md5s {
			if m != "" {
				if fileInfo, err = s.GetFileInfoFromLevelDB(m); err != nil {
					_ = log.Error(err)
					continue
				}
				s.PeerAppendToQueue(fileInfo)
			}
		}
	}
	go AppendFunc(md5s)
}

func (s *server) GetFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		fpath    string
		md5sum   string
		fileInfo *model.FileInfo
		err      error
		result   model.JsonResult
	)
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	result.Status = "fail"
	if !s.IsPeer(r) {
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
		return
	}
	md5sum = r.FormValue("md5")
	if fpath != "" {
		fpath = strings.Replace(fpath, "/"+config.Config().Group+"/", config.STORE_DIR_NAME+"/", 1)
		md5sum = s.util.MD5(fpath)
	}
	if fileInfo, err = s.GetFileInfoFromLevelDB(md5sum); err != nil {
		_ = log.Error(err)
		result.Message = err.Error()
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	result.Status = "ok"
	result.Data = fileInfo
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	return
}

func (s *server) RemoveFile(w http.ResponseWriter, r *http.Request) {
	var (
		err      error
		md5sum   string
		fileInfo *model.FileInfo
		fpath    string
		delUrl   string
		result   model.JsonResult
		inner    string
		name     string
	)
	_ = delUrl
	_ = inner
	_ = r.ParseForm()
	md5sum = r.FormValue("md5")
	fpath = r.FormValue("path")
	inner = r.FormValue("inner")
	result.Status = "fail"
	if !s.IsPeer(r) {
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
		return
	}
	if config.Config().AuthUrl != "" && !s.CheckAuth(w, r) {
		s.NotPermit(w, r)
		return
	}
	if fpath != "" && md5sum == "" {
		fpath = strings.Replace(fpath, "/"+config.Config().Group+"/", config.STORE_DIR_NAME+"/", 1)
		md5sum = s.util.MD5(fpath)
	}
	if inner != "1" {
		for _, peer := range config.Config().Peers {
			delFile := func(peer string, md5sum string, fileInfo *model.FileInfo) {
				delUrl = fmt.Sprintf("%s%s", peer, s.getRequestURI("delete"))
				req := httplib.Post(delUrl)
				req.Param("md5", md5sum)
				req.Param("inner", "1")
				req.SetTimeout(time.Second*5, time.Second*10)
				if _, err = req.String(); err != nil {
					_ = log.Error(err)
				}
			}
			go delFile(peer, md5sum, fileInfo)
		}
	}
	if len(md5sum) < 32 {
		result.Message = "md5 unvalid"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	if fileInfo, err = s.GetFileInfoFromLevelDB(md5sum); err != nil {
		result.Message = err.Error()
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	if fileInfo.OffSet >= 0 {
		result.Message = "small file delete not support"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	name = fileInfo.Name
	if fileInfo.ReName != "" {
		name = fileInfo.ReName
	}
	fpath = fileInfo.Path + "/" + name
	if fileInfo.Path != "" && s.util.FileExists(config.DOCKER_DIR+fpath) {
		s.SaveFileMd5Log(fileInfo, config.CONST_REMOME_Md5_FILE_NAME)
		if err = os.Remove(config.DOCKER_DIR + fpath); err != nil {
			result.Message = err.Error()
			_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
			return
		} else {
			result.Message = "remove success"
			result.Status = "ok"
			_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
			return
		}
	}
	result.Message = "fail remove"
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
}

func (s *server) Upload(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		fn     string
		folder string
		fpTmp  *os.File
		fpBody *os.File
	)
	if r.Method == http.MethodGet {
		s.upload(w, r)
		return
	}
	folder = config.STORE_DIR + "/_tmp/" + time.Now().Format("20060102")
	_ = os.MkdirAll(folder, 0777)
	fn = folder + "/" + s.util.GetUUID()
	defer func() {
		_ = os.Remove(fn)
	}()
	fpTmp, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		_ = log.Error(err)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer fpTmp.Close()
	if _, err = io.Copy(fpTmp, r.Body); err != nil {
		_ = log.Error(err)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	fpBody, err = os.Open(fn)
	r.Body = fpBody
	done := make(chan bool, 1)
	s.queueUpload <- model.WrapReqResp{&w, r, done}
	<-done

}

func (s *server) SysnToAws(fileInfo *model.FileInfo) (err error) {
	dao, err := model.GetDao()
	if dao != nil {
		_ = dao.InsertFileInfo(fileInfo)
	}
	return err
}

func (s *server) BenchMark(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	batch := new(leveldb.Batch)
	for i := 0; i < 100000000; i++ {
		f := model.FileInfo{}
		f.PeerStr="http://192.168.0.1,http://192.168.2.5"
		f.Path = "20190201/19/02"
		s := strconv.Itoa(i)
		s = s.util.MD5(s)
		f.Name = s
		f.Md5 = s
		if data, err := json.Marshal(&f); err == nil {
			batch.Put([]byte(s), data)
		}
		if i%10000 == 0 {
			if batch.Len() > 0 {
				_ = s.ldb.Write(batch, nil)
				//				batch = new(leveldb.Batch)
				batch.Reset()
			}
			fmt.Println(i, time.Since(t).Seconds())
		}
		//fmt.Println(server.GetFileInfoFromLevelDB(s))
	}
	s.util.WriteFile("time.txt", time.Since(t).String())
	fmt.Println(time.Since(t).String())
}

func (s *server) RepairStatWeb(w http.ResponseWriter, r *http.Request) {
	var (
		result model.JsonResult
		date   string
		inner  string
	)
	if !s.IsPeer(r) {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if ok, err := regexp.MatchString("\\d{8}", date); err != nil || !ok {
		result.Message = "invalid date"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	if date == "" || len(date) != 8 {
		date = s.util.GetToDay()
	}
	if inner != "1" {
		for _, peer := range config.Config().Peers {
			req := httplib.Post(peer + s.getRequestURI("repair_stat"))
			req.Param("inner", "1")
			req.Param("date", date)
			if _, err := req.String(); err != nil {
				_ = log.Error(err)
			}
		}
	}
	result.Data = s.RepairStatByDate(date)
	result.Status = "ok"
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
}

func (s *server) Stat(w http.ResponseWriter, r *http.Request) {
	var (
		result   model.JsonResult
		inner    string
		echart   string
		category []string
		barCount []int64
		barSize  []int64
		dataMap  map[string]interface{}
	)
	if !s.IsPeer(r) {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	_ = r.ParseForm()
	inner = r.FormValue("inner")
	echart = r.FormValue("echart")
	data := s.GetStat()
	result.Status = "ok"
	result.Data = data
	if echart == "1" {
		dataMap = make(map[string]interface{}, 3)
		for _, v := range data {
			barCount = append(barCount, v.FileCount)
			barSize = append(barSize, v.TotalSize)
			category = append(category, v.Date)
		}
		dataMap["category"] = category
		dataMap["barCount"] = barCount
		dataMap["barSize"] = barSize
		result.Data = dataMap
	}
	if inner == "1" {
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(data)))
	} else {
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	}
}

func (s *server) GetStat() []model.StatDateFileInfo {
	var (
		min   int64
		max   int64
		err   error
		i     int64
		rows  []model.StatDateFileInfo
		total model.StatDateFileInfo
	)
	min = 20190101
	max = 20190101
	for k := range s.statMap.Get() {
		ks := strings.Split(k, "_")
		if len(ks) == 2 {
			if i, err = strconv.ParseInt(ks[0], 10, 64); err != nil {
				continue
			}
			if i >= max {
				max = i
			}
			if i < min {
				min = i
			}
		}
	}
	for i := min; i <= max; i++ {
		s := fmt.Sprintf("%d", i)
		if v, ok := s.statMap.GetValue(s + "_" + config.CONST_STAT_FILE_TOTAL_SIZE_KEY); ok {
			var info model.StatDateFileInfo
			info.Date = s
			switch v.(type) {
			case int64:
				info.TotalSize = v.(int64)
				total.TotalSize = total.TotalSize + v.(int64)
			}
			if v, ok := s.statMap.GetValue(s + "_" + config.CONST_STAT_FILE_COUNT_KEY); ok {
				switch v.(type) {
				case int64:
					info.FileCount = v.(int64)
					total.FileCount = total.FileCount + v.(int64)
				}
			}
			rows = append(rows, info)
		}
	}
	total.Date = "all"
	rows = append(rows, total)
	return rows
}

func (s *server) RegisterExit() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range c {
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				_ = s.ldb.Close()
				log.Info("Exit", s)
				os.Exit(1)
			}
		}
	}()
}

func (s *server) LoadSearchDict() {
	go func() {
		log.Info("Load search dict ....")
		f, err := os.Open(config.CONST_SEARCH_FILE_NAME)
		if err != nil {
			_ = log.Error(err)
			return
		}
		defer f.Close()
		r := bufio.NewReader(f)
		for {
			line, isprefix, err := r.ReadLine()
			for isprefix && err == nil {
				kvs := strings.Split(string(line), "\t")
				if len(kvs) == 2 {
					s.searchMap.Put(kvs[0], kvs[1])
				}
			}
		}
		log.Info("finish load search dict")
	}()
}

func (s *server) SaveSearchDict() {
	var (
		err        error
		fp         *os.File
		searchDict map[string]interface{}
		k          string
		v          interface{}
	)
	s.lockMap.LockKey(config.CONST_SEARCH_FILE_NAME)
	defer s.lockMap.UnLockKey(config.CONST_SEARCH_FILE_NAME)
	searchDict = s.searchMap.Get()
	fp, err = os.OpenFile(config.CONST_SEARCH_FILE_NAME, os.O_RDWR, 0755)
	if err != nil {
		_ = log.Error(err)
		return
	}
	defer fp.Close()
	for k, v = range searchDict {
		_, _ = fp.WriteString(fmt.Sprintf("%s\t%s", k, v.(string)))
	}
}

func (s *server) AutoRepair(forceRepair bool) {
	if s.lockMap.IsLock("AutoRepair") {
		_ = log.Warn("Lock AutoRepair")
		return
	}
	s.lockMap.LockKey("AutoRepair")
	defer s.lockMap.UnLockKey("AutoRepair")
	AutoRepairFunc := func(forceRepair bool) {
		var (
			dateStats []model.StatDateFileInfo
			err       error
			countKey  string
			md5s      string
			localSet  mapset.Set
			remoteSet mapset.Set
			allSet    mapset.Set
			tmpSet    mapset.Set
			fileInfo  *model.FileInfo
		)
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				_ = log.Error("AutoRepair")
				_ = log.Error(re)
				_ = log.Error(string(buffer))
			}
		}()
		Update := func(peer string, dateStat model.StatDateFileInfo) {
			//从远端拉数据过来
			req := httplib.Get(fmt.Sprintf("%s%s?date=%s&force=%s", peer, s.getRequestURI("sync"), dateStat.Date, "1"))
			req.SetTimeout(time.Second*5, time.Second*5)
			if _, err = req.String(); err != nil {
				_ = log.Error(err)
			}
			log.Info(fmt.Sprintf("syn file from %s date %s", peer, dateStat.Date))
		}
		for _, peer := range config.Config().Peers {
			req := httplib.Post(fmt.Sprintf("%s%s", peer, s.getRequestURI("stat")))
			req.Param("inner", "1")
			req.SetTimeout(time.Second*5, time.Second*15)
			if err = req.ToJSON(&dateStats); err != nil {
				_ = log.Error(err)
				continue
			}
			for _, dateStat := range dateStats {
				if dateStat.Date == "all" {
					continue
				}
				countKey = dateStat.Date + "_" + config.CONST_STAT_FILE_COUNT_KEY
				if v, ok := s.statMap.GetValue(countKey); ok {
					switch v.(type) {
					case int64:
						if v.(int64) != dateStat.FileCount || forceRepair {
							//不相等,找差异
							//TODO
							req := httplib.Post(fmt.Sprintf("%s%s", peer, s.getRequestURI("get_md5s_by_date")))
							req.SetTimeout(time.Second*15, time.Second*60)
							req.Param("date", dateStat.Date)
							if md5s, err = req.String(); err != nil {
								continue
							}
							if localSet, err = s.GetMd5sByDate(dateStat.Date, config.CONST_FILE_Md5_FILE_NAME); err != nil {
								_ = log.Error(err)
								continue
							}
							remoteSet = s.util.StrToMapSet(md5s, ",")
							allSet = localSet.Union(remoteSet)
							md5s = s.util.MapSetToStr(allSet.Difference(localSet), ",")
							req = httplib.Post(fmt.Sprintf("%s%s", peer, s.getRequestURI("receive_md5s")))
							req.SetTimeout(time.Second*15, time.Second*60)
							req.Param("md5s", md5s)
							_, _ = req.String()
							tmpSet = allSet.Difference(remoteSet)
							for v := range tmpSet.Iter() {
								if v != nil {
									if fileInfo, err = s.GetFileInfoFromLevelDB(v.(string)); err != nil {
										_ = log.Error(err)
										continue
									}
									s.PeerAppendToQueue(fileInfo)
								}
							}
							//Update(peer,dateStat)
						}
					}
				} else {
					Update(peer, dateStat)
				}
			}
		}
	}
	AutoRepairFunc(forceRepair)
}

func (s *server) LoadFileInfoByDate(date string, filename string) (mapset.Set, error) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("LoadFileInfoByDate")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		fileInfos mapset.Set
	)
	fileInfos = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := s.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		var fileInfo model.FileInfo
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileInfos.Add(&fileInfo)
	}
	iter.Release()
	return fileInfos, nil
}

func (s *server) RepairFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		result model.JsonResult
	)
	if !s.IsPeer(r) {
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
		return
	}
	if !config.Config().EnableMigrate {
		_, _ = w.Write([]byte("please set enable_migrate=true"))
		return
	}
	result.Status = "ok"
	result.Message = "repair job start,don't try again,very danger "
	go s.RepairFileInfoFromFile()
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
}

// Notice: performance is poor,just for low capacity,but low memory , if you want to high performance,use searchMap for search,but memory ....
func (s *server) Search(w http.ResponseWriter, r *http.Request) {
	var (
		result    model.JsonResult
		err       error
		kw        string
		count     int
		fileInfos []model.FileInfo
		md5s      []string
	)
	kw = r.FormValue("kw")
	if !s.IsPeer(r) {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	iter := s.ldb.NewIterator(nil, nil)
	for iter.Next() {
		var fileInfo model.FileInfo
		value := iter.Value()
		if err = json.Unmarshal(value, &fileInfo); err != nil {
			_ = log.Error(err)
			continue
		}
		if strings.Contains(fileInfo.Name, kw) && !s.util.Contains(fileInfo.Md5, md5s) {
			count = count + 1
			fileInfos = append(fileInfos, fileInfo)
			md5s = append(md5s, fileInfo.Md5)
		}
		if count >= 100 {
			break
		}
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		_ = log.Error()
	}
	//fileInfos=this.SearchDict(kw) // serch file from map for huge capacity
	result.Status = "ok"
	result.Data = fileInfos
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
}

func (s *server) SearchDict(kw string) []model.FileInfo {
	var (
		fileInfos []model.FileInfo
		fileInfo  *model.FileInfo
	)
	for dict := range s.searchMap.Iter() {
		if strings.Contains(dict.Val.(string), kw) {
			if fileInfo, _ = s.GetFileInfoFromLevelDB(dict.Key); fileInfo != nil {
				fileInfos = append(fileInfos, *fileInfo)
			}
		}
	}
	return fileInfos
}

func (s *server) ListDir(w http.ResponseWriter, r *http.Request) {
	var (
		result      model.JsonResult
		dir         string
		filesInfo   []os.FileInfo
		err         error
		filesResult []model.FileInfoResult
		tmpDir      string
	)
	if !s.IsPeer(r) {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	dir = r.FormValue("dir")
	//if dir == "" {
	//	result.Message = "dir can't null"
	//	w.Write([]byte(this.util.JsonEncodePretty(result)))
	//	return
	//}
	dir = strings.Replace(dir, ".", "", -1)
	if tmpDir, err = os.Readlink(dir); err == nil {
		dir = tmpDir
	}
	filesInfo, err = ioutil.ReadDir(config.DOCKER_DIR + config.STORE_DIR_NAME + "/" + dir)
	if err != nil {
		_ = log.Error(err)
		result.Message = err.Error()
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	for _, f := range filesInfo {
		fi := model.FileInfoResult{
			Name:    f.Name(),
			Size:    f.Size(),
			IsDir:   f.IsDir(),
			ModTime: f.ModTime().Unix(),
			Path:    dir,
			Md5:     s.util.MD5(config.STORE_DIR_NAME + "/" + dir + "/" + f.Name()),
		}
		filesResult = append(filesResult, fi)
	}
	result.Status = "ok"
	result.Data = filesResult
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	return
}

func (s *server) Report(w http.ResponseWriter, r *http.Request) {
	var (
		reportFileName string
		result         model.JsonResult
		html           string
	)
	result.Status = "ok"
	_ = r.ParseForm()
	if s.IsPeer(r) {
		reportFileName = config.STATIC_DIR + "/report.html"
		if s.util.IsExist(reportFileName) {
			if data, err := s.util.ReadBinFile(reportFileName); err != nil {
				_ = log.Error(err)
				result.Message = err.Error()
				_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
				return
			} else {
				html = string(data)
				if config.Config().SupportGroupManage {
					html = strings.Replace(html, "{group}", "/"+config.Config().Group, 1)
				} else {
					html = strings.Replace(html, "{group}", "", 1)
				}
				_, _ = w.Write([]byte(html))
				return
			}
		} else {
			_, _ = w.Write([]byte(fmt.Sprintf("%s is not found", reportFileName)))
		}
	} else {
		_, _ = w.Write([]byte(s.GetClusterNotPermitMessage(r)))
	}
}

func (s *server) Repair(w http.ResponseWriter, r *http.Request) {
	var (
		force       string
		forceRepair bool
		result      model.JsonResult
	)
	result.Status = "ok"
	_ = r.ParseForm()
	force = r.FormValue("force")
	if force == "1" {
		forceRepair = true
	}
	if s.IsPeer(r) {
		go s.AutoRepair(forceRepair)
		result.Message = "repair job start..."
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	} else {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	}

}

func (s *server) Status(w http.ResponseWriter, r *http.Request) {
	var (
		status   model.JsonResult
		sts      map[string]interface{}
		today    string
		sumset   mapset.Set
		ok       bool
		v        interface{}
		err      error
		appDir   string
		diskInfo *disk.UsageStat
		memInfo  *mem.VirtualMemoryStat
	)
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	today = s.util.GetToDay()
	sts = make(map[string]interface{})
	sts["Fs.QueueFromPeers"] = len(s.queueFromPeers)
	sts["Fs.QueueToPeers"] = len(s.queueToPeers)
	sts["Fs.QueueFileLog"] = len(s.queueFileLog)
	for _, k := range []string{config.CONST_FILE_Md5_FILE_NAME, config.CONST_Md5_ERROR_FILE_NAME, config.CONST_Md5_QUEUE_FILE_NAME} {
		k2 := fmt.Sprintf("%s_%s", today, k)
		if v, ok = s.sumMap.GetValue(k2); ok {
			sumset = v.(mapset.Set)
			if k == config.CONST_Md5_QUEUE_FILE_NAME {
				sts["Fs.QueueSetSize"] = sumset.Cardinality()
			}
			if k == config.CONST_Md5_ERROR_FILE_NAME {
				sts["Fs.ErrorSetSize"] = sumset.Cardinality()
			}
			if k == config.CONST_FILE_Md5_FILE_NAME {
				sts["Fs.FileSetSize"] = sumset.Cardinality()
			}
		}
	}
	sts["Fs.AutoRepair"] = config.Config().AutoRepair
	sts["Fs.QueueUpload"] = len(s.queueUpload)
	sts["Fs.RefreshInterval"] = config.Config().RefreshInterval
	sts["Fs.Peers"] = config.Config().Peers
	sts["Fs.Local"] = s.host
	sts["Fs.FileStats"] = s.GetStat()
	sts["Fs.ShowDir"] = config.Config().ShowDir
	sts["Sys.NumGoroutine"] = runtime.NumGoroutine()
	sts["Sys.NumCpu"] = runtime.NumCPU()
	sts["Sys.Alloc"] = memStat.Alloc
	sts["Sys.TotalAlloc"] = memStat.TotalAlloc
	sts["Sys.HeapAlloc"] = memStat.HeapAlloc
	sts["Sys.Frees"] = memStat.Frees
	sts["Sys.HeapObjects"] = memStat.HeapObjects
	sts["Sys.NumGC"] = memStat.NumGC
	sts["Sys.GCCPUFraction"] = memStat.GCCPUFraction
	sts["Sys.GCSys"] = memStat.GCSys
	//sts["Sys.MemInfo"] = memStat
	appDir, err = filepath.Abs(".")
	if err != nil {
		_ = log.Error(err)
	}
	diskInfo, err = disk.Usage(appDir)
	if err != nil {
		_ = log.Error(err)
	}
	sts["Sys.DiskInfo"] = diskInfo
	memInfo, err = mem.VirtualMemory()
	if err != nil {
		_ = log.Error(err)
	}
	sts["Sys.MemInfo"] = memInfo
	status.Status = "ok"
	status.Data = sts
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(status)))
}

func (s *server) HeartBeat(w http.ResponseWriter, r *http.Request) {
}

func (s *server) Index(w http.ResponseWriter, r *http.Request) {
	var (
		uploadUrl    string
		uploadBigUrl string
		uppy         string
	)
	uploadUrl = "/upload"
	uploadBigUrl = config.CONST_BIG_UPLOAD_PATH_SUFFIX
	if config.Config().EnableWebUpload {
		if config.Config().SupportGroupManage {
			uploadUrl = fmt.Sprintf("/%s/upload", config.Config().Group)
			uploadBigUrl = fmt.Sprintf("/%s%s", config.Config().Group, config.CONST_BIG_UPLOAD_PATH_SUFFIX)
		}
		uppy = `<html>
			  
			  <head>
				<meta charset="utf-8" />
				<title>go-fastdfs</title>
				<style>form { bargin } .form-line { display:block;height: 30px;margin:8px; } #stdUpload {background: #fafafa;border-radius: 10px;width: 745px; }</style>
				<link href="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.css" rel="stylesheet"></head>
			  
			  <body>
                <div>标准上传(强列建议使用这种方式)</div>
				<div id="stdUpload">
				  
				  <form action="%s" method="post" enctype="multipart/form-data">
					<span class="form-line">文件(file):
					  <input type="file" id="file" name="file" /></span>
					<span class="form-line">场景(scene):
					  <input type="text" id="scene" name="scene" value="%s" /></span>
					<span class="form-line">输出(output):
					  <input type="text" id="output" name="output" value="json" /></span>
					<span class="form-line">自定义路径(path):
					  <input type="text" id="path" name="path" value="" /></span>
	              <span class="form-line">google认证码(code):
					  <input type="text" id="code" name="code" value="" /></span>
					 <span class="form-line">自定义认证(auth_token):
					  <input type="text" id="auth_token" name="auth_token" value="" /></span>
					<input type="submit" name="submit" value="upload" />
                </form>
				</div>
                 <div>断点续传（如果文件很大时可以考虑）</div>
				<div>
				 
				  <div id="drag-drop-area"></div>
				  <script src="https://transloadit.edgly.net/releases/uppy/v0.30.0/dist/uppy.min.js"></script>
				  <script>var uppy = Uppy.Core().use(Uppy.Dashboard, {
					  inline: true,
					  target: '#drag-drop-area'
					}).use(Uppy.Tus, {
					  endpoint: '%s'
					})
					uppy.on('complete', (result) => {
					 // console.log(result) console.log('Upload complete! We’ve uploaded these files:', result.successful)
					})
					uppy.setMeta({ auth_token: '9ee60e59-cb0f-4578-aaba-29b9fc2919ca',callback_url:'http://127.0.0.1/callback' })//这里是传递上传的认证参数,callback_url参数中 id为文件的ID,info 文转的基本信息json
                </script>
				</div>
			  </body>
			</html>`
		uppyFileName := config.STATIC_DIR + "/uppy.html"
		if s.util.IsExist(uppyFileName) {
			if data, err := s.util.ReadBinFile(uppyFileName); err != nil {
				_ = log.Error(err)
			} else {
				uppy = string(data)
			}
		} else {
			s.util.WriteFile(uppyFileName, uppy)
		}
		_, _ = fmt.Fprintf(w,
			fmt.Sprintf(uppy, uploadUrl, config.Config().DefaultScene, uploadBigUrl))
	} else {
		_, _ = w.Write([]byte("web upload deny"))
	}
}

func (s *server) RemoveEmptyDir(w http.ResponseWriter, r *http.Request) {
	var (
		result model.JsonResult
	)
	result.Status = "ok"
	if s.IsPeer(r) {
		go s.util.RemoveEmptyDir(config.DATA_DIR)
		go s.util.RemoveEmptyDir(config.STORE_DIR)
		result.Message = "clean job start ..,don't try again!!!"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	} else {
		result.Message = s.GetClusterNotPermitMessage(r)
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
	}
}

package service

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	"github.com/nfnt/resize"
	"github.com/sjqzhang/goutil"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb/util"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)





func (this *server) ParseSmallFile(filename string) (string, int64, int, error) {
	var (
		err    error
		offset int64
		length int
	)
	err = errors.New("unvalid small file")
	if len(filename) < 3 {
		return filename, -1, -1, err
	}
	if strings.Contains(filename, "/") {
		filename = filename[strings.LastIndex(filename, "/")+1:]
	}
	pos := strings.Split(filename, ",")
	if len(pos) < 3 {
		return filename, -1, -1, err
	}
	offset, err = strconv.ParseInt(pos[1], 10, 64)
	if err != nil {
		return filename, -1, -1, err
	}
	if length, err = strconv.Atoi(pos[2]); err != nil {
		return filename, offset, -1, err
	}
	if length > config.CONST_SMALL_FILE_SIZE || offset < 0 {
		err = errors.New("invalid filesize or offset")
		return filename, -1, -1, err
	}
	return pos[0], offset, length, nil
}


func (this *server) SetDownloadHeader(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment")
}



func (this *server) RepairFileInfoFromFile() {
	var (
		pathPrefix string
		err        error
		fi         os.FileInfo
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("RepairFileInfoFromFile")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	if this.lockMap.IsLock("RepairFileInfoFromFile") {
		_ = log.Warn("Lock RepairFileInfoFromFile")
		return
	}
	this.lockMap.LockKey("RepairFileInfoFromFile")
	defer this.lockMap.UnLockKey("RepairFileInfoFromFile")
	handlefunc := func(file_path string, f os.FileInfo, err error) error {
		var (
			files    []os.FileInfo
			fi       os.FileInfo
			fileInfo model.FileInfo
			sum      string
			pathMd5  string
		)
		if f.IsDir() {
			files, err = ioutil.ReadDir(file_path)

			if err != nil {
				return err
			}
			for _, fi = range files {
				if fi.IsDir() || fi.Size() == 0 {
					continue
				}
				file_path = strings.Replace(file_path, "\\", "/", -1)
				if config.DOCKER_DIR != "" {
					file_path = strings.Replace(file_path, config.DOCKER_DIR, "", 1)
				}
				if pathPrefix != "" {
					file_path = strings.Replace(file_path, pathPrefix, config.STORE_DIR_NAME, 1)
				}
				if strings.HasPrefix(file_path, config.STORE_DIR_NAME+"/"+config.LARGE_DIR_NAME) {
					log.Info(fmt.Sprintf("ignore small file file %s", file_path+"/"+fi.Name()))
					continue
				}
				pathMd5 = this.util.MD5(file_path + "/" + fi.Name())
				//if finfo, _ := this.GetFileInfoFromLevelDB(pathMd5); finfo != nil && finfo.Md5 != "" {
				//	log.Info(fmt.Sprintf("exist ignore file %s", file_path+"/"+fi.Name()))
				//	continue
				//}
				//sum, err = this.util.GetFileSumByName(file_path+"/"+fi.Name(), Config().FileSumArithmetic)
				sum = pathMd5
				if err != nil {
					_ = log.Error(err)
					continue
				}
				fileInfo = model.FileInfo{
					Size:      fi.Size(),
					Name:      fi.Name(),
					Path:      file_path,
					Md5:       sum,
					TimeStamp: fi.ModTime().Unix(),
					PeerStr:     this.host,
					OffSet:    -2,
				}
				//log.Info(fileInfo)
				log.Info(file_path, "/", fi.Name())
				this.PeerAppendToQueue(&fileInfo)
				//this.postFileToPeer(&fileInfo)
				_, _ = this.SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, this.ldb)
				//this.SaveFileMd5Log(&fileInfo, CONST_FILE_Md5_FILE_NAME)
			}
		}
		return nil
	}
	pathname := config.STORE_DIR
	pathPrefix, err = os.Readlink(pathname)
	if err == nil {
		//link
		pathname = pathPrefix
		if strings.HasSuffix(pathPrefix, "/") {
			//bugfix fullpath
			pathPrefix = pathPrefix[0 : len(pathPrefix)-1]
		}
	}
	fi, err = os.Stat(pathname)
	if err != nil {
		_ = log.Error(err)
	}
	if fi.IsDir() {
		_ = filepath.Walk(pathname, handlefunc)
	}
	log.Info("RepairFileInfoFromFile is finish.")
}


func (this *server) RepairStatByDate(date string) model.StatDateFileInfo {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("RepairStatByDate")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	var (
		err       error
		keyPrefix string
		fileInfo  model.FileInfo
		fileCount int64
		fileSize  int64
		stat      model.StatDateFileInfo
	)
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, config.CONST_FILE_Md5_FILE_NAME)
	iter := this.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		if err = json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileCount = fileCount + 1
		fileSize = fileSize + fileInfo.Size
	}
	this.statMap.Put(date+"_"+config.CONST_STAT_FILE_COUNT_KEY, fileCount)
	this.statMap.Put(date+"_"+config.CONST_STAT_FILE_TOTAL_SIZE_KEY, fileSize)
	this.SaveStat()
	stat.Date = date
	stat.FileCount = fileCount
	stat.TotalSize = fileSize
	return stat
}

func (this *server) GetFilePathByInfo(fileInfo *model.FileInfo, withDocker bool) string {
	var (
		fn string
	)
	fn = fileInfo.Name
	if fileInfo.ReName != "" {
		fn = fileInfo.ReName
	}
	if withDocker {
		return config.DOCKER_DIR + fileInfo.Path + "/" + fn
	}
	return fileInfo.Path + "/" + fn
}

func (this *server) CheckFileExistByInfo(md5s string, fileInfo *model.FileInfo) bool {
	var (
		err      error
		fullpath string
		fi       os.FileInfo
		info     *model.FileInfo
	)
	if fileInfo == nil {
		return false
	}
	if fileInfo.OffSet >= 0 {
		//small file
		if info, err = this.GetFileInfoFromLevelDB(fileInfo.Md5); err == nil && info.Md5 == fileInfo.Md5 {
			return true
		} else {
			return false
		}
	}
	fullpath = this.GetFilePathByInfo(fileInfo, true)
	if fi, err = os.Stat(fullpath); err != nil {
		return false
	}
	if fi.Size() == fileInfo.Size {
		return true
	} else {
		return false
	}
}


func (this *server) CrossOrigin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, X-File-Type, Cache-Control, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	//https://blog.csdn.net/yanzisu_congcong/article/details/80552155
}

func (this *server) CheckAuth(w http.ResponseWriter, r *http.Request) bool {
	var (
		err        error
		req        *httplib.BeegoHTTPRequest
		result     string
		jsonResult model.JsonResult
	)
	if err = r.ParseForm(); err != nil {
		_ = log.Error(err)
		return false
	}
	req = httplib.Post(config.Config().AuthUrl)
	req.SetTimeout(time.Second*10, time.Second*10)
	req.Param("__path__", r.URL.Path)
	req.Param("__query__", r.URL.RawQuery)
	for k, _ := range r.Form {
		req.Param(k, r.FormValue(k))
	}
	for k, v := range r.Header {
		req.Header(k, v[0])
	}
	result, err = req.String()
	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "{") && strings.HasSuffix(result, "}") {
		if err = json.Unmarshal([]byte(result), &jsonResult); err != nil {
			_ = log.Error(err)
			return false
		}
		if jsonResult.Data != "ok" {
			_ = log.Warn(result)
			return false
		}
	} else {
		if result != "ok" {
			_ = log.Warn(result)
			return false
		}
	}
	return true
}

func (this *server) NotPermit(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(401)
}


func (this *server) GetFilePathFromRequest(w http.ResponseWriter, r *http.Request) (string, string) {
	var (
		err       error
		fullpath  string
		smallPath string
		prefix    string
	)
	fullpath = r.RequestURI[1:]
	if strings.HasPrefix(r.RequestURI, "/"+config.Config().Group+"/") {
		fullpath = r.RequestURI[len(config.Config().Group)+2 : len(r.RequestURI)]
	}
	fullpath = strings.Split(fullpath, "?")[0] // just path
	fullpath = config.DOCKER_DIR + config.STORE_DIR_NAME + "/" + fullpath
	prefix = "/" + config.LARGE_DIR_NAME + "/"
	if config.Config().SupportGroupManage {
		prefix = "/" + config.Config().Group + "/" + config.LARGE_DIR_NAME + "/"
	}
	if strings.HasPrefix(r.RequestURI, prefix) {
		smallPath = fullpath //notice order
		fullpath = strings.Split(fullpath, ",")[0]
	}
	if fullpath, err = url.PathUnescape(fullpath); err != nil {
		_ = log.Error(err)
	}
	return fullpath, smallPath
}

func (this *server) CheckDownloadAuth(w http.ResponseWriter, r *http.Request) (bool, error) {
	var (
		err          error
		maxTimestamp int64
		minTimestamp int64
		ts           int64
		token        string
		timestamp    string
		fullpath     string
		smallPath    string
		pathMd5      string
		fileInfo     *model.FileInfo
	)
	CheckToken := func(token string, md5sum string, timestamp string) bool {
		if this.util.MD5(md5sum+timestamp) != token {
			return false
		}
		return true
	}
	if config.Config().EnableDownloadAuth && config.Config().AuthUrl != "" && !this.IsPeer(r) && !this.CheckAuth(w, r) {
		return false, errors.New("auth fail")
	}
	if config.Config().DownloadUseToken && !this.IsPeer(r) {
		token = r.FormValue("token")
		timestamp = r.FormValue("timestamp")
		if token == "" || timestamp == "" {
			return false, errors.New("unvalid request")
		}
		maxTimestamp = time.Now().Add(time.Second *
			time.Duration(config.Config().DownloadTokenExpire)).Unix()
		minTimestamp = time.Now().Add(-time.Second *
			time.Duration(config.Config().DownloadTokenExpire)).Unix()
		if ts, err = strconv.ParseInt(timestamp, 10, 64); err != nil {
			return false, errors.New("unvalid timestamp")
		}
		if ts > maxTimestamp || ts < minTimestamp {
			return false, errors.New("timestamp expire")
		}
		fullpath, smallPath = this.GetFilePathFromRequest(w, r)
		if smallPath != "" {
			pathMd5 = this.util.MD5(smallPath)
		} else {
			pathMd5 = this.util.MD5(fullpath)
		}
		if fileInfo, err = this.GetFileInfoFromLevelDB(pathMd5); err != nil {
			// TODO
		} else {
			ok := CheckToken(token, fileInfo.Md5, timestamp)
			if !ok {
				return ok, errors.New("unvalid token")
			}
			return ok, nil
		}
	}
	return true, nil
}

func (this *server) GetSmallFileByURI(w http.ResponseWriter, r *http.Request) ([]byte, bool, error) {
	var (
		err      error
		data     []byte
		offset   int64
		length   int
		fullpath string
		info     os.FileInfo
	)
	fullpath, _ = this.GetFilePathFromRequest(w, r)
	if _, offset, length, err = this.ParseSmallFile(r.RequestURI); err != nil {
		return nil, false, err
	}
	if info, err = os.Stat(fullpath); err != nil {
		return nil, false, err
	}
	if info.Size() < offset+int64(length) {
		return nil, true, errors.New("noFound")
	} else {
		data, err = this.util.ReadFileByOffSet(fullpath, offset, length)
		if err != nil {
			return nil, false, err
		}
		return data, false, err
	}
}


//image
func (this *server) ResizeImageByBytes(w http.ResponseWriter, data []byte, width, height uint) {
	var (
		img     image.Image
		err     error
		imgType string
	)
	reader := bytes.NewReader(data)
	img, imgType, err = image.Decode(reader)
	if err != nil {
		_ = log.Error(err)
		return
	}
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		_ = jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		_ = png.Encode(w, img)
	} else {
		_, _ = w.Write(data)
	}
}

func (this *server) ResizeImage(w http.ResponseWriter, fullpath string, width, height uint) {
	var (
		img     image.Image
		err     error
		imgType string
		file    *os.File
	)
	file, err = os.Open(fullpath)
	if err != nil {
		_ = log.Error(err)
		return
	}
	img, imgType, err = image.Decode(file)
	if err != nil {
		_ = log.Error(err)
		return
	}
	_ = file.Close()
	img = resize.Resize(width, height, img, resize.Lanczos3)
	if imgType == "jpg" || imgType == "jpeg" {
		_ = jpeg.Encode(w, img, nil)
	} else if imgType == "png" {
		_ = png.Encode(w, img)
	} else {
		_, _ = file.Seek(0, 0)
		_, _ = io.Copy(w, file)
	}
}




func (this *server) getRequestURI(action string) string {
	var (
		uri string
	)
	if config.Config().SupportGroupManage {
		uri = "/" + config.Config().Group + "/" + action
	} else {
		uri = "/" + action
	}
	return uri
}

func (this *server) GetClusterNotPermitMessage(r *http.Request) string {
	var (
		message string
	)
	message = fmt.Sprintf(config.CONST_MESSAGE_CLUSTER_IP, this.util.GetClientIp(r))
	return message
}


func (this *server) GetServerURI(r *http.Request) string {
	return fmt.Sprintf("http://%s/", r.Host)
}

func (this *server) FormatStatInfo() {
	var (
		data  []byte
		err   error
		count int64
		stat  map[string]interface{}
	)
	if this.util.FileExists(config.CONST_STAT_FILE_NAME) {
		if data, err = this.util.ReadBinFile(config.CONST_STAT_FILE_NAME); err != nil {
			_ = log.Error(err)
		} else {
			if err = json.Unmarshal(data, &stat); err != nil {
				_ = log.Error(err)
			} else {
				for k, v := range stat {
					switch v.(type) {
					case float64:
						vv := strings.Split(fmt.Sprintf("%f", v), ".")[0]
						if count, err = strconv.ParseInt(vv, 10, 64); err != nil {
							_ = log.Error(err)
						} else {
							this.statMap.Put(k, count)
						}
					default:
						this.statMap.Put(k, v)
					}
				}
			}
		}
	} else {
		this.RepairStatByDate(this.util.GetToDay())
	}
}

func (this *server) CheckScene(scene string) (bool, error) {
	var (
		scenes []string
	)
	if len(config.Config().Scenes) == 0 {
		return true, nil
	}
	for _, s := range config.Config().Scenes {
		scenes = append(scenes, strings.Split(s, ":")[0])
	}
	if !this.util.Contains(scene, scenes) {
		return false, errors.New("not valid scene")
	}
	return true, nil
}


func (this *server) BuildFileResult(fileInfo *model.FileInfo, r *http.Request) model.FileResult {
	var (
		outname     string
		fileResult  model.FileResult
		p           string
		downloadUrl string
		domain      string
	)
	if !strings.HasPrefix(config.Config().DownloadDomain, "http") {
		if config.Config().DownloadDomain == "" {
			config.Config().DownloadDomain = fmt.Sprintf("http://%s", r.Host)
		} else {
			config.Config().DownloadDomain = fmt.Sprintf("http://%s", config.Config().DownloadDomain)
		}
	}
	if config.Config().DownloadDomain != "" {
		domain = config.Config().DownloadDomain
	} else {
		domain = fmt.Sprintf("http://%s", r.Host)
	}
	outname = fileInfo.Name
	if fileInfo.ReName != "" {
		outname = fileInfo.ReName
	}
	p = strings.Replace(fileInfo.Path, config.STORE_DIR_NAME+"/", "", 1)
	if config.Config().SupportGroupManage {
		p = config.Config().Group + "/" + p + "/" + outname
	} else {
		p = p + "/" + outname
	}
	downloadUrl = fmt.Sprintf("http://%s/%s", r.Host, p)
	if config.Config().DownloadDomain != "" {
		downloadUrl = fmt.Sprintf("%s/%s", config.Config().DownloadDomain, p)
	}
	fileResult.Url = downloadUrl
	fileResult.Md5 = fileInfo.Md5
	fileResult.Path = "/" + p
	fileResult.Domain = domain
	fileResult.Scene = fileInfo.Scene
	fileResult.Size = fileInfo.Size
	fileResult.ModTime = fileInfo.TimeStamp
	// Just for Compatibility
	fileResult.Src = fileResult.Path
	fileResult.Scenes = fileInfo.Scene
	return fileResult
}


//md5
func (this *server) GetMd5sMapByDate(date string, filename string) (*goutil.CommonMap, error) {
	var (
		err     error
		result  *goutil.CommonMap
		fpath   string
		content string
		lines   []string
		line    string
		cols    []string
		data    []byte
	)
	result = goutil.NewCommonMap(0)
	if filename == "" {
		fpath = config.DATA_DIR + "/" + date + "/" + config.CONST_FILE_Md5_FILE_NAME
	} else {
		fpath = config.DATA_DIR + "/" + date + "/" + filename
	}
	if !this.util.FileExists(fpath) {
		return result, errors.New(fmt.Sprintf("fpath %s not found", fpath))
	}
	if data, err = ioutil.ReadFile(fpath); err != nil {
		return result, err
	}
	content = string(data)
	lines = strings.Split(content, "\n")
	for _, line = range lines {
		cols = strings.Split(line, "|")
		if len(cols) > 2 {
			if _, err = strconv.ParseInt(cols[1], 10, 64); err != nil {
				continue
			}
			result.Add(cols[0])
		}
	}
	return result, nil
}

func (this *server) GetMd5sByDate(date string, filename string) (mapset.Set, error) {
	var (
		keyPrefix string
		md5set    mapset.Set
		keys      []string
	)
	md5set = mapset.NewSet()
	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)
	iter := this.logDB.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		keys = strings.Split(string(iter.Key()), "_")
		if len(keys) >= 3 {
			md5set.Add(keys[2])
		}
	}
	iter.Release()
	return md5set, nil
}

func (this *server) SaveFileMd5Log(fileInfo *model.FileInfo, filename string) {
	var (
		info model.FileInfo
	)
	for len(this.queueFileLog)+len(this.queueFileLog)/10 > CONST_QUEUE_SIZE {
		time.Sleep(time.Second * 1)
	}
	info = *fileInfo
	this.queueFileLog <- &model.FileLog{FileInfo: &info, FileName: filename}
}


//存储文件日志和文件
func (this *server) saveFileMd5Log(fileInfo *model.FileInfo, filename string) {
	var (
		err      error
		logDate  string
		ok       bool
		logKey   string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("saveFileMd5Log")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	if fileInfo == nil || fileInfo.Md5 == "" || filename == "" {
		_ = log.Warn("saveFileMd5Log", fileInfo, filename)
		return
	}
	logDate = this.util.GetDayFromTimeStamp(fileInfo.TimeStamp)
	logKey = fmt.Sprintf("%s_%s_%s", logDate, filename, fileInfo.Md5)
	if filename == config.CONST_FILE_Md5_FILE_NAME {
		//this.searchMap.Put(fileInfo.Md5, fileInfo.Name)
		if ok, err = this.IsExistFromLevelDB(fileInfo.Md5, this.ldb); !ok {
			this.statMap.AddCountInt64(logDate+"_"+config.CONST_STAT_FILE_COUNT_KEY, 1)
			this.statMap.AddCountInt64(logDate+"_"+config.CONST_STAT_FILE_TOTAL_SIZE_KEY, fileInfo.Size)
			this.SaveStat()
		}
		if _, err = this.SaveFileInfoToLevelDB(logKey, fileInfo, this.logDB); err != nil {
			_ = log.Error(err)
		}
		dao,_:=model.GetDao()
		if dao!=nil{
			_ = dao.InsertFileInfo(fileInfo)
		}
		return
	}
	if filename == config.CONST_REMOME_Md5_FILE_NAME {
		//this.searchMap.Remove(fileInfo.Md5)
		if ok, err = this.IsExistFromLevelDB(fileInfo.Md5, this.ldb); ok {
			this.statMap.AddCountInt64(logDate+"_"+config.CONST_STAT_FILE_COUNT_KEY, -1)
			this.statMap.AddCountInt64(logDate+"_"+config.CONST_STAT_FILE_TOTAL_SIZE_KEY, -fileInfo.Size)
			this.SaveStat()
		}
		_ = this.RemoveKeyFromLevelDB(logKey, this.logDB)

		dao,_:=model.GetDao()
		if dao!=nil{
			_ = dao.DeleteFileInfoById(fileInfo.ID)
		}
		// remove files.md5 for stat info(repair from logDB)
		logKey = fmt.Sprintf("%s_%s_%s", logDate, config.CONST_FILE_Md5_FILE_NAME, fileInfo.Md5)
		_ = this.RemoveKeyFromLevelDB(logKey, this.logDB)
		return
	}
	_, _ = this.SaveFileInfoToLevelDB(logKey, fileInfo, this.logDB)
}

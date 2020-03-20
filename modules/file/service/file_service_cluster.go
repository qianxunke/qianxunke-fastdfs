package service

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/httplib"
	mapset "github.com/deckarep/golang-set"
	log "github.com/sjqzhang/seelog"
	"net/http"
	"os"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
	"strings"
	"time"
)

//集群数据同步
/**

 */
func (s *server) postFileToPeer(fileInfo *model.FileInfo) {
	var (
		err      error
		peer     string
		filename string
		info     *model.FileInfo
		postURL  string
		result   string
		fi       os.FileInfo
		i        int
		data     []byte
		fpath    string
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("postFileToPeer")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	//fmt.Println("postFile",fileInfo)
	for i, peer = range config.Config().Peers {
		_ = i
		if strings.Contains(fileInfo.PeerStr,peer) {
			continue
		}
		filename = fileInfo.Name
		if fileInfo.ReName != "" {
			filename = fileInfo.ReName
			if fileInfo.OffSet != -1 {
				filename = strings.Split(fileInfo.ReName, ",")[0]
			}
		}
		fpath = config.DOCKER_DIR + fileInfo.Path + "/" + filename
		if !s.util.FileExists(fpath) {
			_ = log.Warn(fmt.Sprintf("file '%s' not found", fpath))
			continue
		} else {
			if fileInfo.Size == 0 {
				if fi, err = os.Stat(fpath); err != nil {
					_ = log.Error(err)
				} else {
					fileInfo.Size = fi.Size()
				}
			}
		}
		if fileInfo.OffSet != -2 && config.Config().EnableDistinctFile {
			//not migrate file should check or update file
			// where not EnableDistinctFile should check
			if info, err = s.checkPeerFileExist(peer, fileInfo.Md5, ""); info.Md5 != "" {
				if len(fileInfo.PeerStr)>0{
					fileInfo.PeerStr+=","+peer
				}else {
					fileInfo.PeerStr=peer
				}
				if _, err = s.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, s.ldb); err != nil {
					_ = log.Error(err)
				}
				continue
			}
		}
		postURL = fmt.Sprintf("%s%s", peer, s.getRequestURI("syncfile_info"))
		b := httplib.Post(postURL)
		b.SetTimeout(time.Second*30, time.Second*30)
		if data, err = json.Marshal(fileInfo); err != nil {
			_ = log.Error(err)
			return
		}
		b.Param("fileInfo", string(data))
		result, err = b.String()
		if err != nil {
			if fileInfo.Retry <= config.Config().RetryCount {
				fileInfo.Retry = fileInfo.Retry + 1
				s.PeerAppendToQueue(fileInfo)
			}
			_ = log.Error(err, fmt.Sprintf(" path:%s", fileInfo.Path+"/"+fileInfo.Name))
		}
		if !strings.HasPrefix(result, "http://") || err != nil {
			//this.SaveFileMd5Log(fileInfo, config.CONST_Md5_ERROR_FILE_NAME)
			_ = log.Error(err)
		}
		if strings.HasPrefix(result, "http://") {
			log.Info(result)
			if !strings.Contains(fileInfo.PeerStr,peer) {
				if len(fileInfo.PeerStr)<=0{
					fileInfo.PeerStr=peer
				}else {
					fileInfo.PeerStr+=","+peer
				}
				if _, err = s.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, s.ldb); err != nil {
					_ = log.Error(err)
				}
			}
		}
		if err != nil {
			_ = log.Error(err)
		}
	}
}

func (s *server) DownloadFromPeer(peer string, fileInfo *model.FileInfo) {
	var (
		err         error
		filename    string
		fpath       string
		fpathTmp    string
		fi          os.FileInfo
		sum         string
		data        []byte
		downloadUrl string
	)
	if config.Config().ReadOnly {
		log.Warn("ReadOnly", fileInfo)
		return
	}
	if config.Config().RetryCount > 0 && fileInfo.Retry >= config.Config().RetryCount {
		log.Error("DownloadFromPeer Error ", fileInfo)
		return
	} else {
		fileInfo.Retry = fileInfo.Retry + 1
	}
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	if fileInfo.OffSet != -2 && config.Config().EnableDistinctFile && s.CheckFileExistByInfo(fileInfo.Md5, fileInfo) {
		// ignore migrate file
		log.Info(fmt.Sprintf("DownloadFromPeer file Exist, path:%s", fileInfo.Path+"/"+fileInfo.Name))
		return
	}
	if (!config.Config().EnableDistinctFile || fileInfo.OffSet == -2) && s.util.FileExists(s.GetFilePathByInfo(fileInfo, true)) {
		// ignore migrate file
		if fi, err = os.Stat(s.GetFilePathByInfo(fileInfo, true)); err == nil {
			if fi.ModTime().Unix() > fileInfo.TimeStamp {
				log.Info(fmt.Sprintf("ignore file sync path:%s", s.GetFilePathByInfo(fileInfo, false)))
				fileInfo.TimeStamp = fi.ModTime().Unix()
				s.postFileToPeer(fileInfo) // keep newer
				return
			}
			os.Remove(s.GetFilePathByInfo(fileInfo, true))
		}
	}
	if _, err = os.Stat(fileInfo.Path); err != nil {
		os.MkdirAll(config.DOCKER_DIR+fileInfo.Path, 0775)
	}
	//fmt.Println("downloadFromPeer",fileInfo)
	p := strings.Replace(fileInfo.Path, config.STORE_DIR_NAME+"/", "", 1)
	//filename=this.util.UrlEncode(filename)
	downloadUrl = peer + "/" + config.Config().Group + "/" + p + "/" + filename
	log.Info("DownloadFromPeer: ", downloadUrl)
	fpath = config.DOCKER_DIR + fileInfo.Path + "/" + filename
	fpathTmp = config.DOCKER_DIR + fileInfo.Path + "/" + fmt.Sprintf("%s_%s", "tmp_", filename)
	timeout := fileInfo.Size/1024/1024/1 + 30
	if config.Config().SyncTimeout > 0 {
		timeout = config.Config().SyncTimeout
	}
	s.lockMap.LockKey(fpath)
	defer s.lockMap.UnLockKey(fpath)
	download_key := fmt.Sprintf("downloading_%d_%s", time.Now().Unix(), fpath)
	s.ldb.Put([]byte(download_key), []byte(""), nil)
	defer func() {
		s.ldb.Delete([]byte(download_key), nil)
	}()
	if fileInfo.OffSet == -2 {
		//migrate file
		if fi, err = os.Stat(fpath); err == nil && fi.Size() == fileInfo.Size {
			//prevent double download
			s.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, s.ldb)
			//log.Info(fmt.Sprintf("file '%s' has download", fpath))
			return
		}
		req := httplib.Get(downloadUrl)
		req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
		if err = req.ToFile(fpathTmp); err != nil {
			s.AppendToDownloadQueue(fileInfo) //retry
			os.Remove(fpathTmp)
			log.Error(err, fpathTmp)
			return
		}
		if os.Rename(fpathTmp, fpath) == nil {
			//this.SaveFileMd5Log(fileInfo, CONST_FILE_Md5_FILE_NAME)
			s.SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, s.ldb)
		}
		return
	}
	req := httplib.Get(downloadUrl)
	req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
	if fileInfo.OffSet >= 0 {
		//small file download
		data, err = req.Bytes()
		if err != nil {
			s.AppendToDownloadQueue(fileInfo) //retry
			log.Error(err)
			return
		}
		data2 := make([]byte, len(data)+1)
		data2[0] = '1'
		for i, v := range data {
			data2[i+1] = v
		}
		data = data2
		if int64(len(data)) != fileInfo.Size {
			log.Warn("file size is error")
			return
		}
		fpath = strings.Split(fpath, ",")[0]
		err = s.util.WriteFileByOffSet(fpath, fileInfo.OffSet, data)
		if err != nil {
			log.Warn(err)
			return
		}
		s.SaveFileMd5Log(fileInfo, config.CONST_FILE_Md5_FILE_NAME)
		return
	}
	if err = req.ToFile(fpathTmp); err != nil {
		s.AppendToDownloadQueue(fileInfo) //retry
		os.Remove(fpathTmp)
		log.Error(err)
		return
	}
	if fi, err = os.Stat(fpathTmp); err != nil {
		os.Remove(fpathTmp)
		return
	}
	_ = sum
	if fi.Size() != fileInfo.Size { //  maybe has bug remove || sum != fileInfo.Md5
		log.Error("file sum check error")
		os.Remove(fpathTmp)
		return
	}
	if os.Rename(fpathTmp, fpath) == nil {
		s.SaveFileMd5Log(fileInfo, config.CONST_FILE_Md5_FILE_NAME)
	}
}


func (s *server) CheckFileAndSendToPeer(date string, filename string, isForceUpload bool) {
	var (
		md5set mapset.Set
		err    error
		md5s   []interface{}
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			_ = log.Error("CheckFileAndSendToPeer")
			_ = log.Error(re)
			_ = log.Error(string(buffer))
		}
	}()
	if md5set, err = s.GetMd5sByDate(date, filename); err != nil {
		_ = log.Error(err)
		return
	}
	md5s = md5set.ToSlice()
	for _, md := range md5s {
		if md == nil {
			continue
		}
		if fileInfo, _ := s.GetFileInfoFromLevelDB(md.(string)); fileInfo != nil && fileInfo.Md5 != "" {
			peers:=strings.Split(fileInfo.PeerStr,",")
			if peers==nil{
				peers=[]string{}
			}
			if isForceUpload {
				peers=[]string{}
			}
			if len(peers) > len(config.Config().Peers) {
				continue
			}
			if !strings.Contains(fileInfo.PeerStr, s.host) {
				if len(fileInfo.PeerStr)>0{
					fileInfo.PeerStr+=","+ s.host
				}else {
					fileInfo.PeerStr= s.host
				}
			}
			if filename == config.CONST_Md5_QUEUE_FILE_NAME {
				s.AppendToDownloadQueue(fileInfo)
			} else {
				s.PeerAppendToQueue(fileInfo)
			}
		}
	}
}

func (s *server) checkPeerFileExist(peer string, md5sum string, fpath string) (*model.FileInfo, error) {
	var (
		err      error
		fileInfo model.FileInfo
	)
	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", peer, s.getRequestURI("check_file_exist"), md5sum))
	req.Param("path", fpath)
	req.Param("md5", md5sum)
	req.SetTimeout(time.Second*5, time.Second*10)
	if err = req.ToJSON(&fileInfo); err != nil {
		return &model.FileInfo{}, err
	}
	if fileInfo.Md5 == "" {
		return &fileInfo, errors.New("not found")
	}
	return &fileInfo, nil
}

func (s *server) Sync(w http.ResponseWriter, r *http.Request) {
	var (
		result model.JsonResult
	)
	_ = r.ParseForm()
	result.Status = "fail"
	if !s.IsPeer(r) {
		result.Message = "client must be in cluster"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	date := ""
	force := ""
	inner := ""
	isForceUpload := false
	force = r.FormValue("force")
	date = r.FormValue("date")
	inner = r.FormValue("inner")
	if force == "1" {
		isForceUpload = true
	}
	if inner != "1" {
		for _, peer := range config.Config().Peers {
			req := httplib.Post(peer + s.getRequestURI("sync"))
			req.Param("force", force)
			req.Param("inner", "1")
			req.Param("date", date)
			if _, err := req.String(); err != nil {
				_ = log.Error(err)
			}
		}
	}
	if date == "" {
		result.Message = "require paramete date &force , ?date=20181230"
		_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
		return
	}
	date = strings.Replace(date, ".", "", -1)
	if isForceUpload {
		go s.CheckFileAndSendToPeer(date, config.CONST_FILE_Md5_FILE_NAME, isForceUpload)
	} else {
		go s.CheckFileAndSendToPeer(date, config.CONST_Md5_ERROR_FILE_NAME, isForceUpload)
	}
	result.Status = "ok"
	result.Message = "job is running"
	_, _ = w.Write([]byte(s.util.JsonEncodePretty(result)))
}


func (s *server) IsPeer(r *http.Request) bool {
	var (
		ip    string
		peer  string
		bflag bool
	)
	//return true
	ip = s.util.GetClientIp(r)
	realIp := os.Getenv("GO_FASTDFS_IP")
	if realIp == "" {
		realIp = s.util.GetPulicIP()
	}
	if ip == "127.0.0.1" || ip == realIp {
		return true
	}
	if s.util.Contains(ip, config.Config().AdminIps) {
		return true
	}
	ip = "http://" + ip
	bflag = false
	for _, peer = range config.Config().Peers {
		if strings.HasPrefix(peer, ip) {
			bflag = true
			break
		}
	}
	return bflag
}


func (s *server) ConsumerPostToPeer() {
	ConsumerFunc := func() {
		for {
			fileInfo := <-s.queueToPeers
			s.postFileToPeer(&fileInfo)
		}
	}
	for i := 0; i < config.Config().SyncWorker; i++ {
		go ConsumerFunc()
	}
}

func (s *server) LoadQueueSendToPeer() {
	if queue, err := s.LoadFileInfoByDate(s.util.GetToDay(), config.CONST_Md5_QUEUE_FILE_NAME); err != nil {
		_=log.Error(err)
	} else {
		for fileInfo := range queue.Iter() {
			//this.queueFromPeers <- *fileInfo.(*FileInfo)
			s.AppendToDownloadQueue(fileInfo.(*model.FileInfo))
		}
	}
}

func (s *server) CheckClusterStatus() {
	check := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				_=log.Error("CheckClusterStatus")
				_=log.Error(re)
				_=log.Error(string(buffer))
			}
		}()
		var (
			status  model.JsonResult
			err     error
			subject string
			body    string
			req     *httplib.BeegoHTTPRequest
		)
		for _, peer := range config.Config().Peers {
			req = httplib.Get(fmt.Sprintf("%s%s", peer, s.getRequestURI("status")))
			req.SetTimeout(time.Second*5, time.Second*5)
			err = req.ToJSON(&status)
			if err != nil || status.Status != "ok" {
				for _, to := range config.Config().AlarmReceivers {
					subject = "fastdfs server error"
					if err != nil {
						body = fmt.Sprintf("%s\nserver:%s\nerror:\n%s", subject, peer, err.Error())
					} else {
						body = fmt.Sprintf("%s\nserver:%s\n", subject, peer)
					}
					if err = s.SendToMail(to, subject, body, "text"); err != nil {
						log.Error(err)
					}
				}
				if config.Config().AlarmUrl != "" {
					req = httplib.Post(config.Config().AlarmUrl)
					req.SetTimeout(time.Second*10, time.Second*10)
					req.Param("message", body)
					req.Param("subject", subject)
					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			check()
		}
	}()
}


func (s *server) PeerAppendToQueue(fileInfo *model.FileInfo) {

	for (len(s.queueToPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	s.queueToPeers <- *fileInfo
}

func (s *server) AppendToDownloadQueue(fileInfo *model.FileInfo) {
	for (len(s.queueFromPeers) + CONST_QUEUE_SIZE/10) > CONST_QUEUE_SIZE {
		time.Sleep(time.Millisecond * 50)
	}
	s.queueFromPeers <- *fileInfo
}

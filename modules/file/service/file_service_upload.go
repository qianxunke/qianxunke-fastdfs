package service

import (
	"errors"
	"fmt"
	log "github.com/sjqzhang/seelog"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
	"strings"
	"time"
)

func (this *server) ConsumerUpload() {
	ConsumerFunc := func() {
		for {
			wr := <-this.queueUpload
			this.upload(*wr.W, wr.R)
			this.rtMap.AddCountInt64(config.CONST_UPLOAD_COUNTER_KEY, wr.R.ContentLength)
			if v, ok := this.rtMap.GetValue(config.CONST_UPLOAD_COUNTER_KEY); ok {
				if v.(int64) > 1*1024*1024*1024 {
					var _v int64
					this.rtMap.Put(config.CONST_UPLOAD_COUNTER_KEY, _v)
					debug.FreeOSMemory()
				}
			}
			wr.Done <- true
		}
	}
	for i := 0; i < config.Config().UploadWorker; i++ {
		go ConsumerFunc()
	}
}

func (this *server) upload(w http.ResponseWriter, r *http.Request) {
	var (
		err          error
		md5sum       string
		fileInfo     model.FileInfo
		uploadFile   multipart.File
		uploadHeader *multipart.FileHeader
		scene        string
		output       string
		fileResult   model.FileResult
		data         []byte
	)
	output = r.FormValue("output")
	if config.Config().EnableCrossOrigin {
		this.CrossOrigin(w, r)
		if r.Method == http.MethodOptions {
			return
		}
	}

	if config.Config().AuthUrl != "" {
		if !this.CheckAuth(w, r) {
			_ = log.Warn("auth fail", r.Form)
			this.NotPermit(w, r)
			_, _ = w.Write([]byte("auth fail"))
			return
		}
	}
	if r.Method == http.MethodPost  {

		md5sum = r.FormValue("md5")
		output = r.FormValue("output")
		if config.Config().ReadOnly {
			_, _ = w.Write([]byte("(error) readonly"))
			return
		}
		if config.Config().EnableCustomPath {
			fileInfo.Path = r.FormValue("path")
			fileInfo.Path = strings.Trim(fileInfo.Path, "/")
		}
		scene = r.FormValue("scene")
		if scene == "" {
			scene = r.FormValue("scenes")
		}
		fileInfo.Md5 = md5sum
		fileInfo.OffSet = -1
		fileInfo.Status = -1
		if uploadFile, uploadHeader, err = r.FormFile("file"); err != nil {
			_ = log.Error(err)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		fileInfo.PeerStr=""
		fileInfo.TimeStamp = time.Now().Unix()
		if scene == "" {
			scene = config.Config().DefaultScene
		}
		if output == "" {
			output = "text"
		}
		if !this.util.Contains(output, []string{"json", "text"}) {
			_, _ = w.Write([]byte("output just support json or text"))
			return
		}
		fileInfo.Scene = scene
		if _, err = this.CheckScene(scene); err != nil {
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err = this.SaveUploadFile(uploadFile, uploadHeader, &fileInfo, r); err != nil {
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		//去重
		if config.Config().EnableDistinctFile {
			if v, _ := this.GetFileInfoFromLevelDB(fileInfo.Md5); v != nil && v.Md5 != "" {
				fileResult = this.BuildFileResult(v, r)
				if config.Config().RenameFile {
					_ = os.Remove(config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.ReName)
				} else {
					_ = os.Remove(config.DOCKER_DIR + fileInfo.Path + "/" + fileInfo.Name)
				}
				if output == "json" {
					if data, err = json.Marshal(fileResult); err != nil {
						_ = log.Error(err)
						_, _ = w.Write([]byte(err.Error()))
					}
					_, _ = w.Write(data)
				} else {
					_, _ = w.Write([]byte(fileResult.Url))
				}
				return
			}
		}
		if fileInfo.Md5 == "" {
			_ = log.Warn(" fileInfo.Md5 is null")
			return
		}
		if md5sum != "" && fileInfo.Md5 != md5sum {
			_ = log.Warn(" fileInfo.Md5 and md5sum !=")
			return
		}
		if !config.Config().EnableDistinctFile {
			// bugfix filecount stat
			fileInfo.Md5 = this.util.MD5(this.GetFilePathByInfo(&fileInfo, false))
		}
		if config.Config().EnableMergeSmallFile && fileInfo.Size < config.CONST_SMALL_FILE_SIZE {
			if err = this.SaveSmallFile(&fileInfo); err != nil {
				_ = log.Error(err)
				return
			}
		}
		this.saveFileMd5Log(&fileInfo, config.CONST_FILE_Md5_FILE_NAME) //maybe slow
		//集群同步
		go this.postFileToPeer(&fileInfo)

		if fileInfo.Size <= 0 {
			_ = log.Error("file size is zero")
			return
		}
		fileResult = this.BuildFileResult(&fileInfo, r)
		if output == "json" {
			if data, err = json.Marshal(fileResult); err != nil {
				_ = log.Error(err)
				_, _ = w.Write([]byte(err.Error()))
			}
			_, _ = w.Write(data)
		} else {
			_, _ = w.Write([]byte(fileResult.Url))
		}
		return
	} else {
		md5sum = r.FormValue("md5")
		output = r.FormValue("output")
		if md5sum == "" {
			_, _ = w.Write([]byte("(error) if you want to upload fast md5 is require" +
				",and if you want to upload file,you must use post method  "))
			return
		}
		if v, _ := this.GetFileInfoFromLevelDB(md5sum); v != nil && v.Md5 != "" {
			fileResult = this.BuildFileResult(v, r)
		}
		if output == "json" {
			if data, err = json.Marshal(fileResult); err != nil {
				_ = log.Error(err)
				_, _ = w.Write([]byte(err.Error()))
			}
			_, _ = w.Write(data)
		} else {
			_, _ = w.Write([]byte(fileResult.Url))
		}
	}
}

func (this *server) SaveSmallFile(fileInfo *model.FileInfo) error {
	var (
		err      error
		filename string
		fpath    string
		srcFile  *os.File
		desFile  *os.File
		largeDir string
		destPath string
		reName   string
		fileExt  string
	)
	filename = fileInfo.Name
	fileExt = path.Ext(filename)
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	fpath = config.DOCKER_DIR + fileInfo.Path + "/" + filename
	largeDir = config.LARGE_DIR + "/" + config.Config().PeerId
	if !this.util.FileExists(largeDir) {
		_ = os.MkdirAll(largeDir, 0775)
	}
	reName = fmt.Sprintf("%d", this.util.RandInt(100, 300))
	destPath = largeDir + "/" + reName
	this.lockMap.LockKey(destPath)
	defer this.lockMap.UnLockKey(destPath)
	if this.util.FileExists(fpath) {
		srcFile, err = os.OpenFile(fpath, os.O_CREATE|os.O_RDONLY, 06666)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		desFile, err = os.OpenFile(destPath, os.O_CREATE|os.O_RDWR, 06666)
		if err != nil {
			return err
		}
		defer desFile.Close()
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if _, err = desFile.Write([]byte("1")); err != nil {
			//first byte set 1
			return err
		}
		fileInfo.OffSet, err = desFile.Seek(0, 2)
		if err != nil {
			return err
		}
		fileInfo.OffSet = fileInfo.OffSet - 1 //minus 1 byte
		fileInfo.Size = fileInfo.Size + 1
		fileInfo.ReName = fmt.Sprintf("%s,%d,%d,%s", reName, fileInfo.OffSet, fileInfo.Size, fileExt)
		if _, err = io.Copy(desFile, srcFile); err != nil {
			return err
		}
		_ = srcFile.Close()
		_ = os.Remove(fpath)
		fileInfo.Path = strings.Replace(largeDir, config.DOCKER_DIR, "", 1)
	}
	return nil
}

func (this *server) SaveUploadFile(file multipart.File, header *multipart.FileHeader, fileInfo *model.FileInfo, r *http.Request) (*model.FileInfo, error) {
	var (
		err     error
		outFile *os.File
		folder  string
		fi      os.FileInfo
	)
	defer file.Close()
	_, fileInfo.Name = filepath.Split(header.Filename)
	// bugfix for ie upload file contain fullpath
	if len(config.Config().Extensions) > 0 && !this.util.Contains(path.Ext(fileInfo.Name), config.Config().Extensions) {
		return fileInfo, errors.New("(error)file extension mismatch")
	}

	if config.Config().RenameFile {
		fileInfo.ReName = this.util.MD5(this.util.GetUUID()) + path.Ext(fileInfo.Name)
	}
	folder = time.Now().Format("20060102/15/04")
	if config.Config().PeerId != "" {
		folder = fmt.Sprintf(folder+"/%s", config.Config().PeerId)
	}
	if fileInfo.Scene != "" {
		folder = fmt.Sprintf(config.STORE_DIR+"/%s/%s", fileInfo.Scene, folder)
	} else {
		folder = fmt.Sprintf(config.STORE_DIR+"/%s", folder)
	}
	if fileInfo.Path != "" {
		if strings.HasPrefix(fileInfo.Path, config.STORE_DIR) {
			folder = fileInfo.Path
		} else {
			folder = config.STORE_DIR + "/" + fileInfo.Path
		}
	}
	if !this.util.FileExists(folder) {
		_ = os.MkdirAll(folder, 0775)
	}
	outPath := fmt.Sprintf(folder+"/%s", fileInfo.Name)
	if config.Config().RenameFile {
		outPath = fmt.Sprintf(folder+"/%s", fileInfo.ReName)
	}
    //避免用户自定义路径造成文件覆盖
	if this.util.FileExists(outPath) && config.Config().EnableDistinctFile {
		for i := 0; i < 1000; i++ {
			outPath = fmt.Sprintf(folder+"/%d_%s", i, filepath.Base(header.Filename))
			fileInfo.Name = fmt.Sprintf("%d_%s", i, header.Filename)
			if !this.util.FileExists(outPath) {
				break
			}
		}
	}

	log.Info(fmt.Sprintf("upload: %s", outPath))
	if outFile, err = os.Create(outPath); err != nil {
		_ = log.Error(err)
		return fileInfo, err
	}
	defer outFile.Close()

	if _, err = io.Copy(outFile, file); err != nil {
		_ = log.Error(err)
		return fileInfo, errors.New("(error)fail," + err.Error())
	}
	if fi, err = outFile.Stat(); err != nil {
		_ = log.Error(err)
	} else {
		fileInfo.Size = fi.Size()
	}
	if fi.Size() != header.Size {
		return fileInfo, errors.New("(error)file uncomplete")
	}
	v := "" // this.util.GetFileSum(outFile, Config().FileSumArithmetic)
	if config.Config().EnableDistinctFile {
		v = this.util.GetFileSum(outFile, config.Config().FileSumArithmetic)
	} else {
		v = this.util.MD5(this.GetFilePathByInfo(fileInfo, false))
	}
	fileInfo.Md5 = v
	//fileInfo.Path = folder //strings.Replace( folder,DOCKER_DIR,"",1)
	fileInfo.Path = strings.Replace(folder, config.DOCKER_DIR, "", 1)
	if len(fileInfo.PeerStr)<=0{
		fileInfo.PeerStr=this.host
	}else {
		fileInfo.PeerStr +=","+this.host
	}

	return fileInfo, nil
}

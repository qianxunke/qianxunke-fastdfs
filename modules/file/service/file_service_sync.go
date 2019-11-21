package service

import (
	"fmt"
	log "github.com/sjqzhang/seelog"
	"net/http"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"strings"
)

func (this *server) SyncFileInfo(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		fileInfo    model.FileInfo
		fileInfoStr string
		filename    string
	)
	_ = r.ParseForm()
	if !this.IsPeer(r) {
		return
	}
	fileInfoStr = r.FormValue("fileInfo")
	if err = json.Unmarshal([]byte(fileInfoStr), &fileInfo); err != nil {
		_, _ = w.Write([]byte(this.GetClusterNotPermitMessage(r)))
		_ = log.Error(err)
		return
	}
	if fileInfo.OffSet == -2 {
		// optimize migrate
		_, _ = this.SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, this.ldb)
	} else {
		this.SaveFileMd5Log(&fileInfo, config.CONST_Md5_QUEUE_FILE_NAME)
	}
	this.AppendToDownloadQueue(&fileInfo)
	filename = fileInfo.Name
	if fileInfo.ReName != "" {
		filename = fileInfo.ReName
	}
	p := strings.Replace(fileInfo.Path, config.STORE_DIR+"/", "", 1)
	downloadUrl := fmt.Sprintf("http://%s/%s", r.Host, config.Config().Group+"/"+p+"/"+filename)
	log.Info("SyncFileInfo: ", downloadUrl)
	_, _ = w.Write([]byte(downloadUrl))
}

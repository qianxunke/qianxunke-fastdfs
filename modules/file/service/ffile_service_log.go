package service

import "qianxunke-fastdfs/modules/file/model"

func (this *server) ConsumerLog() {
	go func() {
		var (
			fileLog *model.FileLog
		)
		for {
			fileLog = <-this.queueFileLog
			this.saveFileMd5Log(fileLog.FileInfo, fileLog.FileName)
		}
	}()
}

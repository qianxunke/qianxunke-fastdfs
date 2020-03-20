package service

import "qianxunke-fastdfs/modules/file/model"

func (s *server) ConsumerLog() {
	go func() {
		var (
			fileLog *model.FileLog
		)
		for {
			fileLog = <-s.queueFileLog
			s.saveFileMd5Log(fileLog.FileInfo, fileLog.FileName)
		}
	}()
}

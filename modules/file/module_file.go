package file

import (
	"qianxunke-fastdfs/modules/file/handler"
	"qianxunke-fastdfs/modules/file/model"
	"qianxunke-fastdfs/modules/file/service"
)

func Run()  {
	model.Init()
	service.Init()
	handler.Init()
	handler.HandlerRegister()
}

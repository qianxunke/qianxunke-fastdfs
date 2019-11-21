package handler

import (
	"fmt"
	"log"
	"net/http"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/service"
	"runtime/debug"
	"time"
)

var (
	server service.ServiceInface
)

func Init() {
	var err error
	server, err = service.NewServer()
	if err != nil {
		log.Fatalf("[handler] error: %v", err)
	}
}

func HandlerRegister() {
	uploadPage := "upload.html"
	groupRoute := ""
	if config.Config().SupportGroupManage {
		groupRoute = "/" + config.Config().Group
	}
	if groupRoute == "" {
		http.HandleFunc(fmt.Sprintf("%s", "/"), server.Download)
		http.HandleFunc(fmt.Sprintf("/%s", uploadPage), server.Index)
	} else {
		http.HandleFunc(fmt.Sprintf("%s", "/"), server.Download)
		http.HandleFunc(fmt.Sprintf("%s", groupRoute), server.Download)
		http.HandleFunc(fmt.Sprintf("%s/%s", groupRoute, uploadPage), server.Index)
	}
	http.HandleFunc(fmt.Sprintf("%s/check_files_exist", groupRoute), server.CheckFilesExist)
		http.HandleFunc(fmt.Sprintf("%s/check_file_exist", groupRoute), server.CheckFileExist)
	http.HandleFunc(fmt.Sprintf("%s/upload", groupRoute), server.Upload)
	http.HandleFunc(fmt.Sprintf("%s/delete", groupRoute), server.RemoveFile)
	http.HandleFunc(fmt.Sprintf("%s/get_file_info", groupRoute), server.GetFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/sync", groupRoute), server.Sync)
	http.HandleFunc(fmt.Sprintf("%s/stat", groupRoute), server.Stat)
	http.HandleFunc(fmt.Sprintf("%s/repair_stat", groupRoute), server.RepairStatWeb)
	http.HandleFunc(fmt.Sprintf("%s/status", groupRoute), server.Status)
	http.HandleFunc(fmt.Sprintf("%s/repair", groupRoute), server.Repair)
	http.HandleFunc(fmt.Sprintf("%s/report", groupRoute), server.Report)
	http.HandleFunc(fmt.Sprintf("%s/backup", groupRoute), server.BackUp)
	http.HandleFunc(fmt.Sprintf("%s/search", groupRoute), server.Search)
	http.HandleFunc(fmt.Sprintf("%s/list_dir", groupRoute), server.ListDir)
	http.HandleFunc(fmt.Sprintf("%s/remove_empty_dir", groupRoute), server.RemoveEmptyDir)
	http.HandleFunc(fmt.Sprintf("%s/repair_fileinfo", groupRoute), server.RepairFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/reload", groupRoute), server.Reload)
	http.HandleFunc(fmt.Sprintf("%s/syncfile_info", groupRoute), server.SyncFileInfo)
	http.HandleFunc(fmt.Sprintf("%s/get_md5s_by_date", groupRoute), server.GetMd5sForWeb)
	http.HandleFunc(fmt.Sprintf("%s/receive_md5s", groupRoute), server.ReceiveMd5s)
	//	http.HandleFunc(fmt.Sprintf("%s/gen_google_secret", groupRoute), server.GenGoogleSecret)
	//	http.HandleFunc(fmt.Sprintf("%s/gen_google_code", groupRoute), server.GenGoogleCode)
	http.HandleFunc("/"+config.Config().Group+"/", server.Download)
	fmt.Println("Listen on " + config.Config().Addr)
	srv := &http.Server{
		Addr:              config.Config().Addr,
		Handler:           new(HttpHandler),
		ReadTimeout:       time.Duration(config.Config().ReadTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(config.Config().ReadHeaderTimeout) * time.Second,
		WriteTimeout:      time.Duration(config.Config().WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(config.Config().IdleTimeout) * time.Second,
	}
	err := srv.ListenAndServe()
	log.Println(err)
	fmt.Println(err)

}

type HttpHandler struct {
}

func (HttpHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	status_code := "200"
	defer func(t time.Time) {
		logStr := fmt.Sprintf("[Access] %s | %s | %s | %s | %s |%s",
			time.Now().Format("2006/01/02 - 15:04:05"),
			//res.Header(),
			time.Since(t).String(),
			req.Host,
			req.Method,
			status_code,
			req.RequestURI,
		)
		config.Logacc.Info(logStr)
	}(time.Now())
	defer func() {
		if err := recover(); err != nil {
			status_code = "500"
			res.WriteHeader(500)
			print(err)
			buff := debug.Stack()
			log.Println(err)
			log.Println(string(buff))
		}
	}()
	if config.Config().EnableCrossOrigin {
		CrossOrigin(res, req)
	}
	http.DefaultServeMux.ServeHTTP(res, req)
}
func CrossOrigin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, X-Requested-By, If-Modified-Since, X-File-Name, X-File-Type, Cache-Control, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	//https://blog.csdn.net/yanzisu_congcong/article/details/80552155
}

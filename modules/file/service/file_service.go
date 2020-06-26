package service

import (
	"flag"
	"fmt"
	"github.com/astaxie/beego/httplib"
	jsoniter "github.com/json-iterator/go"
	"github.com/sjqzhang/goutil"
	log "github.com/sjqzhang/seelog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"net/http"
	"os"
	"path/filepath"
	"qianxunke-fastdfs/modules/file/config"
	"qianxunke-fastdfs/modules/file/model"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

var staticHandler http.Handler
var json = jsoniter.ConfigCompatibleWithStandardLibrary
var ser *server
var m sync.RWMutex

var CONST_QUEUE_SIZE = 10000

type server struct {
	ldb            *leveldb.DB
	logDB          *leveldb.DB
	util           *goutil.Common
	statMap        *goutil.CommonMap
	sumMap         *goutil.CommonMap
	rtMap          *goutil.CommonMap
	queueToPeers   chan model.FileInfo
	queueFromPeers chan model.FileInfo
	queueFileLog   chan *model.FileLog
	queueUpload    chan model.WrapReqResp
	lockMap        *goutil.CommonMap
	sceneMap       *goutil.CommonMap
	searchMap      *goutil.CommonMap
	curDate        string
	host           string
}

type ServiceInface interface {
	Download(w http.ResponseWriter, r *http.Request)
	Index(w http.ResponseWriter, r *http.Request)
	CheckFileExist(w http.ResponseWriter, r *http.Request)
	CheckFilesExist(w http.ResponseWriter, r *http.Request)
	Upload(w http.ResponseWriter, r *http.Request)
	RemoveFile(w http.ResponseWriter, r *http.Request)
	GetFileInfo(w http.ResponseWriter, r *http.Request)
	Sync(w http.ResponseWriter, r *http.Request)
	Stat(w http.ResponseWriter, r *http.Request)
	RepairStatWeb(w http.ResponseWriter, r *http.Request)
	Status(w http.ResponseWriter, r *http.Request)
	Repair(w http.ResponseWriter, r *http.Request)
	Report(w http.ResponseWriter, r *http.Request)
	BackUp(w http.ResponseWriter, r *http.Request)
	ListDir(w http.ResponseWriter, r *http.Request)
	RemoveEmptyDir(w http.ResponseWriter, r *http.Request)
	RepairFileInfo(w http.ResponseWriter, r *http.Request)
	Reload(w http.ResponseWriter, r *http.Request)
	SyncFileInfo(w http.ResponseWriter, r *http.Request)
	Search(w http.ResponseWriter, r *http.Request)
	GetMd5sForWeb(w http.ResponseWriter, r *http.Request)
	ReceiveMd5s(w http.ResponseWriter, r *http.Request)
}

func NewServer() (ServiceInface, error) {
	if ser != nil {
		return ser, nil
	}
	var err error

	ser = &server{
		util:           &goutil.Common{},
		statMap:        goutil.NewCommonMap(0),
		lockMap:        goutil.NewCommonMap(0),
		rtMap:          goutil.NewCommonMap(0),
		sceneMap:       goutil.NewCommonMap(0),
		searchMap:      goutil.NewCommonMap(0),
		queueToPeers:   make(chan model.FileInfo, CONST_QUEUE_SIZE),
		queueFromPeers: make(chan model.FileInfo, CONST_QUEUE_SIZE),
		queueFileLog:   make(chan *model.FileLog, CONST_QUEUE_SIZE),
		queueUpload:    make(chan model.WrapReqResp, 100),
		sumMap:         goutil.NewCommonMap(365 * 3),
	}

	defaultTransport := &http.Transport{
		DisableKeepAlives:   true,
		Dial:                httplib.TimeoutDialer(time.Second*15, time.Second*300),
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}
	settins := httplib.BeegoHTTPSettings{
		UserAgent:        "Qainxunke-FastDFS",
		ConnectTimeout:   15 * time.Second,
		ReadWriteTimeout: 15 * time.Second,
		Gzip:             true,
		DumpBody:         true,
		Transport:        defaultTransport,
	}
	httplib.SetDefaultSetting(settins)
	ser.statMap.Put(config.CONST_STAT_FILE_COUNT_KEY, int64(0))
	ser.statMap.Put(config.CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	ser.statMap.Put(ser.util.GetToDay()+"_"+config.CONST_STAT_FILE_COUNT_KEY, int64(0))
	ser.statMap.Put(ser.util.GetToDay()+"_"+config.CONST_STAT_FILE_TOTAL_SIZE_KEY, int64(0))
	ser.curDate = ser.util.GetToDay()
	opts := &opt.Options{
		CompactionTableSize: 1024 * 1024 * 20,
		WriteBuffer:         1024 * 1024 * 20,
	}
	ser.ldb, err = leveldb.OpenFile(config.CONST_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", config.CONST_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)
	}
	ser.logDB, err = leveldb.OpenFile(config.CONST_LOG_LEVELDB_FILE_NAME, opts)
	if err != nil {
		fmt.Println(fmt.Sprintf("open db file %s fail,maybe has opening", config.CONST_LOG_LEVELDB_FILE_NAME))
		log.Error(err)
		panic(err)

	}
	return ser, err
}

func Init() {
	if ser == nil {
		_ = log.Error("error: file service server is nil")
		os.Exit(1)
		return
	}
	go func() {
		for {
			ser.CheckFileAndSendToPeer(ser.util.GetToDay(), config.CONST_Md5_ERROR_FILE_NAME, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(config.Config().RefreshInterval))
			//this.util.RemoveEmptyDir(STORE_DIR)
		}
	}()
	go ser.CleanAndBackUp()
	go ser.CheckClusterStatus()
	go ser.LoadQueueSendToPeer()
	go ser.ConsumerPostToPeer()
	go ser.ConsumerLog()
	go ser.ConsumerDownLoad()
	go ser.ConsumerUpload()
	go ser.RemoveDownloading()
	//go aws.BathToAws()
	if config.Config().EnableFsnotify {
		go ser.WatchfileschangeJob()
	}
	//go this.LoadSearchDict()
	if config.Config().EnableMigrate {
		go ser.RepairFileInfoFromFile()
	}
	if config.Config().AutoRepair {
		go func() {
			for {
				time.Sleep(time.Minute * 3)
				ser.AutoRepair(false)
				time.Sleep(time.Minute * 60)
			}
		}()
	}
	go func() { // force free memory
		for {
			time.Sleep(time.Minute * 1)
			debug.FreeOSMemory()
		}
	}()
}

func init() {
	flag.Parse()
	if *config.V {
		fmt.Printf("%s\n%s\n%s\n%s\n", config.VERSION, config.BUILD_TIME, config.GO_VERSION, config.GIT_VERSION)
		os.Exit(0)
	}
	appDir, e1 := filepath.Abs(filepath.Dir(os.Args[0]))
	curDir, e2 := filepath.Abs(".")
	if e1 == nil && e2 == nil && appDir != curDir {
		msg := fmt.Sprintf("please change directory to '%s' start fileserver\n", appDir)
		msg = msg + fmt.Sprintf("请切换到 '%s' 目录启动 fileserver ", appDir)
		_ = log.Warn(msg)
		fmt.Println(msg)
		os.Exit(1)
	}
	config.DOCKER_DIR = os.Getenv("GO_FASTDFS_DIR")
	if config.DOCKER_DIR != "" {
		if !strings.HasSuffix(config.DOCKER_DIR, "/") {
			config.DOCKER_DIR = config.DOCKER_DIR + "/"
		}
	}
	config.STORE_DIR = config.DOCKER_DIR + config.STORE_DIR_NAME
	config.CONF_DIR = config.DOCKER_DIR + config.CONF_DIR_NAME
	config.DATA_DIR = config.DOCKER_DIR + config.DATA_DIR_NAME
	config.LOG_DIR = config.DOCKER_DIR + config.LOG_DIR_NAME
	config.STATIC_DIR = config.DOCKER_DIR + config.STATIC_DIR_NAME
	config.LARGE_DIR_NAME = "haystack"
	config.LARGE_DIR = config.STORE_DIR + "/haystack"
	config.CONST_LEVELDB_FILE_NAME = config.DATA_DIR + "/qianxunke.db"
	config.CONST_LOG_LEVELDB_FILE_NAME = config.DATA_DIR + "/log.db"
	config.CONST_STAT_FILE_NAME = config.DATA_DIR + "/stat.json"
	config.CONST_CONF_FILE_NAME = config.CONF_DIR + "/cfg.json"
	config.CONST_SEARCH_FILE_NAME = config.DATA_DIR + "/search.txt"
	config.FOLDERS = []string{config.DATA_DIR, config.STORE_DIR, config.CONF_DIR, config.STATIC_DIR}
	config.LogAccessConfigStr = strings.Replace(config.LogAccessConfigStr, "{DOCKER_DIR}", config.DOCKER_DIR, -1)
	config.LogConfigStr = strings.Replace(config.LogConfigStr, "{DOCKER_DIR}", config.DOCKER_DIR, -1)
	for _, folder := range config.FOLDERS {
		_ = os.MkdirAll(folder, 0775)
	}
	_, _ = NewServer()

	peerId := fmt.Sprintf("%d", ser.util.RandInt(0, 9))
	if !ser.util.FileExists(config.CONST_CONF_FILE_NAME) {
		var ip string
		if ip = os.Getenv("GO_FASTDFS_IP"); ip == "" {
			ip = ser.util.GetPulicIP()
		}
		peer := "http://" + ip + ":8080"
		cfg := fmt.Sprintf(config.CfgJson, peerId, peer, peer)
		ser.util.WriteFile(config.CONST_CONF_FILE_NAME, cfg)
	}
	if logger, err := log.LoggerFromConfigAsBytes([]byte(config.LogConfigStr)); err != nil {
		panic(err)
	} else {
		_ = log.ReplaceLogger(logger)
	}
	if _logacc, err := log.LoggerFromConfigAsBytes([]byte(config.LogAccessConfigStr)); err == nil {
		config.Logacc = _logacc
		log.Info("succes init log access")
	} else {
		_ = log.Error(err.Error())
	}
	config.ParseConfig(config.CONST_CONF_FILE_NAME)
	if config.Config().QueueSize == 0 {
		config.Config().QueueSize = CONST_QUEUE_SIZE
	}
	if config.Config().PeerId == "" {
		config.Config().PeerId = peerId
	}
	if config.Config().SupportGroupManage {
		staticHandler = http.StripPrefix("/"+config.Config().Group+"/", http.FileServer(http.Dir(config.STORE_DIR)))
	} else {
		staticHandler = http.StripPrefix("/", http.FileServer(http.Dir(config.STORE_DIR)))
	}
	ser.initComponent(false)
}

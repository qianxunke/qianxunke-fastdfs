package database

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"log"
	"sync"
)

var (
	masterEngine *gorm.DB //主数据库
	slaveEngine  *gorm.DB //从数据库
	lock         sync.Mutex
)

//配置数据库主库
func MasterEngine() *gorm.DB {
	var master = DBCOFIG.Master

	if masterEngine != nil {
		goto EXiST
	}
	//锁住
	lock.Lock()
	defer lock.Unlock()
	if masterEngine != nil {
		goto EXiST
	}
	createEngine(master, true)
	return masterEngine

EXiST:
	var err = masterEngine.DB().Ping()
	if err != nil {
		log.Printf("@@@ 数据库 master 节点连接异常挂掉!! %s", err)
		createEngine(master, true)
	}
	return masterEngine
}

// 从库，单例
func SlaveEngine() *gorm.DB {
	var (
		slave = DBCOFIG.Slave
	)

	if slaveEngine != nil {
		goto EXIST
	}

	lock.Lock()
	defer lock.Unlock()

	if slaveEngine != nil {
		goto EXIST
	}

	createEngine(slave, false)
	return slaveEngine

EXIST:
	var err = slaveEngine.DB().Ping()
	if err != nil {
		log.Printf("@@@ 数据库 slave 节点连接异常挂掉!! %s", err)
		createEngine(slave, false)
	}
	return slaveEngine
}

func createEngine(dbIndo DBConfigInfo, isMaster bool) {
	engine, err := gorm.Open(dbIndo.Dialect, GetConnURL(&dbIndo))
	if err != nil {
		log.Printf("@@@ 初始化数据库连接失败!! %s", err)
		return
	}
	//是否启用日志记录器，将会在控制台打印sql
	engine.LogMode(dbIndo.ShowSql == "ok")
	if dbIndo.MaxIdleConns > 0 {
		engine.DB().SetMaxIdleConns(dbIndo.MaxIdleConns)
	}
	if dbIndo.MaxOpenConns > 0 {
		engine.DB().SetMaxOpenConns(dbIndo.MaxOpenConns)
	}
	// 性能优化的时候才考虑，加上本机的SQL缓存
	//cacher := xorm.NewLRUCacher(xorm.NewMemoryStore(), 1000)
	//engine.SetDefaultCacher(cacher)

	if isMaster {
		masterEngine = engine
	} else {
		slaveEngine = engine
	}
}

// 获取数据库连接的url
// true：master主库
func GetConnURL(info *DBConfigInfo) (url string) {
	//db, err := gorm.Open("mysql", "user:password@/dbname?charset=utf8&parseTime=True&loc=Local")
	url = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true",
		info.User,
		info.Password,
		info.Host,
		info.Port,
		info.Database,
		info.Charset)
	return
}

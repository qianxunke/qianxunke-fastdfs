package database

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

/**
  解析数据库配置工具
*/

var (
	DBCOFIG DB
)

type DB struct {
	Master DBConfigInfo
	Slave  DBConfigInfo
}

type DBConfigInfo struct {
	Dialect      string `yaml:"dialect"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Database     string `yaml:"database"`
	Charset      string `yaml:"charset"`
	ShowSql      string `yaml:"showSql"`
	LogLevel     string `yaml:"logLevel"`
	MaxOpenConns int    `yaml:"maxOpenConns"`
	MaxIdleConns int    `yaml:"maxIdleConns"`
}

func DBCofigParese() {
	log.Println("@@@ Init db conf")
	dbFile, err := ioutil.ReadFile("./plugins/database/db.yml")
	if err != nil {
		log.Fatalf("Error. %s", err)
	}
	if err = yaml.Unmarshal(dbFile, &DBCOFIG); err != nil {
		log.Fatalf("Error. %s", err)
	}
	log.Println("conf", DBCOFIG)
}

package client

import "github.com/yanmengfei/logical/model"

type Config struct {
	Host     string
	Port     uint16
	User     string
	Password string
	Database string
	Table    string
	Slot     string
	Callback func(records []*model.Waldata)
}

package client

import "github.com/yanmengfei/logical/model"

type Handler interface {
	Deal([]*model.Waldata)
}

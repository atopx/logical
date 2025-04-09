package client

import "github.com/atopx/logical/model"

type Handler interface {
	Deal([]*model.Waldata)
}

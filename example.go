package main

import (
	"context"
	"fmt"

	"github.com/yanmengfei/logical/client"
	"github.com/yanmengfei/logical/logger"
	"github.com/yanmengfei/logical/model"
	"go.uber.org/zap"
)

type Consumer struct{}

func (h *Consumer) Deal(records []*model.Waldata) {
	for _, record := range records {
		fmt.Println(record)
		// consumer data
	}
}

func main() {
	err := logger.Setup(zap.InfoLevel.String())
	if err != nil {
		panic(err)
	}
	c := client.New(&client.Config{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "itmeng",
		Password: "postgres_logical",
		Database: "webstore",
		Table:    "book",
		Slot:     "book_cache_slot",
	})
	c.Register(new(Consumer))
	logger.Info("start postgresql logical replication client")
	if err = c.Start(context.Background()); err != nil {
		logger.Panic(err.Error())
	}
}

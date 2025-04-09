package main

import (
	"context"
	"fmt"

	"github.com/atopx/logical/client"
	"github.com/atopx/logical/logger"
	"github.com/atopx/logical/model"
	"go.uber.org/zap"
)

type Consumer struct{}

func (h *Consumer) Deal(records []*model.Waldata) {
	for _, record := range records {
		// consumer data
		fmt.Println(record)
	}
}

func main() {
	err := logger.Setup(zap.InfoLevel.String())
	if err != nil {
		panic(err)
	}

	var handler Consumer

	c := client.New(&client.Config{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "postgres",
		Password: "postgres_logical",
		Database: "webstore",
		Table:    "book",
		Slot:     "book_cache_slot",
	}, &handler)

	logger.Info("start postgresql logical replication client")

	if err = c.Start(context.Background()); err != nil {
		logger.Panic(err.Error())
	}
}

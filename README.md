# logical

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![GitHub release](https://img.shields.io/github/release/atopx/logical.svg)](https://github.com/atopx/logical/releases)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/atopx/logical)
![Github report](https://img.shields.io/badge/go%20report-A%2B-green)
[![GitHub stars](https://img.shields.io/github/stars/atopx/logical)](https://github.com/atopx/logical/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/atopx/logical)](https://github.com/atopx/logical/network)
[![GitHub issues](https://img.shields.io/github/issues/atopx/logical)](https://github.com/atopx/logical/issues)

logical is tool for synchronizing from PostgreSQL to custom handler through replication slot

## Required
> requires Go 1.24 or greater.

Postgresql 10.0+

## Howto

### Config

1. change `postgresql.conf`

```
wal_level = 'logical';  # minimal, replica, or logical. postgres default replica, It determines how much information is written to the wal
max_replication_slots = 10; # max number of replication slots, The value should be greater than 1
```

2. change `pg_hba.conf`

```
# Add a new line below `replication`, $ is a variable
host $dbname $user $address md5  # example: `host webstore itmeng 192.168.0.1/24 md5`
```

### Install

```shell
go get github.com/atopx/logical
```

### Example
> [example](./example.go)
```go
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
```

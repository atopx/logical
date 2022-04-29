# logical

[![golang](https://img.shields.io/badge/Language-Go-green.svg?style=flat)](https://golang.org)
[![GitHub release](https://img.shields.io/github/release/yanmengfei/logical.svg)](https://github.com/yanmengfei/logical/releases)

logical is tool for synchronizing from PostgreSQL to custom handler through replication slot

## Required
> requires Go 1.13 or greater.

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
go get github.com/yanmengfei/logical
```

### Example

```go
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx"
	"github.com/yanmengfei/logical/logger"
	"github.com/yanmengfei/logical/client"
	"github.com/yanmengfei/logical/model"
)

func callback(records []*model.Waldata) {
    for i := 0; i < len(records); i++ {
        data := records[i]
        fmt.Println(*data)
        model.ReleaseWaldata(data)
    }
}

func main() {
    cfg := pgx.ConnConfig{
        Host:     "127.0.0.1",
        Port:     5432,
        User:     "itmeng",
        Password: "postgres_logical",
        Database: "webstore",
    }
    table := "book"
    slot := "book_cache_slot"
    c, err := client.New(cfg, table, slot, callback)
    if err != nil {
        logger.Panic(err.Error())
    }
    logger.Info("start postgresql logical replication client")
    if err = c.Start(context.Background()); err != nil {
    	logger.Panic(err.Error())
    }
}
```

package model

import "sync"

type Operate uint8

const (
	UNKNOW Operate = iota
	BEGIN
	INSERT
	DELETE
	UPDATE
	COMMIT
)

var operate = sync.Map{}
var unoperate = [6]string{"UNKNOW", "BEGIN", "INSERT", "DELETE", "UPDATE", "COMMIT"}

func init() {
	operate.Store("INSERT", INSERT)
	operate.Store("UPDATE", UPDATE)
	operate.Store("DELETE", DELETE)
	operate.Store("BEGIN", BEGIN)
	operate.Store("COMMIT", COMMIT)
	operate.Store("UNKNOW", UNKNOW)
}

func (o Operate) String() string {
	return unoperate[int(o)]
}

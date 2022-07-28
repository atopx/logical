package model

import "sync"

type Operate uint8

const (
	UNKNOWN Operate = iota
	BEGIN
	INSERT
	DELETE
	UPDATE
	COMMIT
)

var operate = sync.Map{}
var unoperate = [6]string{"UNKNOWN", "BEGIN", "INSERT", "DELETE", "UPDATE", "COMMIT"}

func init() {
	operate.Store("INSERT", INSERT)
	operate.Store("UPDATE", UPDATE)
	operate.Store("DELETE", DELETE)
	operate.Store("BEGIN", BEGIN)
	operate.Store("COMMIT", COMMIT)
	operate.Store("UNKNOWN", UNKNOWN)
}

func (o Operate) String() string {
	return unoperate[int(o)]
}

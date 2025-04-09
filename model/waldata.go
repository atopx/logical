package model

import (
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/atopx/logical/parser"
	"github.com/jackc/pgx"
	jsoniter "github.com/json-iterator/go"
)

var datapool = sync.Pool{New: func() any { return new(Waldata) }}

// Waldata represent parsed wal logger data
type Waldata struct {
	OperationType Operation
	Schema        string
	Table         string
	Data          map[string]any
	Timestamp     int64
	Pos           uint64
	Rule          string
}

func (w *Waldata) Decode(wal *pgx.WalMessage, tableName string) error {
	// convert []byte to string without memory allocatio
	msg := *(*string)(unsafe.Pointer(&wal.WalData))
	result := parser.New(msg)
	if err := result.Parse(); err != nil {
		return err
	}
	var schema, table string
	if result.Relation != "" {
		i := strings.IndexByte(result.Relation, '.')
		if i < 0 {
			table = result.Relation
		} else {
			schema = result.Relation[:i]
			table = strings.ReplaceAll(result.Relation[i+1:], `"`, "")
		}
		// table name based filtering
		if table != tableName {
			return nil
		}
		w.Schema = schema
		w.Table = table
	}
	w.Pos = wal.WalStart
	w.OperationType = NewOperation(result.Operation)
	w.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if len(result.Columns) == 0 {
		return nil
	}
	w.Data = make(map[string]any, len(result.Columns))
	for key, cell := range result.Columns {
		var value any
		if cell.Value != "null" {
			switch cell.Type {
			case "boolean":
				value, _ = strconv.ParseBool(cell.Value)
			case "smallint", "integer", "bigint", "smallserial", "serial", "bigserial", "interval":
				value, _ = strconv.ParseInt(cell.Value, 10, 64)
			case "float", "decimal", "numeric", "double precision", "real":
				value, _ = strconv.ParseFloat(cell.Value, 64)
			case "character varying[]":
				value = strings.Split(cell.Value[1:len(cell.Value)-1], ",")
			case "jsonb":
				value = make(map[string]any)
				_ = jsoniter.UnmarshalFromString(cell.Value, &value)
			case "timestamp without time zone":
				value, _ = time.Parse(time.DateTime, cell.Value)
			default:
				value = cell.Value
			}
		}
		w.Data[key] = value
	}
	w.Data["operate"] = w.OperationType.String()
	return nil
}

func NewWaldata() *Waldata {
	return datapool.Get().(*Waldata)
}

func PutWaldata(waldata *Waldata) {
	waldata.OperationType = Unknown
	waldata.Schema = ""
	waldata.Table = ""
	waldata.Data = nil
	waldata.Timestamp = 0
	waldata.Pos = 0
	waldata.Rule = ""
	datapool.Put(waldata)
}

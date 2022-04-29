package model

import (
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/jackc/pgx"
	jsoniter "github.com/json-iterator/go"
	"github.com/nickelser/parselogical"
)

const datelayout = "2006-01-02 15:04:05"

var datapool = sync.Pool{New: func() interface{} { return new(Waldata) }}

// Waldata represent parsed wal logger data
type Waldata struct {
	OperationType Operate
	Schema        string
	Table         string
	Data          map[string]interface{}
	Timestamp     int64
	Pos           uint64
	Rule          string
}

func (w *Waldata) Decode(wal *pgx.WalMessage, tableName string) error {
	result := parselogical.NewParseResult(*(*string)(unsafe.Pointer(&wal.WalData)))
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
	op, ok := operate.Load(result.Operation)
	if !ok {
		return nil
	}
	w.OperationType = op.(Operate)
	w.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if len(result.Columns) == 0 {
		return nil
	}
	w.Data = make(map[string]interface{}, len(result.Columns))
	for key, cell := range result.Columns {
		var value interface{}
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
				value = make(map[string]interface{})
				_ = jsoniter.UnmarshalFromString(cell.Value, &value)
			case "timestamp without time zone":
				value, _ = time.Parse(datelayout, cell.Value)
			default:
				value = cell.Value
			}
		}
		w.Data[key] = value
	}
	w.Data["operate"] = w.OperationType.String()
	return nil
}

func AcquireWaldata() *Waldata {
	return datapool.Get().(*Waldata)
}

func ReleaseWaldata(waldata *Waldata) {
	waldata.OperationType = UNKNOW
	waldata.Schema = ""
	waldata.Table = ""
	waldata.Data = nil
	waldata.Timestamp = 0
	waldata.Pos = 0
	waldata.Rule = ""
	datapool.Put(waldata)
}

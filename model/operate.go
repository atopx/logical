package model

type Operation uint8

const (
	UNKNOWN Operation = iota
	BEGIN
	INSERT
	DELETE
	UPDATE
	COMMIT
)

func NewOperation(operate string) (op Operation) {
	switch operate {
	case "BEGIN":
		op = BEGIN
	case "INSERT":
		op = INSERT
	case "DELETE":
		op = DELETE
	case "UPDATE":
		op = UPDATE
	case "COMMIT":
		op = COMMIT
	default:
		op = UNKNOWN
	}
	return op
}

func (o Operation) String() (operate string) {
	switch o {
	case UNKNOWN:
		operate = "UNKNOWN"
	case BEGIN:
		operate = "BEGIN"
	case INSERT:
		operate = "INSERT"
	case DELETE:
		operate = "DELETE"
	case UPDATE:
		operate = "UPDATE"
	case COMMIT:
		operate = "COMMIT"
	}
	return operate
}

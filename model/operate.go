package model

type Operation uint8

const (
	Unknown Operation = iota
	Begin
	Insert
	Delete
	Update
	Commit
)

func NewOperation(operate string) (op Operation) {
	switch operate {
	case "BEGIN":
		op = Begin
	case "INSERT":
		op = Insert
	case "DELETE":
		op = Delete
	case "UPDATE":
		op = Update
	case "COMMIT":
		op = Commit
	default:
		op = Unknown
	}
	return op
}

func (o Operation) String() (operate string) {
	switch o {
	case Unknown:
		operate = "UNKNOWN"
	case Begin:
		operate = "BEGIN"
	case Insert:
		operate = "INSERT"
	case Delete:
		operate = "DELETE"
	case Update:
		operate = "UPDATE"
	case Commit:
		operate = "COMMIT"
	}
	return operate
}

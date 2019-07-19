package rpcmerr


const(
	ErrorNoCreateTransportFail = iota + 1
	ErrorNoOpenTransportFail
	ErrorNoFunctionNotExist
)

type rpcError struct{
}

func (err rpcError)Error() string{
	return ""
}

func New(errno int, format string, args ...interface{})  error{
	return &rpcError{
	}
}

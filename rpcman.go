package rpcman

import (
	"github.com/afLnk/idlparser/idltypes"
	"github.com/afLnk/rpcman/echo"
	"github.com/afLnk/rpcman/rpcmerr"
	"github.com/apache/thrift/lib/go/thrift"
	"os"
)



func Call(ret idltypes.Result, req map[string]interface{}, hostAddr string)(map[string]interface{}, error){
	transport, err := thrift.NewTSocket(hostAddr)
	if err != nil{
		return nil, rpcmerr.New(rpcmerr.ErrorNoCreateTransportFail, err.Error())
	}

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTCompactProtocolFactory()

	useTransport, err := transportFactory.GetTransport(transport)
	client := echo.NewEchoClientFactory(useTransport, protocolFactory)
	if err := transport.Open(); err != nil {
		return nil, rpcmerr.New(rpcmerr.ErrorNoOpenTransportFail, err)
		os.Exit(1)
	}
	defer transport.Close()

	client.Echo()
}

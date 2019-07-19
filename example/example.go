package main

import (
	"context"
	"fmt"
	"github.com/afLnk/idlparser"
	"github.com/afLnk/idlparser/idltypes"
	"github.com/afLnk/rpcman/thriftclient"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/afLnk/rpcman/example/servers/thrift/gen-go/echo"
)

func main() {
	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTCompactProtocolFactory()

	transport, err := thrift.NewTSocket(net.JoinHostPort("127.0.0.1", "9898"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error resolving address:", err)
		os.Exit(1)
	}

	useTransport, err := transportFactory.GetTransport(transport)
	oldClient := echo.NewEchoClientFactory(useTransport, protocolFactory)

	fc := idltypes.NewFileCollection()
	idlFileContent, err := ioutil.ReadFile("./servers/thrift/idl/echo.thrift")
	if err != nil{
		panic("fail to read idl files.")
	}

	fc.AddFile("echo.thrift", idlFileContent)
	ret, err := idlparser.Parse(*fc)
	if err != nil{
		panic("invalid idl file.")
	}
	newCLient := thriftclient.NewClientFactory(useTransport, protocolFactory, ret)

	if err := transport.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to 127.0.0.1:9898", " ", err)
		os.Exit(1)
	}
	defer transport.Close()

	req := &echo.EchoReq{Msg:"You are welcome."}
	res, err := oldClient.Echo(context.Background(), req)
	if err != nil {
		log.Println("Echo failed:", err)
		return
	}

	fmt.Println("(old)response:", res.Msg)

	newResp, err := newCLient.Call("Echo", map[string]interface{}{"msg":"you are welcome"})
	if err != nil{
		panic("new error")
	}

	fmt.Println("(new)response:", newResp)
}
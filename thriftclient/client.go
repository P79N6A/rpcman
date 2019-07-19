package thriftclient

import (
	"code.byted.org/ee/lobster-apigate/common"
	baseObj "code.byted.org/ee/lobster-apigate/commonclient/common"
	"code.byted.org/ee/lobster-apigate/consts"
	"code.byted.org/ee/lobster-apigate/util"
	"code.byted.org/ee/lobster-pliers/util/log-p"
	"code.byted.org/kite/endpoint"
	"code.byted.org/kite/kitc"
	"context"
	"fmt"
	"github.com/afLnk/idlparser/idltypes"
	"github.com/afLnk/rpcman/example/servers/thrift/gen-go/echo"
	"github.com/afLnk/rpcman/rpcmerr"
	"github.com/afLnk/rpcman/thriftclient/codec"
	"github.com/apache/thrift/lib/go/thrift"
	"runtime/debug"
)

type Echo interface {
	// Parameters:
	//  - Req
	Echo(req *EchoReq) (r *EchoRes, err error)
}

type CommonClient struct {
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
	InputProtocol   thrift.TProtocol
	OutputProtocol  thrift.TProtocol
	SeqId           int32
	IDLParsedResult idltypes.Result
}

func NewCommonClientFactory(t thrift.TTransport, f thrift.TProtocolFactory, parsedResult idltypes.Result) *CommonClient {
	return &CommonClient{
		Transport: t,
		ProtocolFactory: f,
		InputProtocol:   f.GetProtocol(t),
		OutputProtocol:  f.GetProtocol(t),
		SeqId:           0,
		IDLParsedResult: parsedResult,
	}
}

func NewCommonClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol, parsedResult idltypes.Result) *CommonClient {
	return &CommonClient{
		Transport: t,
		ProtocolFactory: nil,
		InputProtocol:   iprot,
		OutputProtocol:  oprot,
		SeqId:           0,
		IDLParsedResult:parsedResult,
	}
}

// Parameters:
//  - Req
func (p *CommonClient) Call(funcName string, req interface{}) (resp interface{}, err error) {
	fnc := p.IDLParsedResult.GetFunction(funcName)
	if fnc == nil{
		return nil, rpcmerr.New(rpcmerr.ErrorNoFunctionNotExist, "function %s not exist", funcName)
	}

	if err = p.sendEcho(fnc, req); err != nil {
		return
	}
	return p.recv()
}

func (p *CommonClient) recv() (*CommonReader, error) {
	iprot := p.InputProtocol
	if iprot == nil {
		iprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.InputProtocol = iprot
	}

	r := NewCommonRespReader(p.SeqId)
	r.Read(iprot)
	_, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return
	}
	if mTypeId == thrift.EXCEPTION {
		error0 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error1 error
		error1, err = error0.Read(iprot)
		if err != nil {
			return
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return
		}
		err = error1
		return
	}
	if p.SeqId != seqId {
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "echo failed: out of sequence response")
		return
	}
	result := echo.EchoResult{}
	if err = result.Read(iprot); err != nil {
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return
	}
	value = result.GetSuccess()
	return
}

func (p *CommonClient) sendEcho(fnc idltypes.Function, req interface{}) (err error) {
	oprot := p.OutputProtocol
	if oprot == nil {
		oprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.OutputProtocol = oprot
	}
	p.SeqId++

	codec.newCommonRequest(fnc, p.SeqId).WriteArgs(oprot, req)

	return oprot.Flush()
}

// CommonReq TODO: add comment
type CommonReq struct {
	EEHead *common.Head
	Body   map[string]interface{}
	Files  []*common.File
	Base   *baseObj.Base
}

func (p *CommonReq)Dump()string{
	var fileArr []string
	for _, file := range p.Files{
		fileArr = append(fileArr, fmt.Sprintf("%s(%d)", file.Name, len(file.Content)))
	}

	return fmt.Sprintf("head=%s,body=%s,base=%s,files=%s", util.DesensitizeObj(p.EEHead), util.DesensitizeObj(p.Body), p.Base, fileArr)
}

// GetBase TODO: add comment
func (req *CommonReq) GetBase() *baseObj.Base {
	if req.Base == nil {
		req.Base = baseObj.NewBase()
	}

	return req.Base
}

// SetExtra TODO: add comment
func (req *CommonReq) SetExtra(key, val string) *baseObj.Base {
	if req.Base == nil {
		req.Base = baseObj.NewBase()
	}

	if req.Base.Extra == nil {
		req.Base.Extra = make(map[string]string)
	}

	req.Base.Extra[key] = val

	return req.Base
}

func hide(input string)string{
	inputLen := len(input)
	hideLen := inputLen*6/10 + 1
	showLen := inputLen - hideLen
	hideStart := showLen/2
	t := make([]byte, len(input))
	for i := 0; i < inputLen; i++ {
		if i >= hideStart && i <= hideLen + hideLen{
			t[i] = '*'
		}

		t[i] = input[i]
	}

	return string(t)
}

func (req *CommonReq) String() string{
	if req == nil{
		return "{}"
	}

	var reqHead string
	if req.EEHead != nil{
		if req.EEHead.IsSetId(){
			reqHead += fmt.Sprintf("Id=%s", req.EEHead.GetId())
			reqHead += fmt.Sprintf("TenantID=%d", req.EEHead.GetTenantID())
			reqHead += fmt.Sprintf("AppID=%d", req.EEHead.GetAppID())
			reqHead += fmt.Sprintf("OpenID=%s", req.EEHead.GetOpenID())
			reqHead += fmt.Sprintf("TenantKey=%s",hide(req.EEHead.GetTenantKey()))
			if req.EEHead.IsSetAuth(){
				if req.EEHead.Auth.IsSetSessionKey(){
					reqHead += fmt.Sprintf("Auth.SessionKey=%s", hide(req.EEHead.Auth.GetSessionKey()))
				}

				for k, v := range req.EEHead.Auth.Extra{
					reqHead += fmt.Sprintf("Auth.Extra.%s=%s", k, hide(v))
				}
			}
		}
	}
	
	reqBody := "{**hidden**}"

	var reqFile string
	for _, file := range req.Files{
		if reqFile != "" {
			reqFile += fmt.Sprintf(",%s", file.Name)
		}else{
			reqFile += file.GetName()
		}
	}

	return fmt.Sprintf("Head:\n%s\nBody:\n%s\nFile:\n%s", reqHead, reqBody, reqFile)
}

// CommonResp TODO: add comment
type CommonResp struct {
	Body            map[string]interface{}
	BaseResp        *baseObj.BaseResp
	forceHttpHeader *common.ForceHttpHeader
	httpHeader      map[string]string
}

func (p *CommonResp)Dump()string{
	if p == nil{
		return "<nil>"
	}

	return fmt.Sprintf("body=%s, baseResp=%s, forceHttpHeader=%s, httpHeader=%s", util.DesensitizeObj(p.Body),
			p.BaseResp, p.forceHttpHeader, util.DesensitizeObj(p.httpHeader))
}


// GetBaseResp TODO: add comment
func (p *CommonResp) GetBaseResp() *baseObj.BaseResp {
	if !p.IsSetBaseResp() {
		return GetCommonRespBaseRespDefault
	}
	return p.BaseResp
}

// GetCommonRespBaseRespDefault TODO: add comment
var GetCommonRespBaseRespDefault *baseObj.BaseResp

// IsSetBaseResp TODO: add comment
func (p *CommonResp) IsSetBaseResp() bool {
	return p.BaseResp != nil
}

// KiteCommonReq TODO: add comment
type KiteCommonReq struct {
	*CommonReq
}

// KiteCommonResp TODO: add comment
type KiteCommonResp struct {
	*CommonResp
	addr string
}

// GetBaseResp TODO: add comment
func (kp *KiteCommonResp) GetBaseResp() endpoint.KiteBaseResp {
	if kp.CommonResp != nil {
		if ret := kp.CommonResp.GetBaseResp(); ret != nil {
			return ret
		}
	}
	return nil
}

// RemoteAddr TODO: add comment
func (kp *KiteCommonResp) RemoteAddr() string {
	return kp.addr
}

// RealResponse TODO: add comment
func (kp *KiteCommonResp) RealResponse() interface{} {
	return kp.CommonResp
}

// RealRequest TODO: add comment
func (kr *KiteCommonReq) RealRequest() interface{} {
	return kr.CommonReq
}

// SetBase TODO: add comment
func (kr *KiteCommonReq) SetBase(kb endpoint.KiteBase) error {
	kr.CommonReq.Base = &baseObj.Base{
		LogID:  kb.GetLogID(),
		Caller: kb.GetCaller(),
		Addr:   kb.GetAddr(),
		Client: kb.GetClient(),
		Extra: map[string]string{
			"cluster": kb.GetCluster(),
			"env":     kb.GetEnv(),
		},
	}
	return nil
}

// KitccommonClient TODO: add comment
type KitcCommonClient struct {
	PSM string
}

// Init TODO: add comment
func Init(PSM string) {
	util.Register(&KitcCommonClient{PSM}, PSM)
}

// New TODO: add comment
func (c *KitcCommonClient) New(kc *kitc.KitcClient) kitc.Caller {
	t := kitc.NewBufferedTransport(kc)
	f := thrift.NewTBinaryProtocolFactoryDefault()
	client := &CommonClient{
		PSM:             c.PSM,
		Transport:       t,
		ProtocolFactory: f,
		InputProtocol:   f.GetProtocol(t),
		OutputProtocol:  f.GetProtocol(t),
	}
	return &KitcCommonCaller{client}
}

// KitcCommonCaller TODO: add comment
type KitcCommonCaller struct {
	client *CommonClient
}

func mkCommonCall(client *CommonClient, cmd string) endpoint.EndPoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		transport := client.Transport.(kitc.Transport)
		err := transport.OpenWithContext(ctx)
		if err != nil {
			return nil, err
		}
		defer transport.Close()
		addr := transport.RemoteAddr()

		commReq, ok := request.(endpoint.KitcCallRequest).RealRequest().(*CommonReq)
		if !ok {
			return &KiteCommonResp{nil, addr}, err
		}

		lobContextVal := ctx.Value(consts.HeaderLobContextKey)
		if lobContextVal != nil {
			commReq.SetExtra(consts.HeaderLobContextKey, fmt.Sprint(lobContextVal))
		}

		log_p.Debugf(ctx, "mkCommonCall client Call:%s\nExtra:%v", cmd, commReq.Base.GetExtra())

		resp, err := client.Call(ctx, cmd, commReq)
		return &KiteCommonResp{resp, addr}, err
	}
}

// Call TODO: add comment
func (c *KitcCommonCaller) Call(name string, request interface{}) (endpoint.EndPoint, endpoint.KitcCallRequest) {
	return mkCommonCall(c.client, name), &KiteCommonReq{request.(*CommonReq)}
}

func (cl *CommonClient) recvCmd(ctx context.Context, cmd string) (response *CommonResp, err error) {
	defer func() {
		if err := recover(); err != nil {
			log_p.Errorf(ctx, "System panic, recvcmd error: %v, stack: %s", err, debug.Stack())
		}
	}()
	cr := CommonReader{PSM: cl.PSM, CMD: cmd, ctx: ctx}
	iprot := cl.InputProtocol
	if iprot == nil {
		iprot = cl.ProtocolFactory.GetProtocol(cl.Transport)
		cl.InputProtocol = iprot
	}
	_, mTypeID, seqID, err1 := iprot.ReadMessageBegin()
	if err1 != nil {
		log_p.Warnf(ctx, "recvCmd fail to read message begin:%s", err1.Error())
		err = fmt.Errorf("recvCmd fail to read message begin:%s", err1.Error())
		return
	}

	if mTypeID == thrift.EXCEPTION {
		log_p.Warnf(ctx, "Unknown Exception")
		error6 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		error7, err71 := error6.Read(iprot)
		if err71 != nil {
			log_p.Warnf(ctx, "recvCmd fail to read message begin:%s", err71.Error())
			err = fmt.Errorf("recvCmd fail to read message begin:%s", err71.Error())
			return
		}

		if err81 := iprot.ReadMessageEnd(); err81 != nil {
			log_p.Warnf(ctx, "recvCmd fail to read message begin:%s", err81.Error())
			err = fmt.Errorf("recvCmd fail to read message begin:%s", err81.Error())
			return
		}

		log_p.Warnf(ctx,"err(from error7):%s", error7.Error())
		err = fmt.Errorf("err(from error7):%s", error7.Error())
		return
	}
	if cl.seq != seqID {
		log_p.Warnf(ctx, "recvCmd bad seq， cl.seq=%d, actual.seq=%d", cl.seq, seqID)
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "Auth failed: out of sequence response")
		return
	}

	if err = cr.Read(iprot); err != nil {
		log_p.Warnf(ctx, "recvCmd cr.Read err:%s", err.Error())
		err = fmt.Errorf("recvCmd cr.Read err:%s", err.Error())
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		log_p.Warnf(ctx,"recvCmd iprot.ReadMessageEnd():%s", err.Error())
		err = fmt.Errorf("recvCmd iprot.ReadMessageEnd():%s", err.Error())
		return
	}

	response = cr.Success
	return
}

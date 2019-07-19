package thriftclient

import (
	"fmt"
	"github.com/afLnk/idlparser/idltypes"
	"github.com/apache/thrift/lib/go/thrift"
)
/*
// CommonReader TODO: add comment
type CommonReader struct {
	PSM     string
	CMD     string
	Success *CommonResp

	ctx context.Context
}

// MaxFloatCanStoreInt TODO: add comment
var MaxFloatCanStoreInt = int64(1) << 51 - 1

// Read TODO: add comment
func (crResult *CommonReader) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		log_p.Errorf(crResult.ctx, "%s result read struct begin error: %s", crResult.CMD, err.Error())
		return fmt.Errorf("%s result read struct begin error: %s", crResult.CMD, err.Error())
	}
	for {
		_, fieldTypeID, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			log_p.Errorf(crResult.ctx, "%s field %d[%s] read field begin error: %s", crResult.CMD, fieldID, fieldTypeID.String(), err.Error())
			return fmt.Errorf("%s field %d[%s] read field begin error: %s", crResult.CMD, fieldID, fieldTypeID.String(), err.Error())
		}

		if fieldTypeID == thrift.STOP {
			break
		}

		mod := idlloader.GetMicroService(crResult.ctx, crResult.PSM)
		if nil == mod {
			log_p.Errorf(crResult.ctx, "GetMicroModule<%s> fail: %+v", crResult.PSM, err)
			return fmt.Errorf("GetMicroModule<%s> fail: %+v", crResult.PSM, err)
		}

		fnc := mod.GetFunction(crResult.CMD)
		if nil == fnc {
			log_p.Errorf(crResult.ctx, "GetFunction<%s> fail: %+v", crResult.CMD, err)
			return fmt.Errorf("GetFunction<%s> fail: %+v", crResult.CMD, err)
		}

		if nil == fnc.RspType {
			log_p.Errorf(crResult.ctx, "Function<%s> is empty", fnc.GetName())
			return fmt.Errorf("function<%s> is empty", fnc.GetName())
		}

		respStruct, ok := fnc.RspType.(*types.IDLStruct)
		if !ok {
			log_p.Errorf(crResult.ctx, "Function<%s> response<%s> is not struct", fnc.GetName(), fnc.RspType.GetName())
			return fmt.Errorf("function<%s> response<%s> is not struct", fnc.GetName(), fnc.RspType.GetName())
		}

		switch fieldID {
		case 0:
			if err := crResult.ReadField0(iprot, respStruct); err != nil {
				log_p.Errorf(crResult.ctx, "crReult.ReadField0 fail:%s", err.Error())
				return fmt.Errorf("crReult.ReadField0 fail:%s", err.Error())
			}
		default:
			if err := iprot.Skip(fieldTypeID); err != nil {
				log_p.Errorf(crResult.ctx, "%s:%s Skip %s err:%s", crResult.PSM, crResult.CMD, fieldTypeID.String(), err.Error())
				return fmt.Errorf("%s:%s Skip %s err:%s", crResult.PSM, crResult.CMD, fieldTypeID.String(), err.Error())
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			log_p.Errorf(crResult.ctx, "%s:%s ReadFieldEnd err:%s", crResult.PSM, crResult.CMD, err.Error())
			return fmt.Errorf("%s:%s ReadFieldEnd err:%s", crResult.PSM, crResult.CMD, err.Error())
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		log_p.Errorf(crResult.ctx, "%s:%s ReadStructEnd err:%s", crResult.PSM, crResult.CMD, err.Error())
		return fmt.Errorf("%s:%s ReadStructEnd err:%s", crResult.PSM, crResult.CMD, err.Error())
	}
	return nil
}

// ReadField0 TODO: add comment
func (crResult *CommonReader) ReadField0(iprot thrift.TProtocol, idlType *types.IDLStruct) error {
	crr := CommonRespReader{PSM: crResult.PSM, respType: idlType, ctx: crResult.ctx}
	if err := crr.Read(iprot); err != nil {
		log_p.Errorf(crResult.ctx, "%s:%s error ReadField0: %s", crResult.PSM, crResult.CMD, err.Error())
		return fmt.Errorf("%s:%s error ReadField0: %s", crResult.PSM, crResult.CMD, err.Error())
	}
	crResult.Success = crr.Response
	return nil
}
*/
// CommonRespReader TODO: add comment
type CommonRespReader struct {
	PSM      string
	Response *CommonResp

	name string

	reqSeqID int32
	function idltypes.Function
}

func NewCommonRespReader(reqSeqID int32, fnc idltypes.Function)*CommonRespReader{
	return &CommonRespReader{
		reqSeqID: reqSeqID,
		function: fnc,
	}
}

// Read TODO: add comment
func (cr *CommonRespReader) Read(iprot thrift.TProtocol) error {
	name, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return err
	}

	if cr.reqSeqID != seqId {
		return thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "echo failed: out of sequence response")
	}

	switch mTypeId{
	case thrift.EXCEPTION:
		error0 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error1 error
		error1, err = error0.Read(iprot)
		if err != nil {
			return err
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return err
		}

		return error1
	default:
		fmt.Println(mTypeId)
		//if err = cr.read2(iprot); err != nil {
		//	return err
		//}

		if err = iprot.ReadMessageEnd(); err != nil {
			return err
		}
		cr.name = name
	}

	return nil
}
/*
func (cr *CommonRespReader)read2(iprot thrift.TProtocol) error{

	if _, err := iprot.ReadStructBegin(); err != nil {
		return fmt.Errorf("%s struct read error: %s", cr.PSM, err)
	}

	cr.Response = &CommonResp{Body: make(map[string]interface{})}
	for {
		fieldName, thriftFieldType, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			return fmt.Errorf("%s field %d read error: %s", cr.respType.GetName(), fieldID, err.Error())
		}
		if thriftFieldType == thrift.STOP {
			break
		}

		fieldMember := GetStructFieldMember(cr.ctx, cr.respType, fieldName, thriftFieldType, fieldID)
		idlFieldType := fieldMember.GetFieldType()

		switch {
		case int16(255) == fieldID:
			cr.Response.BaseResp = &baseObj.BaseResp{}
			if err = cr.Response.BaseResp.Read(iprot); err != nil {
				log_p.Errorf(cr.ctx, "%s:%s MapRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
				return fmt.Errorf("%s:%s BaseRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
			}
		case isForceHttpHeader(fieldMember, thriftFieldType):
			str := NewStructTypeReader(cr.ctx, cr.ginCtx)
			str.idlType, _ = idlFieldType.(*types.IDLStruct)
			if err = str.Read(iprot); err != nil {
				log_p.Errorf(cr.ctx,"str.Read(iprot) fail:%s", err.Error())
				return fmt.Errorf("str.Read(iprot) fail:%s", err.Error())
			}

			if len(str.value) > 0{
				err = cr.Response.SetForceHttpHeader(str.value)
				if err != nil{
					return fmt.Errorf("set_force_http_header_fail:%s", err.Error())
				}
			}
		case isHttpHeader(fieldMember, thriftFieldType):
			str := NewStructTypeReader(cr.ctx, cr.ginCtx)
			str.idlType, _ = idlFieldType.(*types.IDLStruct)
			if err = str.Read(iprot); err != nil {
				log_p.Errorf(cr.ctx,"str.Read(iprot) fail:%s", err.Error())
				return fmt.Errorf("str.Read(iprot) fail:%s", err.Error())
			}

			if len(str.value) > 0{
				cr.Response.SetHttpHeader(str.value)
			}
		case isMember(cr.respType, int32(fieldID)):
			if thriftFieldType == thrift.STRUCT {
				str := NewStructTypeReader(cr.ctx, cr.ginCtx)
				if idlFieldType != nil{
					var ok bool
					str.idlType, ok = idlFieldType.(*types.IDLStruct)
					if !ok{
						log_p.Errorf(cr.ctx,"[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v", thriftFieldType.String(), fieldMember)
						return fmt.Errorf("[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
					}
				}

				if err = str.Read(iprot); err != nil {
					log_p.Errorf(cr.ctx,"str.Read(iprot) fail:%s", err.Error())
					return fmt.Errorf("str.Read(iprot) fail:%s", err.Error())
				}

				if len(str.value) > 0{
					cr.Response.Body[fieldMember.FieldName] = str.value
				}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT{
					cr.Response.Body[fieldMember.FieldName] = make(map[string]bool)
				}
			} else if thriftFieldType == thrift.MAP {
				mr := &MapReader{ctx:cr.ctx}
				if idlFieldType != nil{
					idlMap, ok := idlFieldType.(*types.IDLMap)
					if !ok{
						log_p.Errorf(cr.ctx,"[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
						return fmt.Errorf("[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
					}

					if idlMap != nil {
						mr.KIdl = idlMap.GetKeyType()
						mr.VIdl = idlMap.GetValueType()
					}
				}

				if err = mr.Read(iprot); err != nil {
					log_p.Errorf(cr.ctx, "%s:%s MapRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
					return fmt.Errorf("%s:%s MapRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
				}

				if len(mr.Value) > 0{
					cr.Response.Body[fieldMember.FieldName] = mr.Value
				}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT{
					cr.Response.Body[fieldMember.FieldName] = "{}"
				}
			} else if thriftFieldType == thrift.SET {
				sr := &SetReader{ctx:cr.ctx}
				if idlFieldType != nil{
					idlSet, ok := idlFieldType.(*types.IDLSet)
					if !ok{
						log_p.Errorf(cr.ctx,"[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
						return fmt.Errorf("[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
					}

					if nil != idlSet {
						sr.EIdl = idlSet.GetKeyType()
					}
				}

				if err = sr.Read(iprot); err != nil {
					log_p.Errorf(cr.ctx, "%s:%s SETRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
					return fmt.Errorf("%s:%s SETRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
				}

				if len(sr.Value) > 0{
					cr.Response.Body[fieldMember.FieldName] = sr.Value
				}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT {
					cr.Response.Body[fieldMember.FieldName] = make([]string, 0)
				}
			} else if thriftFieldType == thrift.LIST {
				lr := NewListReader(cr.ctx, cr.ginCtx)
				if nil != idlFieldType {
					listType, ok := idlFieldType.(*types.IDLList)
					if !ok {
						log_p.Errorf(cr.ctx,"[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
						return fmt.Errorf("[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v",
							thriftFieldType.String(), fieldMember)
					}

					if listType != nil{
						lr.EIdl = listType.GetValueType()
					}
				}
				if err = lr.Read(iprot); err != nil {
					log_p.Errorf(cr.ctx, "%s:%s ListRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
					return fmt.Errorf("%s:%s ListRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
				}

				if len(lr.Value) > 0{
					cr.Response.Body[fieldMember.FieldName] = lr.Value
				}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT {
					cr.Response.Body[fieldMember.FieldName] = make([]string, 0)
				}
			} else {
				value, err := BaseRead(iprot, thriftFieldType, fieldMember)
				if err != nil {
					log_p.Errorf(cr.ctx, "%s:%s BaseRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
					return fmt.Errorf("%s:%s BaseRead err:%s", cr.PSM, cr.respType.GetName(),  err.Error())
				}

				// 基础类型不需要判断，thrift自己已经做了处理。
				if value != nil {
					cr.Response.Body[fieldMember.FieldName] = value
				}
			}
		default:
			if err := iprot.Skip(thriftFieldType); err != nil {
				log_p.Errorf(cr.ctx, "%s:%s Skip %s err:%s", cr.PSM, cr.respType.GetName(), thriftFieldType.String(), err.Error())
				return fmt.Errorf("%s:%s Skip %s err:%s", cr.PSM, cr.respType.GetName(), thriftFieldType.String(), err.Error())
			}

		}
		if err := iprot.ReadFieldEnd(); err != nil {
			log_p.Errorf(cr.ctx, "%s ReadFieldEnd error: %s", cr.respType.GetName(), err)
			return fmt.Errorf("%s ReadFieldEnd error: %s", cr.respType.GetName(), err)
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		log_p.Errorf(cr.ctx, "%s ReadStructEnd error: %s", cr.respType.GetName(), err)
		return fmt.Errorf("%s read struct end error: %s", cr.respType.GetName(), err)
	}
}

func i642IdlType(idlField *types.IDLField, val int64)interface{}{
	if idlField != nil && idlField.GetForceType() == types.BaseTypeString{
		return strconv.FormatInt(val,10)
	}

	if val > MaxFloatCanStoreInt{
		return strconv.FormatInt(val,10)
	}

	return val
}

// BaseRead TODO: add comment
func BaseRead(iprot thrift.TProtocol, tType thrift.TType, idlField *types.IDLField) (result interface{}, err error) {
	switch tType {
	case thrift.BYTE:
		return iprot.ReadByte()
	case thrift.I16:
		return iprot.ReadI16()
	case thrift.I32:
		return iprot.ReadI32()
	case thrift.I64:
		 v, err := iprot.ReadI64()
		 if err != nil{
		 	return v, err
		 }
		 return i642IdlType(idlField, v), nil
	case thrift.STRING:
		if idlField == nil { // 一般情况如果是String就按照String来解
			return iprot.ReadString()
		}
		if realTyle, ok := idlField.GetFieldType().(*types.IDLBaseType); ok {
			if realTyle.ThriftTypeID() == thrift.BINARY {
				return iprot.ReadBinary()
			}
		}
		return iprot.ReadString()
	case thrift.UTF8, thrift.UTF16:
		return iprot.ReadString()
	case thrift.BOOL:
		return iprot.ReadBool()
	case thrift.DOUBLE:
		return iprot.ReadDouble()
	case thrift.BINARY:
		return iprot.ReadBinary()
	default:
		return nil, fmt.Errorf("unsupport type %s", tType.String())
	}
}

// StructTypeReader TODO: add comment
type StructTypeReader struct {
	idlType *types.IDLStruct
	value   map[string]interface{}
	ctx     context.Context
	ginCtx *gin.Context
}

func NewStructTypeReader(ctx context.Context, ginCtx *gin.Context)*StructTypeReader{
	return &StructTypeReader{
		ctx: ctx,
		ginCtx: ginCtx,
	}
}

// Read TODO: add comment
func (str *StructTypeReader) Read(iprot thrift.TProtocol) error {
	logEntry := util.GetLogger2(str.ginCtx)

	if nil == str.idlType {
		appMethod := ctxrw.GetAppMethod(str.ginCtx)
		metrics.EmitCounterV2(metrics.Metric_CommCli_FailReaderStructIdl, map[string]string{"method": appMethod.MethodCommand})
	}

	if _, err := iprot.ReadStructBegin(); err != nil {
		logEntry.Errorf("%s read begin error: %s", str.idlType.GetName(), err)
		return fmt.Errorf("%s read begin error: %s", str.idlType.GetName(), err)
	}

	str.value = make(map[string]interface{})
	for {
		idlFieldName, fieldTypeID, fieldID, err := iprot.ReadFieldBegin()
		if err != nil {
			logEntry.Errorf("type %d field %d:%s read error: %s", fieldID, fieldID, fieldTypeID, err)
			return fmt.Errorf("type %d field %d:%s read error: %s", fieldID, fieldID, fieldTypeID, err)
		}

		if fieldTypeID == thrift.STOP {
			break
		}

		fieldMember := GetStructFieldMember(str.ctx, str.idlType, idlFieldName, fieldTypeID, fieldID)
		idlFieldType := fieldMember.GetFieldType()

		if fieldTypeID == thrift.STRUCT {
			valueStr := NewStructTypeReader(str.ctx, str.ginCtx)
			if idlFieldType != nil{
				var ok bool
				valueStr.idlType, ok = idlFieldType.(*types.IDLStruct)
				if !ok{
					logEntry.Errorf("[CommonRespReader]TypeNotMatch, thrift resp:%s, idl define:%v", fieldTypeID.String(), fieldMember)
					return fmt.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v",
						fieldTypeID.String(), fieldMember)
				}
			}

			if err = valueStr.Read(iprot); err != nil {
				logEntry.Errorf("[StructTypeReader]ValueStrRead fail:%s", err.Error())
				return fmt.Errorf("[StructTypeReader]ValueStrRead fail:%s", err.Error())
			}

			if len(valueStr.value) > 0{
				str.value[fieldMember.FieldName] = valueStr.value
			}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT{
				str.value[fieldMember.FieldName] = make(map[string]bool)
			}
		} else if fieldTypeID == thrift.MAP {
			mr := NewMapReader(str.ctx, str.ginCtx)
			if idlFieldType != nil{
				idlMap, ok := idlFieldType.(*types.IDLMap)
				if !ok{
					logEntry.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v", fieldTypeID.String(), fieldMember)
					return fmt.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v", fieldTypeID.String(), fieldMember)
				}

				if idlMap != nil {
					mr.KIdl = idlMap.GetKeyType()
					mr.VIdl = idlMap.GetValueType()
				}
			}

			if err = mr.Read(iprot); err != nil {
				logEntry.Errorf("StructTypeReader sr.Read fail, err: %s", err.Error())
				return fmt.Errorf("StructTypeReader sr.Read fail, err: %s", err.Error())
			}

			if len(mr.Value) > 0{
				str.value[fieldMember.FieldName] = mr.Value
			}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT{
				str.value[fieldMember.FieldName] = "{}"
			}
		} else if fieldTypeID == thrift.SET {
			sr := NewSetReader(str.ctx, str.ginCtx)
			if idlFieldType != nil{
				idlSet, ok := idlFieldType.(*types.IDLSet)
				if !ok{
					logEntry.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v", fieldTypeID.String(), fieldMember)
					return fmt.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v", fieldTypeID.String(), fieldMember)
				}

				if nil != idlSet {
					sr.EIdl = idlSet.GetKeyType()
				}
			}

			if err = sr.Read(iprot); err != nil {
				logEntry.Errorf("StructTypeReader sr.Read fail, err: %s", err.Error())
				return fmt.Errorf("StructTypeReader sr.Read fail, err: %s", err.Error())
			}

			if len(sr.Value) > 0{
				str.value[fieldMember.FieldName] = sr.Value
			}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT {
				str.value[fieldMember.FieldName] = make([]string, 0)
			}
		} else if fieldTypeID == thrift.LIST {
			lr := NewListReader(str.ctx, str.ginCtx)
			if nil != idlFieldType {
				listType, ok := idlFieldType.(*types.IDLList)
				if !ok {
					logEntry.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v",
						fieldTypeID.String(), fieldMember)
					return fmt.Errorf("[StructTypeReader]TypeNotMatch, thrift resp:%s, idl define:%v",
						fieldTypeID.String(), fieldMember)
				}

				if listType != nil{
					lr.EIdl = listType.GetValueType()
				}
			}
			if err = lr.Read(iprot); err != nil {
				logEntry.Errorf("%s Read List end error: %s", str.idlType.GetName(), err.Error())
				return fmt.Errorf("%s Read List end error: %s", str.idlType.GetName(), err.Error())
			}

			if len(lr.Value) > 0{
				str.value[fieldMember.FieldName] = lr.Value
			}else if fieldMember.Required == types.T_REQUIRED || fieldMember.Required == types.T_OPT_IN_REQ_OUT {
				str.value[fieldMember.FieldName] = make([]string, 0)
			}
		} else {
			value, err := BaseRead(iprot, fieldTypeID, fieldMember)
			if err != nil {
				logEntry.Errorf("%s BaseRead end error: %s", str.idlType.GetName(), err.Error())
				return fmt.Errorf("%s BaseRead end error: %s", str.idlType.GetName(), err.Error())
			}

			// 基础类型不需要判断，thrift自己已经做了处理。
			if value != nil {
				str.value[fieldMember.FieldName] = value
			}
		}
	}

	if err := iprot.ReadStructEnd(); err != nil {
		logEntry.Errorf("%s read struct end error: %s", str.idlType.GetName(), err.Error())
		return fmt.Errorf("%s read struct end error: %s", str.idlType.GetName(), err.Error())
	}

	return nil
}

// ValueReader TODO: add comment
type ValueReader struct {
	Idl       types.IDLTypeI
	Value     interface{}
	FieldType thrift.TType
	ctx context.Context
	ginCtx *gin.Context
}

func NewValueReader(ctx context.Context, ginCtx *gin.Context, idlType types.IDLTypeI, elemType thrift.TType)*ValueReader{
	return &ValueReader{
		ctx: ctx,
		ginCtx: ginCtx,
		Idl: idlType,
		FieldType:elemType,
	}
}

// String TODO: add comment
func (vr *ValueReader) String()string{
	if vr != nil && vr.Idl != nil{
		return vr.Idl.GetName()
	}
	return "NULL"
}

// Read TODO: add comment
func (vr *ValueReader) Read(ip thrift.TProtocol) (err error) {
	log_p.Debugf(vr.ctx, "[ValueReader][Read]type:%s, idl:%+v", vr.FieldType.String(), vr.Idl)
	if nil == vr.Idl {
		appMethod := ctxrw.GetAppMethod(vr.ginCtx)
		metrics.EmitCounterV2(metrics.Metric_CommCli_FailReaderValueIdl, map[string]string{"method": appMethod.MethodCommand})
	}

	switch {
	case vr.FieldType == thrift.STRUCT:
		stReader := NewStructTypeReader(vr.ctx, vr.ginCtx)
		if nil != vr.Idl {
			tmp, ok := vr.Idl.(*types.IDLStruct)
			if ok {
				stReader.idlType = tmp
			}
		}
		if err = stReader.Read(ip); err != nil {
			log_p.Warnf(vr.ctx, "read struct error: %s", err.Error())
			return fmt.Errorf("read struct error: %s", err.Error())
		}

		if len(stReader.value) > 0{
			vr.Value = stReader.value
		}
	case vr.FieldType == thrift.MAP:
		mr := &MapReader{ctx: vr.ctx}
		if nil != vr.Idl {
			tmp, ok := vr.Idl.(*types.IDLMap)
			if ok {
				mr.KIdl = tmp.GetKeyType()
				mr.VIdl = tmp.GetValueType()
			}
		}

		if err = mr.Read(ip); err != nil {
			log_p.Warnf(vr.ctx, "read map error: %s", err.Error())
			return fmt.Errorf("read map error: %s", err.Error())
		}

		if len(mr.Value) > 0{
			vr.Value = mr.Value
		}
	case vr.FieldType == thrift.SET:
		sr := &SetReader{ctx:vr.ctx}
		if nil != vr.Idl {
			tmp, ok := vr.Idl.(*types.IDLSet)
			if ok {
				sr.EIdl = tmp.GetKeyType()
			}
		}
		if err = sr.Read(ip); err != nil {
			log_p.Warnf(vr.ctx, "read set error: %s", err.Error())
			return fmt.Errorf("read set error: %s", err.Error())
		}

		if len(sr.Value) > 0{
			vr.Value = sr.Value
		}
	case vr.FieldType == thrift.LIST:
		lr := NewListReader(vr.ctx, vr.ginCtx)
		if nil != vr.Idl {
			tmp, ok := vr.Idl.(*types.IDLList)
			if ok {
				lr.EIdl = tmp.GetValueType()
			}
		}
		if err = lr.Read(ip); err != nil {
			log_p.Warnf(vr.ctx, "read list error: %s", err.Error())
			return fmt.Errorf("read list error: %s", err.Error())
		}

		if len(lr.Value) > 0 {
			vr.Value = lr.Value
		}
	default:
		vr.Value, err = BaseRead(ip, vr.FieldType, nil)
		if err != nil {
			log_p.Warnf(vr.ctx, "read basetype error: %s", err.Error())
			return fmt.Errorf("read basetype error: %s", err.Error())
		}
	}
	return nil
}

// MapReader TODO: add comment
type MapReader struct {
	KIdl  types.IDLTypeI
	VIdl  types.IDLTypeI
	Value map[string]interface{}
	ctx   context.Context
	ginCtx *gin.Context
}

func NewMapReader(ctx context.Context, ginCtx *gin.Context) *MapReader{
	return &MapReader{
		ctx:ctx,
		ginCtx: ginCtx,
	}
}

// Read TODO: add comment
func (mr *MapReader) Read(ip thrift.TProtocol) error {
	thriftKeyType, thriftValType, size, err := ip.ReadMapBegin()
	if err != nil {
		log_p.Errorf(mr.ctx, "MapReader ReadMapBegin err: %s", err.Error())
		return fmt.Errorf("error reading map begin: %s", err.Error())
	}
	mr.Value = make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		var _key string
		kvr := NewValueReader(mr.ctx, mr.ginCtx, mr.KIdl, thriftKeyType)
		if err := kvr.Read(ip); err != nil {
			log_p.Errorf(mr.ctx, "MapReader map_key_read(keyType=%s,idlType=%s) fail: %s", thriftKeyType.String(), kvr.String(), err.Error())
			return fmt.Errorf("MapReader map_key_read(keyType=%s,idlType=%s) fail: %s", thriftKeyType.String(), kvr.String(), err.Error())
		}
		_key = jsonUtil.AsStringValue(kvr.Value)

		var _val interface{}
		vvr := NewValueReader(mr.ctx, mr.ginCtx, mr.VIdl, thriftValType)
		if err := vvr.Read(ip); err != nil {
			log_p.Errorf(mr.ctx, "MapReader Read(valType=%s,idlType=%s) fail: %s", thriftValType.String(), vvr.String(), err.Error())
			return fmt.Errorf("MapReader Read(valType=%s,idlType=%s) fail: %s", thriftValType.String(), vvr.String(), err.Error())
		}

		_val = vvr.Value

		if _key != ""{
			if _val != nil {
				mr.Value[_key] = _val
			}else {
				if vvr.FieldType == thrift.STRUCT || vvr.FieldType == thrift.MAP {
					mr.Value[_key] = make(map[string]bool)
				}else if vvr.FieldType == thrift.LIST || vvr.FieldType == thrift.SET{
					mr.Value[_key] = make([]string, 0)
				}
			}
		}
	}
	if err := ip.ReadMapEnd(); err != nil {
		log_p.Errorf(mr.ctx, "ReadSetEnd error reading set end: %s", err.Error())
		return fmt.Errorf("ReadMapEnd error reading map end: %s", err.Error())
	}

	return nil
}

// SetReader TODO: add comment
type SetReader struct {
	EIdl  types.IDLTypeI
	Value []interface{}
	ctx   context.Context
	ginCtx *gin.Context
}

func NewSetReader(ctx context.Context, ginCtx *gin.Context) *SetReader{
	return &SetReader{
		ctx:ctx,
		ginCtx: ginCtx,
	}
}

// Read TODO: add comment
func (sr *SetReader) Read(ip thrift.TProtocol) error {
	elemType, size, err := ip.ReadSetBegin()
	if err != nil {
		log_p.Errorf(sr.ctx, "SetReader ReadSetBegin err: %s", err.Error())
		return fmt.Errorf("SetReader ReadSetBegin err: %s", err.Error())
	}
	for i := 0; i < size; i++ {
		vr := NewValueReader(sr.ctx, sr.ginCtx, sr.EIdl, elemType)
		if err := vr.Read(ip); err != nil {
			log_p.Errorf(sr.ctx, "SetReader read(%v) fail: %s", vr, err.Error())
			return fmt.Errorf("SetReader read(%v) fail: %s", vr, err.Error())
		}
		if vr.Value != nil {
			sr.Value = append(sr.Value, vr.Value)
		}
	}
	if err := ip.ReadSetEnd(); err != nil {
		log_p.Errorf(sr.ctx, "ReadSetEnd error reading set end: %s", err.Error())
		return fmt.Errorf("ReadSetEnd error reading set end: %s", err.Error())
	}
	return nil
}

// ListReader TODO: add comment
type ListReader struct {
	EIdl  types.IDLTypeI
	Value []interface{}
	ctx   context.Context
	ginCtx *gin.Context
}

func NewListReader(ctx context.Context, ginCtx *gin.Context)*ListReader{
	return &ListReader{
		ctx: ctx,
		ginCtx: ginCtx,
	}
}

// Read TODO: add comment
func (lr *ListReader) Read(ip thrift.TProtocol) error {
	if nil == lr.EIdl {
		appMethod := ctxrw.GetAppMethod(lr.ginCtx)
		metrics.EmitCounterV2(metrics.Metric_CommCli_FailReaderListIdl, map[string]string{"method": appMethod.MethodCommand})
	}
	elemTType, size, err := ip.ReadListBegin()
	if err != nil {
		log_p.Errorf(lr.ctx,"ListReader error ReadListBegin list begin: %s", err.Error())
		return fmt.Errorf("ListReader error ReadListBegin list begin: %s", err.Error())
	}
	var tSlice []interface{}
	for i := 0; i < size; i++ {
		var _elem interface{}
		vr := NewValueReader(lr.ctx, lr.ginCtx, lr.EIdl, elemTType)
		if err := vr.Read(ip); err != nil {
			log_p.Errorf(lr.ctx, "List value_read(%v) fail: %s", vr, err.Error())
			return fmt.Errorf("list value_read(%v) fail: %s", vr, err.Error())
		}

		_elem = vr.Value
		if _elem != nil {
			tSlice = append(tSlice, _elem)
		}
	}
	if err := ip.ReadListEnd(); err != nil {
		log_p.Errorf(lr.ctx, "ListRead error reading list end: %s", err.Error())
		return fmt.Errorf("ListRead error reading list end: %s", err.Error())
	}
	lr.Value = tSlice
	return nil
}
*/


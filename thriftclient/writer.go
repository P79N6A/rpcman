package thriftclient

import (
	"code.byted.org/ee/lobster-apigate/util/metrics"
	"code.byted.org/ee/lobster-idlloader/types"
	"errors"
	"fmt"
	"github.com/afLnk/idlparser/idltypes"
	"strconv"
	"github.com/apache/thrift/lib/go/thrift"
)

// CommonRequest TODO: add comment
type CommonWriter struct {
	function idltypes.Function
	seqID int32
}

func (cr *CommonWriter)String() string{
	return fmt.Sprintf("CommonReqWrite: %s-%s", cr.CMD, cr.PSM)
}

func Encode(op thrift.TProtocol, req interface{}){

}
// NewEncoder TODO: add comment
func NewEncoder(fnc idltypes.Function, seqID int32) *CommonWriter {
	return &CommonWriter{
		function:fnc,
		seqID:seqID,
	}
}

// WriteArgs TODO: add comment
func (cr CommonWriter) WriteArgs(op thrift.TProtocol, req interface{}) (err error) {
	if err = op.WriteMessageBegin(cr.function.Name(), thrift.CALL, cr.seqID); err != nil {
		return
	}

	argsName := cr.function.Name() + "_args"
	if err := op.WriteStructBegin(argsName); err != nil {
		return fmt.Errorf("%s write struct begin error: %s", argsName, err)
	}

	requestField := cr.function.RequestField()
	if requestField != nil{
		if err = op.WriteFieldBegin("req", thrift.STRUCT, 1); err != nil {
			return fmt.Errorf("req write field begin error 1:req: %s", req, err)
		}
		if err = cr.WriteField(op, req, requestField); err != nil {
			return fmt.Errorf("%s error writing struct: %s", reqType.GetName(), err)
		}
		if err = op.WriteFieldEnd(); err != nil {
			return fmt.Errorf("%s write field end error 1:req: %s", reqType.GetName(), err)
		}

		if err := op.WriteFieldStop(); err != nil {
			return fmt.Errorf("write field stop error: %s", err)
		}
	}

	if err := op.WriteStructEnd(); err != nil {
		return fmt.Errorf("write struct stop error: %s", err)
	}

	if err = op.WriteMessageEnd(); err != nil {
		return
	}

	return
}

// WriteBodyFields TODO: add comment
func (cr CommonWriter) WriteBodyFields(op thrift.TProtocol, request *CommonReq, reqType *types.IDLStruct) (err error) {
	for k, v := range reqType.NameMembers {
		if v.Required == 0 { //需要的
			_, ok := request.Body[k]
			if !ok { //必填字段没有传。
				cr.crLog.Infof("[WriteBodyFields]: file: %s struct: %s, request: %s missed", reqType.GetFileName(), reqType.IDLDefine.GetName(), k)
			}
		}
	}
	idlMember, okName := reqType.NameMembers["Files"]
	if okName {
		listType, okType := idlMember.GetFieldType().(*types.IDLList)
		if okType && listType.GetValueType().GetName() == "File" {
			if err := cr.WriteFiles(op, request.Files, idlMember); err != nil {
				return err
			}
		}
	}
	for k, v := range request.Body {
		idlMember, ok := reqType.NameMembers[k]
		if !ok { // 这个字段协议不能识别，跳过
			metrics.EmitCounterV2(metrics.Metric_CommCli_FailWriterType,
				map[string]string{"err_file_type": fmt.Sprintf("%s_%s", reqType.GetFileName(), reqType.GetName())})
			continue
		}

		if err := cr.WriteField(op, v, idlMember); err != nil {
			return fmt.Errorf("%s WriteField err: %v", cr, err)
		}
	}
	return nil
}

// WriteFiles TODO: add comment
func (cr CommonWriter) WriteFiles(op thrift.TProtocol, files []*common.File, member *types.IDLField) error {
	dataList := make([]interface{}, 0, len(files))
	for _, fileInfo := range files {
		oneData := make(map[string]interface{})
		oneData["Name"] = fileInfo.Name
		oneData["Content"] = fileInfo.Content
		dataList = append(dataList, oneData)
	}
	return cr.WriteList(op, dataList, member)
}

func (cr CommonWriter)writeFieldValue(op thrift.TProtocol, fieldName string, fieldType idltypes.TypeI, value interface{})error{
	switch realType := fieldType.(type){
	case *idltypes.Struct:
		realData, ok := value.(map[string]interface{})
		if !ok{
			return nil
		}
		return cr.WriteStruct(op, fieldName, realType, realData)
	case *idltypes.Map:
		realData, ok := value.(map[string]interface{})
		if !ok{
			return nil
		}
		return cr.WriteMap(op, realType, realData)
	case *idltypes.List:
		dataArray, ok := value.([]interface{})
		if !ok {
			dataArray = append(dataArray, value)
		}
		return cr.WriteList(op, realType, dataArray)
	case *idltypes.Set:
		dataArray, ok := value.([]interface{})
		if !ok {
			dataArray = append(dataArray, value)
		}
		return cr.WriteSet(op, realType, dataArray)
	case *idltypes.BaseType:
		return cr.writeBaseType(op, field, realType, value)
	case *idltypes.Enum:
		return cr.writeEnum(op, value, member)
	}

	return fmt.Errorf("unsupport config type %s", ft.GetName())
}

// WriteField TODO: add comment
func (cr CommonWriter) WriteField(op thrift.TProtocol, data interface{}, field *idltypes.Field) error {
	if err := op.WriteFieldBegin(field.Name(), thrift.STRUCT, int16(field.Tag())); err != nil {
		return buildWriteError(field, "begin", err)
	}

	if err := cr.writeFieldValue(op, data, field); err != nil{
		return fmt.Errorf("eerere")//TODO
	}

	if err := op.WriteFieldEnd(); err != nil {
		return buildWriteError(field, "end", err)
	}
}

func buildWriteError(member *idltypes.Field, writePhase string, err error) error {
	if member != nil {
		return fmt.Errorf("field %s write %s error: %s", member, writePhase, err)
	}
	return err
}

func (cr CommonWriter)writeEnumValue(op thrift.TProtocol, data interface{}, tType *types.IDLEnum) error {
	var iVal int32
	switch emV := data.(type) {
	case (string):
		numVal, err := strconv.Atoi(emV)
		if err == nil {
			iVal = int32(numVal)
		} else {
			emK, ok := tType.GetValue(emV)
			if !ok{
				cr.crLog.Errorf("invalid enum name:%s for enum:%s", emV, tType.GetName())
			}

			iVal = int32(emK)
		}
	case (float64):
		iVal = int32(emV)
	default:
		return errors.New("unsupport basic data type of thrift:" + tType.GetName())
	}

	return op.WriteI32(iVal)
}

// writeEnum TODO: add comment
func (cr CommonWriter) writeEnum(op thrift.TProtocol, data interface{}, member *types.IDLField) error {
	fieldType := baseName2ThriftTypeIDMap["I32"]
	if err := cr.writeEnumValue(op, data, bt); err != nil {
		return buildWriteError(member, "value", err)
	}
	return nil
}

// writeBaseType TODO: add comment
func (cr CommonWriter) writeBaseType(op thrift.TProtocol, baseType *idltypes.BaseType, value interface{}) error {
	fieldType := idlbasetype2ThriftTypeID(baseType)
	switch fieldType{
	case thrift.I32:
		idata, err1 := toI64(data)
		if err1 != nil {
			return fmt.Errorf("write i32 fail:%s", err1.Error())
		}
		op.WriteI32(int32(idata))
	case thrift.I64:

	}

	tType.GetName() {
	case types.BaseTypeI32.GetName():

	case types.BaseTypeI64.GetName():
		idata, err1 := getInt64ConcludeStr(data)
		if err1 != nil {
			return fmt.Errorf("write i64 fail:%s", err1.Error())
		}
		err = op.WriteI64(idata)
	case types.BaseTypeI16.GetName():
		idata, err1 := getInt64ConcludeStr(data)
		if err1 != nil {
			return fmt.Errorf("write i16 fail:%s", err1.Error())
		}
		err = op.WriteI16(int16(idata))
	case types.BaseTypeI8.GetName():
		idata, err1 := getInt64ConcludeStr(data)
		if err1 != nil {
			return fmt.Errorf("write i8 fail:%s", err1.Error())
		}
		err = op.WriteByte(byte(idata))
	case types.BaseTypeDouble.GetName():
		ddata, err1 := getDouble(data)
		if err1 != nil {
			return fmt.Errorf("write double fail:%s", err1.Error())
		}
		err = op.WriteDouble(ddata)
	case types.BaseTypeString.GetName():
		sdata, err1 := getString(data)
		if err1 != nil {
			return fmt.Errorf("write string fail:%s", err1.Error())
		}

		err = op.WriteString(sdata)
	case types.BaseTypeBool.GetName():
		bdata, err1 := getBool(data)
		if err1 != nil {
			return fmt.Errorf("write bool fail:%s", err1.Error())
		}
		err = op.WriteBool(bdata)
	case types.BaseTypeBinary.GetName():
		binData, err1 := getByteArr(data)
		if err1 != nil {
			return fmt.Errorf("write BaseTypeBinary fail:%s", err1.Error())
		}
		err = op.WriteBinary(binData)
	default:
		err = errors.New("unsupport basic data type of thrift:" + tType.GetName())
	}
	return
	return nil
}

// WriteMap TODO: add comment
func (cr CommonWriter) WriteMap(op thrift.TProtocol, mAp *idltypes.Map, data map[string]interface{}) error {
	_ktt := idltype2ThriftTypeID(_kIdl)
	if _ktt == 0 {
		msg := fmt.Sprintf("no support type: %s", _vIdl.GetName())
		cr.crLog.Error(msg)
		return errors.New(msg)
	}

	vtt := idltype2ThriftTypeID(_vIdl)
	if vtt == 0 {
		msg := fmt.Sprintf("no support type: %s", _vIdl.GetName())
		cr.crLog.Error(msg)
		return errors.New(msg)
	}

	if err := op.WriteMapBegin(_ktt, vtt, len(data)); err != nil {
		return fmt.Errorf("error writing map begin: %s", err)
	}
	for k, v := range data {
		if err := cr.writeFieldValue(op, k, _kIdl); err != nil {
			msg := fmt.Sprintf("%s key write error: %s", _kIdl.GetName(), err)
			cr.crLog.Error(msg)
			return errors.New(msg)
		}
		if err := cr.writeFieldValue(op, v, _vIdl); err != nil {
			msg := fmt.Sprintf("%s value write error: %s", _vIdl.GetName(), err)
			cr.crLog.Error(msg)
			return errors.New(msg)
		}
	}
	if err := op.WriteMapEnd(); err != nil {
		msg := fmt.Sprintf("error writing map end: %s", err)
		cr.crLog.Error(msg)
		return errors.New(msg)
	}
	return nil

	return nil
}

// WriteList TODO: add comment
func (cr CommonWriter) WriteList(op thrift.TProtocol, lIst *idltypes.List, data []interface{}) error {
	if err := op.WriteListBegin(thrift.LIST, len(data)); err != nil {
		return fmt.Errorf("error writing list begin: %s", err)
	}
	for _, v := range data {
		if err := cr.writeFieldValue(op, v, lIst.ValueType()); err != nil {
			return fmt.Errorf("%s field write error: %s", lIst.ValueType().Name(), err)
		}
	}
	if err := op.WriteListEnd(); err != nil {
		return fmt.Errorf("error writing list end: %s", err)
	}
	return nil
}

// WriteSet TODO: add comment
func (cr CommonWriter) WriteSet(op thrift.TProtocol, sEt *idltypes.Set, data []interface{}) error {
	keyType := sEt.KeyType()
	if keyType == nil{
		return fmt.Errorf("set key type is nil")
	}

	thriftTypeID := idltype2ThriftTypeID(keyType)
	if thriftTypeID == thrift.STOP{
		return fmt.Errorf("invalid key type:%s", keyType)
	}

	if err := op.WriteSetBegin(thriftTypeID, len(data)); err != nil {
		return fmt.Errorf("error writing set begin: %s", err)
	}

	for _, v := range data {
		if err := cr.writeFieldValue(op, v, sEt.KeyType()); err != nil {
			return fmt.Errorf("%s field write error: %s", sEt.KeyType().Name(), err)
		}
	}

	if err := op.WriteSetEnd(); err != nil {
		return fmt.Errorf("error writing set end: %s", err)
	}
	return nil
}

// WriteStruct TODO: add comment
func (cr CommonWriter) WriteStruct(op thrift.TProtocol, structName string, st *idltypes.Struct, data map[string]interface{}) (err error) {
	if err := op.WriteStructBegin(structName); err != nil {
		return fmt.Errorf("write struct start error: %s", err)
	}

	for k, v := range data {
		subField := st.GetFieldByName(k)
		if subField == nil{
			// TODO, drop data.
			continue
		}

		if err := cr.WriteField(op, v, subField); err != nil {
			return err
		}
	}

	if err := op.WriteFieldStop(); err != nil {
		return fmt.Errorf("write field stop error: %s", err)
	}

	if err := op.WriteStructEnd(); err != nil {
		return fmt.Errorf("write struct stop error: %s", err)
	}

	return nil
}


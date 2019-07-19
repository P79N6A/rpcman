package thriftclient

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/afLnk/idlparser/idltypes"
	"strconv"
	"strings"
)

var (
	baseName2ThriftTypeIDMap = map[string]thrift.TType{
		idltypes.BaseTypeStop.Name():   thrift.STOP,
		idltypes.BaseTypeVoid.Name():   thrift.VOID,
		idltypes.BaseTypeBool.Name():   thrift.BOOL,
		idltypes.BaseTypeByte.Name():   thrift.BYTE,
		idltypes.BaseTypeI8.Name():     thrift.BYTE,
		idltypes.BaseTypeI16.Name():    thrift.I16,
		idltypes.BaseTypeI32.Name():    thrift.I32,
		idltypes.BaseTypeI64.Name():    thrift.I64,
		idltypes.BaseTypeString.Name(): thrift.STRING,
		idltypes.BaseTypeUTF7.Name():   thrift.UTF7,
		idltypes.BaseTypeUTF8.Name():   thrift.UTF8,
		idltypes.BaseTypeUTF16.Name():   thrift.UTF16,
		idltypes.BaseTypeDouble.Name():   thrift.DOUBLE,
		idltypes.BaseTypeBinary.Name():   thrift.STRING,
	}
)

func idlbasetype2ThriftTypeID(bt *idltypes.BaseType) thrift.TType{
	ret, ok := baseName2ThriftTypeIDMap[bt.Name()]
	if ok{
		return ret
	}

	return thrift.STOP
}

func idltype2ThriftTypeID(idlType idltypes.TypeI) thrift.TType {
	switch ft := idlType.(type) {
	case *idltypes.Struct:
		return thrift.STRUCT
	case *idltypes.List:
		return thrift.LIST
	case *idltypes.Set:
		return thrift.SET
	case *idltypes.Map:
		return thrift.MAP
	case *idltypes.BaseType:
		return idlbasetype2ThriftTypeID(ft)
	}

	return thrift.STOP
}


func toI64(data interface{}) (int64, error) {
	if data == nil{
		return 0, nil
	}

	switch realData := data.(type) {
	case string:
		if realData == ""{
			return 0, nil
		}

		i, err := strconv.ParseInt(realData, 10, 64)
		if err != nil {
			return 0, err
		}
		return i, nil

	case float64:
		return int64(realData), nil
	case int32:
	case int:
	case int8:
	case int16:
		return int64(realData), nil
	case int64:
		return realData, nil
	}

	return 0, fmt.Errorf("need number value, actual:%s", data)
}


func toDouble(data interface{}) (float64, error) {
	if data == nil{
		return 0, nil
	}

	switch realData := data.(type) {
	case float64:
		return realData, nil
	case int8:
		return float64(realData), nil
	case int16:
		return float64(realData), nil
	case int32:
		return float64(realData), nil
	case int64:
		return float64(realData), nil
	case int:
		return float64(realData), nil
	case string:
		if realData == ""{
			return 0, nil
		}

		f, err := strconv.ParseFloat(realData, 64)
		if err != nil {
			return 0, err
		}
		return f, nil
	}

	return 0, nil
}

func toStr(data interface{}) (string, error) {
	if data == nil{
		return "", nil
	}

	sdata, ok := data.(string)
	if ok {
		return sdata, nil
	}

	return fmt.Sprint(sdata), nil
}

func toBool(data interface{}) (bool, error) {
	if data == nil{
		return false, nil
	}

	switch realData := data.(type) {
	case bool:
		return realData, nil
	case float64:
		return realData != 0, nil
	case int8:
		return realData != 0, nil
	case int16:
		return realData != 0, nil
	case int32:
		return realData != 0, nil
	case int64:
		return realData != 0, nil
	case int:
		return realData != 0, nil
	case string:
		return strings.ToLower(realData) == "true", nil
	}

	return false, fmt.Errorf("invalid bool data:%s", data)
}

func toBytes(data interface{}) ([]byte, error) {
	if data == nil{
		return []byte{}, nil
	}

	switch realData := data.(type) {
	case []byte:
		return realData, nil
	case string:
		return []byte(realData), nil
	}

	return []byte{}, fmt.Errorf("invalid []byte:%s", data)
}


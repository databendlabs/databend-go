package godatabend

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type ColumnType interface {
	Desc() *TypeDesc
	DatabaseTypeName() string
	Nullable() (bool, bool)
	ScanType() reflect.Type
	Parse(s string) (driver.Value, error)
	Length() (int64, bool)
	PrecisionScale() (int64, int64, bool)
}

type columnTypeDefault struct{}

func (columnTypeDefault) Length() (int64, bool) {
	return 0, false
}

func (columnTypeDefault) PrecisionScale() (int64, int64, bool) {
	return 0, 0, false
}

type unknownColumnType struct {
	columnTypeDefault
	dbType string
	desc   *TypeDesc
}

func (c unknownColumnType) ScanType() reflect.Type {
	return reflectTypeString
}

func (c unknownColumnType) Nullable() (bool, bool) {
	return false, false
}

func (c unknownColumnType) DatabaseTypeName() string {
	return c.dbType
}

func (c unknownColumnType) Parse(s string) (driver.Value, error) {
	return s, nil
}

func (c unknownColumnType) Desc() *TypeDesc {
	return c.desc
}

type isNullable bool

func (b isNullable) Nullable() (bool, bool) {
	return bool(b), true
}

func (b isNullable) wrapName(s string) string {
	if bool(b) {
		return s + " NULL"
	}
	return s
}

func (b isNullable) checkNull(s string) bool {
	return bool(b) && s == "NULL"
}

type simpleColumnType struct {
	dbType    string
	scanType  reflect.Type
	nullable  bool
	parseNull bool
}

func (*simpleColumnType) PrecisionScale() (int64, int64, bool) {
	return 0, 0, false
}

func (c *simpleColumnType) DatabaseTypeName() string {
	return c.dbType
}

func (c *simpleColumnType) Nullable() (bool, bool) {
	return c.nullable, true
}

func (c *simpleColumnType) Desc() *TypeDesc {
	return &TypeDesc{Name: c.dbType, Nullable: c.nullable}
}

func (*simpleColumnType) Length() (int64, bool) {
	return 0, false
}

func (c *simpleColumnType) Parse(s string) (driver.Value, error) {
	if c.nullable && c.parseNull && s == "NULL" {
		return nil, nil
	}
	return s, nil
}

func (c *simpleColumnType) ScanType() reflect.Type {
	return c.scanType
}

type timestampColumnType struct {
	tz *time.Location
	columnTypeDefault
	isNullable
}

func (c timestampColumnType) Parse(s string) (driver.Value, error) {
	if c.checkNull(s) {
		return nil, nil
	}
	return time.ParseInLocation("2006-01-02 15:04:05.999999", s, c.tz)
}

func (c timestampColumnType) ScanType() reflect.Type {
	return reflectTypeTime
}

func (c timestampColumnType) DatabaseTypeName() string {
	return c.wrapName("Timestamp")
}

func (c timestampColumnType) Desc() *TypeDesc {
	return &TypeDesc{Name: "Timestamp", Nullable: bool(c.isNullable)}
}

type dateColumnType struct {
	columnTypeDefault
	isNullable
}

func (c dateColumnType) Parse(s string) (driver.Value, error) {
	if c.checkNull(s) {
		return nil, nil
	}
	// always return Time with location UTC
	return time.Parse("2006-01-02", s)
}

func (c dateColumnType) ScanType() reflect.Type {
	return reflectTypeTime
}

func (c dateColumnType) DatabaseTypeName() string {
	return c.wrapName("Date")
}

func (c dateColumnType) Desc() *TypeDesc {
	return &TypeDesc{Name: "Date", Nullable: bool(c.isNullable)}
}

type decimalColumnType struct {
	precision int64
	scale     int64
	columnTypeDefault
	isNullable
}

func (c *decimalColumnType) DatabaseTypeName() string {
	return c.wrapName(fmt.Sprintf("Decimal(%d, %d)", c.precision, c.scale))
}

func (c *decimalColumnType) Desc() *TypeDesc {
	return &TypeDesc{Name: "Decimal", Nullable: bool(c.isNullable), Args: []*TypeDesc{{Name: strconv.Itoa(int(c.precision))}, {Name: strconv.Itoa(int(c.scale))}}}
}

func (*decimalColumnType) Parse(s string) (driver.Value, error) {
	return s, nil
}

func (c *decimalColumnType) PrecisionScale() (int64, int64, bool) {
	return c.precision, c.scale, true
}

func (*decimalColumnType) ScanType() reflect.Type {
	return reflectTypeString
}

func NewColumnType(dbType string, opts *ColumnTypeOptions) (ColumnType, error) {
	if opts == nil {
		opts = defaultColumnTypeOptions()
	}
	desc, err := ParseTypeDesc(dbType)
	if err != nil {
		return nil, err
	}
	desc = desc.Normalize()
	nullable := isNullable(desc.Nullable)
	parseNull := opts.formatNullAsStr
	switch desc.Name {
	case "String":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeString, nullable: desc.Nullable, parseNull: false}, nil
	case "Boolean":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeBool, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Int8":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeInt8, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Int16":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeInt16, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Int32":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeInt32, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Int64":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeInt64, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "UInt8":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeUInt8, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "UInt16":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeUInt16, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "UInt32":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeUInt32, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "UInt64":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeUInt64, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Float32":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeFloat32, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Float64":
		return &simpleColumnType{dbType: nullable.wrapName(desc.Name), scanType: reflectTypeFloat64, nullable: desc.Nullable, parseNull: parseNull}, nil
	case "Timestamp":
		return &timestampColumnType{isNullable: nullable, tz: opts.timezone}, nil
	case "Date":
		return &dateColumnType{isNullable: nullable}, nil
	case "Decimal":
		precision, err := strconv.ParseInt(desc.Args[0].Name, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed precision specified for Decimal: %v", err)
		}
		scale, err := strconv.ParseInt(desc.Args[1].Name, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed scale specified for Decimal: %v", err)
		}
		return &decimalColumnType{isNullable: nullable, precision: precision, scale: scale}, nil
	default:
		return unknownColumnType{dbType: dbType, desc: desc}, nil
	}
}

type ColumnTypeOptions struct {
	formatNullAsStr bool
	timezone        *time.Location
}

func defaultColumnTypeOptions() *ColumnTypeOptions {
	return &ColumnTypeOptions{
		formatNullAsStr: false,
		timezone:        time.UTC,
	}
}

func (opt *ColumnTypeOptions) SetFormatNullAsStr(v bool) {
	opt.formatNullAsStr = v
}

func (opt *ColumnTypeOptions) SetTimezone(v *time.Location) {
	opt.timezone = v
}

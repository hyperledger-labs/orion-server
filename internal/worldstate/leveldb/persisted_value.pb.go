// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pkg/worldstate/leveldb/persisted_value.proto

package leveldb

import (
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ValueAndMetadata struct {
	Value                []byte          `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Metadata             *types.Metadata `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"`
	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	XXX_unrecognized     []byte          `json:"-"`
	XXX_sizecache        int32           `json:"-"`
}

func (m *ValueAndMetadata) Reset()         { *m = ValueAndMetadata{} }
func (m *ValueAndMetadata) String() string { return proto.CompactTextString(m) }
func (*ValueAndMetadata) ProtoMessage()    {}
func (*ValueAndMetadata) Descriptor() ([]byte, []int) {
	return fileDescriptor_8f5d039453665b3e, []int{0}
}

func (m *ValueAndMetadata) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ValueAndMetadata.Unmarshal(m, b)
}
func (m *ValueAndMetadata) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ValueAndMetadata.Marshal(b, m, deterministic)
}
func (m *ValueAndMetadata) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ValueAndMetadata.Merge(m, src)
}
func (m *ValueAndMetadata) XXX_Size() int {
	return xxx_messageInfo_ValueAndMetadata.Size(m)
}
func (m *ValueAndMetadata) XXX_DiscardUnknown() {
	xxx_messageInfo_ValueAndMetadata.DiscardUnknown(m)
}

var xxx_messageInfo_ValueAndMetadata proto.InternalMessageInfo

func (m *ValueAndMetadata) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *ValueAndMetadata) GetMetadata() *types.Metadata {
	if m != nil {
		return m.Metadata
	}
	return nil
}

func init() {
	proto.RegisterType((*ValueAndMetadata)(nil), "leveldb.ValueAndMetadata")
}

func init() {
	proto.RegisterFile("pkg/worldstate/leveldb/persisted_value.proto", fileDescriptor_8f5d039453665b3e)
}

var fileDescriptor_8f5d039453665b3e = []byte{
	// 205 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xd2, 0x29, 0xc8, 0x4e, 0xd7,
	0x2f, 0xcf, 0x2f, 0xca, 0x49, 0x29, 0x2e, 0x49, 0x2c, 0x49, 0xd5, 0xcf, 0x49, 0x2d, 0x4b, 0xcd,
	0x49, 0x49, 0xd2, 0x2f, 0x48, 0x2d, 0x2a, 0xce, 0x2c, 0x2e, 0x49, 0x4d, 0x89, 0x2f, 0x4b, 0xcc,
	0x29, 0x4d, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x87, 0x4a, 0x4b, 0xc9, 0x95, 0x54,
	0x16, 0xa4, 0x16, 0xeb, 0x17, 0x96, 0xa6, 0x16, 0x55, 0xc6, 0x27, 0xe6, 0xa5, 0xc4, 0x17, 0xa5,
	0x16, 0x17, 0xe4, 0xe7, 0x15, 0x43, 0x15, 0x2a, 0x85, 0x72, 0x09, 0x84, 0x81, 0xf4, 0x39, 0xe6,
	0xa5, 0xf8, 0xa6, 0x96, 0x24, 0xa6, 0x24, 0x96, 0x24, 0x0a, 0x89, 0x70, 0xb1, 0x82, 0xcd, 0x92,
	0x60, 0x54, 0x60, 0xd4, 0xe0, 0x09, 0x82, 0x70, 0x84, 0xb4, 0xb9, 0x38, 0x72, 0xa1, 0x2a, 0x24,
	0x98, 0x14, 0x18, 0x35, 0xb8, 0x8d, 0xf8, 0xf5, 0xc0, 0x86, 0xeb, 0xc1, 0x34, 0x06, 0xc1, 0x15,
	0x38, 0x59, 0x47, 0x59, 0xa6, 0x67, 0x96, 0x64, 0x94, 0x26, 0xe9, 0x65, 0x26, 0xe5, 0xea, 0x25,
	0xe7, 0xe7, 0xea, 0x27, 0xe5, 0xe4, 0x27, 0x67, 0x27, 0x67, 0x24, 0x66, 0xe6, 0xa5, 0x24, 0xe9,
	0x17, 0xa7, 0x16, 0x95, 0xa5, 0x16, 0xe9, 0x63, 0xf7, 0x52, 0x12, 0x1b, 0xd8, 0x69, 0xc6, 0x80,
	0x00, 0x00, 0x00, 0xff, 0xff, 0x90, 0x50, 0x12, 0xc1, 0xf3, 0x00, 0x00, 0x00,
}

// Code generated by protoc-gen-go. DO NOT EDIT.
// source: internal/mptrie/trie.proto

package mptrie

import (
	fmt "fmt"
	math "math"

	proto "github.com/golang/protobuf/proto"
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

type BranchNode struct {
	Children             [][]byte `protobuf:"bytes,1,rep,name=children,proto3" json:"children,omitempty"`
	ValuePtr             []byte   `protobuf:"bytes,2,opt,name=valuePtr,proto3" json:"valuePtr,omitempty"`
	Deleted              bool     `protobuf:"varint,3,opt,name=deleted,proto3" json:"deleted,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BranchNode) Reset()         { *m = BranchNode{} }
func (m *BranchNode) String() string { return proto.CompactTextString(m) }
func (*BranchNode) ProtoMessage()    {}
func (*BranchNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_d3bacd5d6aa6830e, []int{0}
}

func (m *BranchNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BranchNode.Unmarshal(m, b)
}
func (m *BranchNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BranchNode.Marshal(b, m, deterministic)
}
func (m *BranchNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BranchNode.Merge(m, src)
}
func (m *BranchNode) XXX_Size() int {
	return xxx_messageInfo_BranchNode.Size(m)
}
func (m *BranchNode) XXX_DiscardUnknown() {
	xxx_messageInfo_BranchNode.DiscardUnknown(m)
}

var xxx_messageInfo_BranchNode proto.InternalMessageInfo

func (m *BranchNode) GetChildren() [][]byte {
	if m != nil {
		return m.Children
	}
	return nil
}

func (m *BranchNode) GetValuePtr() []byte {
	if m != nil {
		return m.ValuePtr
	}
	return nil
}

func (m *BranchNode) GetDeleted() bool {
	if m != nil {
		return m.Deleted
	}
	return false
}

type ExtensionNode struct {
	Key                  []byte   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Child                []byte   `protobuf:"bytes,2,opt,name=child,proto3" json:"child,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ExtensionNode) Reset()         { *m = ExtensionNode{} }
func (m *ExtensionNode) String() string { return proto.CompactTextString(m) }
func (*ExtensionNode) ProtoMessage()    {}
func (*ExtensionNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_d3bacd5d6aa6830e, []int{1}
}

func (m *ExtensionNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ExtensionNode.Unmarshal(m, b)
}
func (m *ExtensionNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ExtensionNode.Marshal(b, m, deterministic)
}
func (m *ExtensionNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExtensionNode.Merge(m, src)
}
func (m *ExtensionNode) XXX_Size() int {
	return xxx_messageInfo_ExtensionNode.Size(m)
}
func (m *ExtensionNode) XXX_DiscardUnknown() {
	xxx_messageInfo_ExtensionNode.DiscardUnknown(m)
}

var xxx_messageInfo_ExtensionNode proto.InternalMessageInfo

func (m *ExtensionNode) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *ExtensionNode) GetChild() []byte {
	if m != nil {
		return m.Child
	}
	return nil
}

type ValueNode struct {
	Key                  []byte   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	ValuePtr             []byte   `protobuf:"bytes,2,opt,name=valuePtr,proto3" json:"valuePtr,omitempty"`
	Deleted              bool     `protobuf:"varint,3,opt,name=deleted,proto3" json:"deleted,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ValueNode) Reset()         { *m = ValueNode{} }
func (m *ValueNode) String() string { return proto.CompactTextString(m) }
func (*ValueNode) ProtoMessage()    {}
func (*ValueNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_d3bacd5d6aa6830e, []int{2}
}

func (m *ValueNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ValueNode.Unmarshal(m, b)
}
func (m *ValueNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ValueNode.Marshal(b, m, deterministic)
}
func (m *ValueNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ValueNode.Merge(m, src)
}
func (m *ValueNode) XXX_Size() int {
	return xxx_messageInfo_ValueNode.Size(m)
}
func (m *ValueNode) XXX_DiscardUnknown() {
	xxx_messageInfo_ValueNode.DiscardUnknown(m)
}

var xxx_messageInfo_ValueNode proto.InternalMessageInfo

func (m *ValueNode) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *ValueNode) GetValuePtr() []byte {
	if m != nil {
		return m.ValuePtr
	}
	return nil
}

func (m *ValueNode) GetDeleted() bool {
	if m != nil {
		return m.Deleted
	}
	return false
}

type EmptyNode struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *EmptyNode) Reset()         { *m = EmptyNode{} }
func (m *EmptyNode) String() string { return proto.CompactTextString(m) }
func (*EmptyNode) ProtoMessage()    {}
func (*EmptyNode) Descriptor() ([]byte, []int) {
	return fileDescriptor_d3bacd5d6aa6830e, []int{3}
}

func (m *EmptyNode) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_EmptyNode.Unmarshal(m, b)
}
func (m *EmptyNode) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_EmptyNode.Marshal(b, m, deterministic)
}
func (m *EmptyNode) XXX_Merge(src proto.Message) {
	xxx_messageInfo_EmptyNode.Merge(m, src)
}
func (m *EmptyNode) XXX_Size() int {
	return xxx_messageInfo_EmptyNode.Size(m)
}
func (m *EmptyNode) XXX_DiscardUnknown() {
	xxx_messageInfo_EmptyNode.DiscardUnknown(m)
}

var xxx_messageInfo_EmptyNode proto.InternalMessageInfo

func init() {
	proto.RegisterType((*BranchNode)(nil), "mptrie.BranchNode")
	proto.RegisterType((*ExtensionNode)(nil), "mptrie.ExtensionNode")
	proto.RegisterType((*ValueNode)(nil), "mptrie.ValueNode")
	proto.RegisterType((*EmptyNode)(nil), "mptrie.EmptyNode")
}

func init() { proto.RegisterFile("internal/mptrie/trie.proto", fileDescriptor_d3bacd5d6aa6830e) }

var fileDescriptor_d3bacd5d6aa6830e = []byte{
	// 230 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x90, 0xcf, 0x4b, 0x03, 0x31,
	0x10, 0x85, 0x59, 0x17, 0x6b, 0x3b, 0x2a, 0xc8, 0xe2, 0x21, 0xf4, 0xb4, 0xec, 0xa9, 0xa7, 0x0d,
	0xa8, 0xe0, 0xbd, 0xd0, 0xab, 0x48, 0x05, 0x0f, 0x1e, 0x84, 0x4d, 0x32, 0x98, 0xd0, 0xfc, 0x58,
	0xa6, 0xd3, 0x62, 0xff, 0x7b, 0x49, 0x6a, 0x3d, 0x08, 0x5e, 0xbc, 0x84, 0x7c, 0x19, 0xf2, 0xbd,
	0xe1, 0xc1, 0xdc, 0x45, 0x46, 0x8a, 0x83, 0x97, 0x61, 0x64, 0x72, 0x28, 0xf3, 0xd1, 0x8f, 0x94,
	0x38, 0x35, 0x93, 0xe3, 0x53, 0xf7, 0x0e, 0xb0, 0xa4, 0x21, 0x6a, 0xfb, 0x94, 0x0c, 0x36, 0x73,
	0x98, 0x6a, 0xeb, 0xbc, 0x21, 0x8c, 0xa2, 0x6a, 0xeb, 0xc5, 0xd5, 0xfa, 0x87, 0xf3, 0x6c, 0x3f,
	0xf8, 0x1d, 0x3e, 0x33, 0x89, 0xb3, 0xb6, 0xca, 0xb3, 0x13, 0x37, 0x02, 0x2e, 0x0c, 0x7a, 0x64,
	0x34, 0xa2, 0x6e, 0xab, 0xc5, 0x74, 0x7d, 0xc2, 0xee, 0x11, 0xae, 0x57, 0x9f, 0x8c, 0x71, 0xeb,
	0x52, 0x2c, 0x11, 0x37, 0x50, 0x6f, 0xf0, 0x20, 0xaa, 0x62, 0xc8, 0xd7, 0xe6, 0x16, 0xce, 0x4b,
	0xc8, 0xb7, 0xf5, 0x08, 0xdd, 0x0b, 0xcc, 0x5e, 0xb3, 0xfe, 0x8f, 0x4f, 0xff, 0xdb, 0xe6, 0x12,
	0x66, 0xab, 0x30, 0xf2, 0x21, 0x4b, 0x97, 0x0f, 0x6f, 0x77, 0x1f, 0x8e, 0xed, 0x4e, 0xf5, 0x4e,
	0x85, 0x5e, 0xa7, 0x20, 0x95, 0x4f, 0x7a, 0xa3, 0xed, 0xe0, 0xa2, 0x51, 0x72, 0x8b, 0xb4, 0x47,
	0x92, 0xbf, 0x3a, 0x54, 0x93, 0xd2, 0xdf, 0xfd, 0x57, 0x00, 0x00, 0x00, 0xff, 0xff, 0xc3, 0xb6,
	0x28, 0x54, 0x5d, 0x01, 0x00, 0x00,
}

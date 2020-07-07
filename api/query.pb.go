// Code generated by protoc-gen-go. DO NOT EDIT.
// source: query.proto

package api

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
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

func init() { proto.RegisterFile("query.proto", fileDescriptor_5c6ac9b241082464) }

var fileDescriptor_5c6ac9b241082464 = []byte{
	// 339 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x74, 0x51, 0x4b, 0x4b, 0xc3, 0x40,
	0x10, 0x46, 0x4a, 0x7d, 0x6c, 0x1f, 0xe2, 0x2a, 0x22, 0xf1, 0x28, 0xd4, 0x9e, 0x36, 0xa2, 0xe2,
	0xc5, 0x5b, 0xb1, 0xa4, 0x07, 0x05, 0xb5, 0xd5, 0x83, 0x97, 0xb2, 0x49, 0xa7, 0xe9, 0xd2, 0x34,
	0x13, 0x77, 0x27, 0xa5, 0xfd, 0x4f, 0xfe, 0x48, 0xc9, 0xee, 0x56, 0x72, 0xf1, 0x36, 0xdf, 0x83,
	0x6f, 0xbf, 0x99, 0x65, 0xad, 0xef, 0x12, 0xf4, 0x56, 0x14, 0x1a, 0x09, 0x79, 0x43, 0x16, 0x2a,
	0x68, 0xd1, 0xb6, 0x00, 0xe3, 0x98, 0xe0, 0x2c, 0xce, 0x30, 0x59, 0x4e, 0x69, 0x33, 0xad, 0xb3,
	0x97, 0x29, 0x62, 0x9a, 0x41, 0x68, 0x51, 0x5c, 0xce, 0x43, 0x58, 0x15, 0xe4, 0x43, 0x6e, 0x7f,
	0x1a, 0xac, 0xf9, 0x56, 0x85, 0xf2, 0x2b, 0x76, 0x14, 0x01, 0x8d, 0x49, 0x52, 0x69, 0xf8, 0x81,
	0x90, 0x85, 0x12, 0x4f, 0x83, 0xa0, 0xe3, 0x07, 0xcf, 0xf7, 0xd8, 0xa1, 0x37, 0x01, 0xef, 0x3a,
	0x49, 0x92, 0xb4, 0x01, 0x01, 0xb3, 0xf8, 0x53, 0x66, 0x25, 0xf0, 0x07, 0xc6, 0x22, 0xa0, 0x91,
	0x32, 0x84, 0x7a, 0xcb, 0x4f, 0xac, 0xe2, 0x91, 0x33, 0x9f, 0xd6, 0xa9, 0x2a, 0x63, 0x0c, 0x74,
	0xb3, 0xc7, 0xaf, 0x6d, 0xfe, 0xab, 0x46, 0x9c, 0xf3, 0x63, 0x6b, 0xb1, 0x73, 0xfd, 0x01, 0x27,
	0xde, 0xb3, 0x4e, 0x04, 0xf4, 0x02, 0x7a, 0x99, 0xc1, 0x3b, 0x22, 0xf1, 0x73, 0xe1, 0xd6, 0x14,
	0xbb, 0x35, 0xc5, 0xb0, 0x5a, 0x33, 0x68, 0xb9, 0x96, 0x2a, 0x05, 0x43, 0x3c, 0x64, 0xcd, 0x08,
	0x68, 0xb2, 0xe1, 0x6d, 0xcb, 0x4e, 0x36, 0x2e, 0xf8, 0xc2, 0x21, 0x2d, 0x73, 0x23, 0x13, 0x52,
	0x98, 0x0f, 0xf3, 0x35, 0x64, 0x58, 0x80, 0xef, 0x33, 0xa8, 0xce, 0xea, 0xfb, 0xd8, 0xb9, 0xde,
	0xc7, 0x89, 0x7d, 0x6b, 0xfc, 0x30, 0xa0, 0x8d, 0x3f, 0x4c, 0x35, 0x3b, 0x5f, 0xfb, 0x0f, 0x8f,
	0x81, 0xf8, 0x23, 0xeb, 0xee, 0x22, 0x47, 0xa0, 0xd2, 0xc5, 0xff, 0xd5, 0xdd, 0xd9, 0x9e, 0x61,
	0x96, 0x82, 0x76, 0xd6, 0x41, 0xff, 0xab, 0x97, 0x2a, 0x5a, 0x94, 0xb1, 0x50, 0xf1, 0x4a, 0x24,
	0xb8, 0x0a, 0xed, 0x97, 0x27, 0x0b, 0xa9, 0xf2, 0x59, 0x1c, 0x1a, 0xd0, 0x6b, 0xd0, 0xa1, 0x2c,
	0x54, 0xbc, 0x6f, 0xc3, 0xee, 0x7e, 0x03, 0x00, 0x00, 0xff, 0xff, 0x07, 0xdc, 0x44, 0x51, 0x33,
	0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// QueryClient is the client API for Query service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type QueryClient interface {
	GetStatus(ctx context.Context, in *DB, opts ...grpc.CallOption) (*DBStatus, error)
	GetState(ctx context.Context, in *DataQuery, opts ...grpc.CallOption) (*Value, error)
	GetHistory(ctx context.Context, in *HistoryQuery, opts ...grpc.CallOption) (Query_GetHistoryClient, error)
	GetProof(ctx context.Context, in *ProofQuery, opts ...grpc.CallOption) (*Proof, error)
	GetMerkleRoot(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*Digest, error)
	GetTx(ctx context.Context, in *TxQuery, opts ...grpc.CallOption) (*TransactionEnvelope, error)
	GetBlock(ctx context.Context, in *BlockQuery, opts ...grpc.CallOption) (*Block, error)
	GetUsers(ctx context.Context, in *UserQuery, opts ...grpc.CallOption) (*UserSet, error)
	GetBlockHeight(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*LedgerHeight, error)
}

type queryClient struct {
	cc *grpc.ClientConn
}

func NewQueryClient(cc *grpc.ClientConn) QueryClient {
	return &queryClient{cc}
}

func (c *queryClient) GetStatus(ctx context.Context, in *DB, opts ...grpc.CallOption) (*DBStatus, error) {
	out := new(DBStatus)
	err := c.cc.Invoke(ctx, "/api.Query/GetStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetState(ctx context.Context, in *DataQuery, opts ...grpc.CallOption) (*Value, error) {
	out := new(Value)
	err := c.cc.Invoke(ctx, "/api.Query/GetState", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetHistory(ctx context.Context, in *HistoryQuery, opts ...grpc.CallOption) (Query_GetHistoryClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Query_serviceDesc.Streams[0], "/api.Query/GetHistory", opts...)
	if err != nil {
		return nil, err
	}
	x := &queryGetHistoryClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Query_GetHistoryClient interface {
	Recv() (*HistoryDataSet, error)
	grpc.ClientStream
}

type queryGetHistoryClient struct {
	grpc.ClientStream
}

func (x *queryGetHistoryClient) Recv() (*HistoryDataSet, error) {
	m := new(HistoryDataSet)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *queryClient) GetProof(ctx context.Context, in *ProofQuery, opts ...grpc.CallOption) (*Proof, error) {
	out := new(Proof)
	err := c.cc.Invoke(ctx, "/api.Query/GetProof", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetMerkleRoot(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*Digest, error) {
	out := new(Digest)
	err := c.cc.Invoke(ctx, "/api.Query/GetMerkleRoot", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetTx(ctx context.Context, in *TxQuery, opts ...grpc.CallOption) (*TransactionEnvelope, error) {
	out := new(TransactionEnvelope)
	err := c.cc.Invoke(ctx, "/api.Query/GetTx", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetBlock(ctx context.Context, in *BlockQuery, opts ...grpc.CallOption) (*Block, error) {
	out := new(Block)
	err := c.cc.Invoke(ctx, "/api.Query/GetBlock", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetUsers(ctx context.Context, in *UserQuery, opts ...grpc.CallOption) (*UserSet, error) {
	out := new(UserSet)
	err := c.cc.Invoke(ctx, "/api.Query/GetUsers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *queryClient) GetBlockHeight(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*LedgerHeight, error) {
	out := new(LedgerHeight)
	err := c.cc.Invoke(ctx, "/api.Query/GetBlockHeight", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// QueryServer is the server API for Query service.
type QueryServer interface {
	GetStatus(context.Context, *DB) (*DBStatus, error)
	GetState(context.Context, *DataQuery) (*Value, error)
	GetHistory(*HistoryQuery, Query_GetHistoryServer) error
	GetProof(context.Context, *ProofQuery) (*Proof, error)
	GetMerkleRoot(context.Context, *empty.Empty) (*Digest, error)
	GetTx(context.Context, *TxQuery) (*TransactionEnvelope, error)
	GetBlock(context.Context, *BlockQuery) (*Block, error)
	GetUsers(context.Context, *UserQuery) (*UserSet, error)
	GetBlockHeight(context.Context, *empty.Empty) (*LedgerHeight, error)
}

// UnimplementedQueryServer can be embedded to have forward compatible implementations.
type UnimplementedQueryServer struct {
}

func (*UnimplementedQueryServer) GetStatus(ctx context.Context, req *DB) (*DBStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetStatus not implemented")
}
func (*UnimplementedQueryServer) GetState(ctx context.Context, req *DataQuery) (*Value, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetState not implemented")
}
func (*UnimplementedQueryServer) GetHistory(req *HistoryQuery, srv Query_GetHistoryServer) error {
	return status.Errorf(codes.Unimplemented, "method GetHistory not implemented")
}
func (*UnimplementedQueryServer) GetProof(ctx context.Context, req *ProofQuery) (*Proof, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetProof not implemented")
}
func (*UnimplementedQueryServer) GetMerkleRoot(ctx context.Context, req *empty.Empty) (*Digest, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMerkleRoot not implemented")
}
func (*UnimplementedQueryServer) GetTx(ctx context.Context, req *TxQuery) (*TransactionEnvelope, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTx not implemented")
}
func (*UnimplementedQueryServer) GetBlock(ctx context.Context, req *BlockQuery) (*Block, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlock not implemented")
}
func (*UnimplementedQueryServer) GetUsers(ctx context.Context, req *UserQuery) (*UserSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetUsers not implemented")
}
func (*UnimplementedQueryServer) GetBlockHeight(ctx context.Context, req *empty.Empty) (*LedgerHeight, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetBlockHeight not implemented")
}

func RegisterQueryServer(s *grpc.Server, srv QueryServer) {
	s.RegisterService(&_Query_serviceDesc, srv)
}

func _Query_GetStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DB)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetStatus(ctx, req.(*DB))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DataQuery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetState",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetState(ctx, req.(*DataQuery))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetHistory_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(HistoryQuery)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(QueryServer).GetHistory(m, &queryGetHistoryServer{stream})
}

type Query_GetHistoryServer interface {
	Send(*HistoryDataSet) error
	grpc.ServerStream
}

type queryGetHistoryServer struct {
	grpc.ServerStream
}

func (x *queryGetHistoryServer) Send(m *HistoryDataSet) error {
	return x.ServerStream.SendMsg(m)
}

func _Query_GetProof_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProofQuery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetProof(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetProof",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetProof(ctx, req.(*ProofQuery))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetMerkleRoot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetMerkleRoot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetMerkleRoot",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetMerkleRoot(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetTx_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TxQuery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetTx(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetTx",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetTx(ctx, req.(*TxQuery))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BlockQuery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetBlock(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetBlock",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetBlock(ctx, req.(*BlockQuery))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetUsers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UserQuery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetUsers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetUsers(ctx, req.(*UserQuery))
	}
	return interceptor(ctx, in, info, handler)
}

func _Query_GetBlockHeight_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(QueryServer).GetBlockHeight(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.Query/GetBlockHeight",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(QueryServer).GetBlockHeight(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _Query_serviceDesc = grpc.ServiceDesc{
	ServiceName: "api.Query",
	HandlerType: (*QueryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetStatus",
			Handler:    _Query_GetStatus_Handler,
		},
		{
			MethodName: "GetState",
			Handler:    _Query_GetState_Handler,
		},
		{
			MethodName: "GetProof",
			Handler:    _Query_GetProof_Handler,
		},
		{
			MethodName: "GetMerkleRoot",
			Handler:    _Query_GetMerkleRoot_Handler,
		},
		{
			MethodName: "GetTx",
			Handler:    _Query_GetTx_Handler,
		},
		{
			MethodName: "GetBlock",
			Handler:    _Query_GetBlock_Handler,
		},
		{
			MethodName: "GetUsers",
			Handler:    _Query_GetUsers_Handler,
		},
		{
			MethodName: "GetBlockHeight",
			Handler:    _Query_GetBlockHeight_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetHistory",
			Handler:       _Query_GetHistory_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "query.proto",
}

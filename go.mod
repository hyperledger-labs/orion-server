module github.ibm.com/blockchaindb/server

go 1.14

require (
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.4.0
	github.com/stretchr/testify v1.6.1
	github.com/syndtr/goleveldb v1.0.0
	github.ibm.com/blockchaindb/protos v0.0.0
	go.uber.org/zap v1.10.0
)

replace github.ibm.com/blockchaindb/library v0.0.0 => ../library

replace github.ibm.com/blockchaindb/protos v0.0.0 => ../protos-go

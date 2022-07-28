// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package marshal

import (
	"bytes"
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type DefaultMarshal struct {
	marshalOption *protojson.MarshalOptions
}

func DefaultMarshaler() *DefaultMarshal {
	return &DefaultMarshal{
		marshalOption: &protojson.MarshalOptions{
			Multiline:       false,
			AllowPartial:    false,
			UseProtoNames:   true,
			UseEnumNumbers:  false,
			EmitUnpopulated: false,
			Resolver:        nil,
		},
	}
}

func (o DefaultMarshal) Marshal(m proto.Message) ([]byte, error) {
	mBytes, err := o.marshalOption.Marshal(m)
	if err != nil {
		return nil, err
	}

	compactedPayloadBytes := bytes.NewBuffer([]byte{})
	if err := json.Compact(compactedPayloadBytes, mBytes); err != nil {
		return nil, err
	}
	return compactedPayloadBytes.Bytes(), nil
}

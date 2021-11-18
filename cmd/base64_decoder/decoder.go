package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/hyperledger-labs/orion-server/pkg/types"
)

var help = "The decoder decodes the base64 encoded value field in the " +
	"GetDataResponseEnvelope. Pass the json output of " +
	"GetDataResponseEnvelope to the `-getresponse` flag"

type GetDataResponseEnvelope struct {
	Response  *GetDataResponse `json:"response,omitempty"`
	Signature []byte           `json:"signature,omitempty"`
}

type GetDataResponse struct {
	Header   *types.ResponseHeader `json:"header,omitempty"`
	Value    string                `json:"value,omitempty"`
	Metadata *types.Metadata       `json:"metadata,omitempty"`
}

func main() {
	getresponse := flag.String(
		"getresponse",
		"",
		"json output of GetDataResponseEnvelope. The value field in the json output will be decoded. Surround the JSON data with single quotes.",
	)

	flag.CommandLine.Output()
	flag.Parse()

	if *getresponse == "" {
		fmt.Println(help)
		flag.PrintDefaults()
		return
	}

	r := &types.GetDataResponseEnvelope{}
	if err := json.Unmarshal([]byte(*getresponse), r); err != nil {
		fmt.Printf("the json data provided for -getresponse flag does not get marshalled to `GetDataResponseEnvelope`, err:%s\n", err.Error())
		return
	}

	decodedResponse := &GetDataResponseEnvelope{
		Response: &GetDataResponse{
			Header:   r.GetResponse().GetHeader(),
			Value:    string(r.GetResponse().GetValue()),
			Metadata: r.GetResponse().GetMetadata(),
		},
		Signature: r.GetSignature(),
	}
	jsonDecodedResponse, err := json.Marshal(decodedResponse)
	if err != nil {
		fmt.Printf("error marshalling decoded response, err:%s\n", err.Error())
		return
	}
	fmt.Printf(string(jsonDecodedResponse))
}

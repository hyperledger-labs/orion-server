package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
)

var help = "all the following two flags must be set. An example command is shown below: \n\n" +
	"  signer -data='{\"userID\":\"admin\"}\" -privatekey=admin.key\n"

func main() {
	pKey := flag.String("privatekey", "", "path to the private key to be used for adding a digital signature")
	data := flag.String("data", "", "json data to be signed. Surround that data with single quotes. An example json data is '{\"userID\":\"admin\"}'")

	flag.CommandLine.Output()
	flag.Parse()

	if *pKey == "" || *data == "" {
		fmt.Println(help)
		flag.PrintDefaults()
		return
	}

	signer, err := crypto.NewSigner(
		&crypto.SignerOptions{
			KeyFilePath: *pKey,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	sign, err := signer.Sign([]byte(*data))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(base64.StdEncoding.EncodeToString(sign))
}

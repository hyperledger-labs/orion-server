package main

import (
	"encoding/base64"
	"flag"
	"fmt"
)

var help = "The data field accepts a JSON or a string value. THE data flag must be set. Two example commands are shown below: \n\n" +
	"  encoder -data='{\"userID\":\"admin\"}' \n" +
	"  encoder -data='value' \n"

func main() {
	data := flag.String("data", "", "json or string data to be encoded. Surround the JSON data with single quotes. An example json data is '{\"userID\":\"admin\"}'")

	flag.CommandLine.Output()
	flag.Parse()

	if *data == "" {
		fmt.Println(help)
		flag.PrintDefaults()
		return
	}

	fmt.Println(base64.StdEncoding.EncodeToString([]byte(*data)))
}

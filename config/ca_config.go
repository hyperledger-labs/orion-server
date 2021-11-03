// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/pkg/errors"
)

// CAConfiguration holds the path to the x509 certificates of the certificate authorities who issues all certificates.
type CAConfiguration struct {
	RootCACertsPath         []string
	IntermediateCACertsPath []string
}

func (c *CAConfiguration) WriteBundle(filePath string) error {
	dirName := path.Dir(filePath)
	err := fileops.CreateDir(dirName)
	if err != nil {
		return errors.Wrapf(err, "error creating directory: %s", dirName)
	}

	bundleFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errors.Wrapf(err, "error while opening file: %s", filePath)
	}
	defer bundleFile.Close()

	for _, caFileName := range c.RootCACertsPath {
		caBytes, err := os.ReadFile(caFileName)
		if err != nil {
			return errors.Wrapf(err, "error while opening file: %s", filePath)
		}
		_, err = bundleFile.Write(caBytes)
		if err != nil {
			return errors.Wrapf(err, "error while writing file: %s", bundleFile.Name())
		}
	}

	for _, caFileName := range c.IntermediateCACertsPath {
		caBytes, err := os.ReadFile(caFileName)
		if err != nil {
			return errors.Wrapf(err, "error while opening file: %s", filePath)
		}
		_, err = bundleFile.Write(caBytes)
		if err != nil {
			return errors.Wrapf(err, "error while writing file: %s", bundleFile.Name())
		}
	}

	return nil
}

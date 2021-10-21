package setup

import (
	"io/ioutil"
	"strings"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// WriteLocalConfig writes the local config object to a YAML file.
// Provide full path with .yml suffix.
func WriteLocalConfig(localConfig *config.LocalConfiguration, localConfigYamlFile string) error {
	return WriteConfigAsYaml(localConfig, localConfigYamlFile)
}

// WriteSharedConfig writes the shared config object to a YAML file.
// Provide full path with .yml suffix.
func WriteSharedConfig(sharedConfig *config.SharedConfiguration, sharedConfigYamlFile string) error {
	return WriteConfigAsYaml(sharedConfig, sharedConfigYamlFile)
}

// WriteConfigAsYaml writes the config object to a YAML file.
// Provide full path with .yml suffix.
func WriteConfigAsYaml(configObject interface{}, configYamlFile string) error {
	if !strings.HasSuffix(configYamlFile, ".yml") {
		return errors.New("file must have '.yml' suffix")
	}

	data, err := yaml.Marshal(configObject)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(configYamlFile, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

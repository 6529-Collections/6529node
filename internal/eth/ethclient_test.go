package eth

import (
	"testing"

	"github.com/6529-Collections/6529node/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestCreateEthClient_Success(t *testing.T) {
	originalConfig := config.Get
	defer func() { config.Get = originalConfig }()

	config.Get = func() config.Config {
		return config.Config{
			EthereumNodeUrl: "http://localhost:8545",
		}
	}

	client, err := createEthClient()
	assert.NoError(t, err)
	assert.NotNil(t, client)
	client.Close()
}

func TestCreateEthClient_EmptyURL(t *testing.T) {
	originalConfig := config.Get
	defer func() { config.Get = originalConfig }()

	config.Get = func() config.Config {
		return config.Config{
			EthereumNodeUrl: "",
		}
	}

	client, err := createEthClient()
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "EthereumNodeUrl is not set")
}

func TestCreateEthClient_InvalidURL(t *testing.T) {
	originalConfig := config.Get
	defer func() { config.Get = originalConfig }()

	config.Get = func() config.Config {
		return config.Config{
			EthereumNodeUrl: "invalid://url",
		}
	}

	client, err := createEthClient()
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "failed to configure Ethereum client")
}

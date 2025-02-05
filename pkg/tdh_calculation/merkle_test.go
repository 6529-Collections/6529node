package tdh_calculation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMerkleRoot(t *testing.T) {
	// test data
	data := []DataItem{
		{Key: "address1", Value: 100},
		{Key: "address2", Value: 200},
		{Key: "address3", Value: 300},
	}

	root := GetMerkleRoot(data)
	assert.NotEmpty(t, root)
	assert.Equal(t, "0x", root[:2])

	expected := "0x9aec8a88f4913421ae198e5852d820cd78c54a2f2f406c7893294e7d74e12fbf"
	assert.Equal(t, expected, root)
}

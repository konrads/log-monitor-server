package util

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	assert.Equal(t, 1, Min(1, 5))
	assert.Equal(t, 1, Min(5, 1))
}

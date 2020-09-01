package worldstate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsSystemDBs(t *testing.T) {
	tests := []struct {
		name     string
		dbName   string
		expected bool
	}{
		{
			name:     "configDB",
			dbName:   ConfigDBName,
			expected: true,
		},
		{
			name:     "DatabasesDB",
			dbName:   DatabasesDBName,
			expected: true,
		},
		{
			name:     "UsersDB",
			dbName:   UsersDBName,
			expected: true,
		},
		{
			name:     "DefaultDB",
			dbName:   DefaultDBName,
			expected: true,
		},
		{
			name:     "non-system DB",
			dbName:   "random",
			expected: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, IsSystemDB(tt.dbName))
		})
	}
}

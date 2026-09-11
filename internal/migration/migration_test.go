package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRange_String(t *testing.T) {
	r := Range{
		Start: "a",
		End:   "z",
	}
	assert.Equal(t, "a", r.Start)
	assert.Equal(t, "z", r.End)
}

func TestMigrationRequest_Fields(t *testing.T) {
	req := MigrationRequest{
		FromShard: "A",
		ToShard:   "B",
		KeyRange: Range{
			Start: "key-start",
			End:   "key-end",
		},
	}
	assert.Equal(t, "A", req.FromShard)
	assert.Equal(t, "B", req.ToShard)
	assert.Equal(t, "key-start", req.KeyRange.Start)
	assert.Equal(t, "key-end", req.KeyRange.End)
}

func TestMigrationResult_Success(t *testing.T) {
	result := MigrationResult{
		Success:   true,
		Message:   "Migration completed successfully",
		KeysCount: 42,
	}
	assert.True(t, result.Success)
	assert.Equal(t, "Migration completed successfully", result.Message)
	assert.Equal(t, 42, result.KeysCount)
}

func TestMigrationResult_Failure(t *testing.T) {
	result := MigrationResult{
		Success: false,
		Message: "Migration failed due to verification error",
	}
	assert.False(t, result.Success)
	assert.Equal(t, "Migration failed due to verification error", result.Message)
	assert.Equal(t, 0, result.KeysCount)
}

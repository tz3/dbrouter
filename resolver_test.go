package dbrouter

import (
	"database/sql"
	"testing"
)

// TestWrapDBs is a table-driven test that verifies the behavior of WrapDBs function.
func TestWrapDBs(t *testing.T) {
	// Create mock sql.DB instances
	rwDB := &sql.DB{}  // Simulating RW DB (primary)
	roDB1 := &sql.DB{} // Simulating RO DB 1
	roDB2 := &sql.DB{} // Simulating RO DB 2

	// Define test cases
	tests := []struct {
		name            string
		input           []*sql.DB
		expectPanic     bool
		expectedRWDB    *sql.DB
		expectedROCount int
	}{
		{
			name:            "Valid RW and two RO connections",
			input:           []*sql.DB{rwDB, roDB1, roDB2},
			expectPanic:     false,
			expectedRWDB:    rwDB,
			expectedROCount: 2,
		},
		{
			name:            "Valid RW with no RO connections",
			input:           []*sql.DB{rwDB},
			expectPanic:     false,
			expectedRWDB:    rwDB,
			expectedROCount: 0,
		},
		{
			name:        "No connections provided (expect panic)",
			input:       []*sql.DB{},
			expectPanic: true,
		},
	}

	// Iterate through test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expectPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic but no panic occurred")
					}
				}()
				// Call WrapDBs with inputs that are expected to cause a panic
				WrapDBs(tc.input...)
			} else {
				// Call WrapDBs with valid input
				db := WrapDBs(tc.input...)

				// Assert RW DB is correct
				if db.readWriteDB != tc.expectedRWDB {
					t.Errorf("Expected RW DB to be %v, got %v", tc.expectedRWDB, db.readWriteDB)
				}

				// Assert the number of RO DBs is correct
				if len(db.readOnlyDBs) != tc.expectedROCount {
					t.Errorf("Expected %d RO DBs, got %d", tc.expectedROCount, len(db.readOnlyDBs))
				}
			}
		})
	}
}

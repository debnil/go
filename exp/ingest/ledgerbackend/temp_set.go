package ledgerbackend

// TempSet is an interface that must be implemented by stores that
// hold temporary set of objects for state reader. The implementation
// does not need to be thread-safe.
//
// TODO: This is currently duplicated in `io` to avoid circular imports.
// This should likely be pulled out into a different package altogether.
type TempSet interface {
	Open() error
	// Preload batch-loads keys into internal cache (if a store has any) to
	// improve execution time by removing many round-trips.
	Preload(keys []string) error
	// Add adds key to the store.
	Add(key string) error
	// Exist returns value true if the value is found in the store.
	// If the value has not been set, it should return false.
	Exist(key string) (bool, error)
	Close() error
}

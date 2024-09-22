package dbrouter

// doConcurrent runs a function concurrently n times and collects any errors.
// It returns the last non-nil error, if any.
func doConcurrent(n int, fn func(i int) error) error {
	errors := make(chan error, n) // Channel to collect errors.

	for i := 0; i < n; i++ {
		go func(i int) { errors <- fn(i) }(i) // Launch goroutines to execute fn.
	}

	var err error
	for i := 0; i < n; i++ {
		if e := <-errors; e != nil { // Collect errors from all goroutines.
			err = e
		}
	}

	return err // Return the last non-nil error, or nil if no errors.
}

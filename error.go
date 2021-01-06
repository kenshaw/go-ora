package ora

// Error is a ora package error.
type Error struct {
}

// Error satisfies the error interface.
func (err *Error) Error() string {
	return ""
}

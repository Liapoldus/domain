package models

// ProductError is a business failure of one product peer method. It is
// returned in the data payload of the response envelope, never as a peer
// protocol error.
type ProductError struct {
	Code           string
	Retryable      bool
	UnknownOutcome bool
	Message        string
}

func (err *ProductError) Error() string {
	if err == nil || err.Message == "" {
		return "domain product call failed"
	}
	return err.Message
}

// Scope is the trusted tenant, site and group a product caller is mapped to
// by its authenticated identity. The request payloads carry no tenant/site or
// group; the scope IS the authority for the call.
type Scope struct {
	Tenant string
	Site   string
	Group  string
}

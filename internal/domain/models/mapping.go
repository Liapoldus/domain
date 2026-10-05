package models

type Mapping struct {
	Entity string `json:"entity"`
	From   string `json:"from"`
	To     string `json:"to"`
	// Conversion is a named, versioned and deterministic conversion registered
	// by the product. A missing conversion never implies a lossy cast.
	Conversion string `json:"conversion,omitempty"`
}

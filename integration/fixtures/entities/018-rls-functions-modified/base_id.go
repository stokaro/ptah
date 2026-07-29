package entities

// BaseID represents a common ID structure that can be embedded in other entities
type BaseID struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

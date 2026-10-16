package sqlcontract

import "encoding/json"

// Vector is one conformance case for a client of the failure contract: a
// published code, the status the platform answers it with, and the evidence the
// reply carries.
type Vector struct {
	Name      string
	Code      string
	Status    int
	Completed *bool
	Committed *bool
}

func flag(value bool) *bool { return &value }

// Vectors returns the conformance kit: every published code crossed with every
// evidence shape the gateway can produce and every status the platform answers a
// failure with. Every consumer's matrix test iterates this table, so a code
// added to the vocabulary extends every consumer's coverage at once — the check
// the three review rounds of this contract did not have.
func Vectors() []Vector {
	statuses := []int{400, 403, 409, 413, 429, 500, 502, 503, 504}
	shapes := []struct {
		name      string
		completed *bool
		committed *bool
	}{
		{"no-evidence", nil, nil},
		{"committed-only", nil, flag(true)},
		{"settled-committing", flag(true), flag(true)},
		{"settled-non-committing", flag(true), flag(false)},
		{"rejected", flag(false), flag(false)},
		{"contradictory", flag(false), flag(true)},
	}
	vectors := make([]Vector, 0, len(vocabulary)*len(statuses)*len(shapes))
	for _, code := range Codes() {
		for _, status := range statuses {
			for _, shape := range shapes {
				vectors = append(vectors, Vector{
					Name:      code + "/" + shape.name + "@" + json.Number(itoa(status)).String(),
					Code:      code,
					Status:    status,
					Completed: shape.completed,
					Committed: shape.committed,
				})
			}
		}
	}
	return vectors
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [12]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}

// ErrorBody renders the vector as the failure object a client receives.
func (vector Vector) ErrorBody() []byte {
	fields := map[string]any{"code": vector.Code}
	if vector.Completed != nil {
		fields["execution_completed"] = *vector.Completed
	}
	if vector.Committed != nil {
		fields["may_have_committed"] = *vector.Committed
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return encoded
}

// Body renders the vector as the whole reply a client receives, so a consumer
// test can feed the same bytes to the code the client really runs.
func (vector Vector) Body() []byte {
	return []byte(`{"version":1,"request_id":"vector","ok":false,"result":null,"error":` + string(vector.ErrorBody()) + `}`)
}

// Failure decodes the vector through the authority, which is the answer every
// consumer must reach.
func (vector Vector) Failure() Failure {
	var failure Failure
	if err := json.Unmarshal(vector.ErrorBody(), &failure); err != nil {
		panic(err)
	}
	return failure
}

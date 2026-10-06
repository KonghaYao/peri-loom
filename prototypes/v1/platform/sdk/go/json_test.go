package sdk

import "testing"

func TestStrictJSONResponseValidation(test *testing.T) {
	for _, payload := range []string{`{"ok":false,"ok":true}`, `{"nested":{"key":1,"key":2}}`, `{"\u006bey":1,"key":2}`, `{} {}`, "{\"key\":\"\xff\"}"} {
		if validateJSON([]byte(payload)) == nil {
			test.Fatal("accepted malformed or ambiguous JSON")
		}
	}
	if err := validateJSON([]byte(`{"nested":[{"key":1},null,9223372036854775807]}`)); err != nil {
		test.Fatal(err)
	}
}

package platform

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// The digest names its own construction so scheme and bytes cannot drift apart,
// and a reader can tell what a stored digest covers without a second field to
// keep in sync. A bare 64-hex value is the legacy domain: written before the
// scheme existed, recomputable from nothing, comparable to nothing, and never
// usable as evidence or as a key. Any other scheme is refused instead of being
// reinterpreted, so a future construction never loads under today's rules.
const commandDigestScheme = "logical_command_v1"

const commandDigestPrefix = commandDigestScheme + ":"

var errCommandNotEncodable = errors.New("operation command not encodable")

// frozenCommand is the one final logical command of a dispatch. Its fields are
// assembled and normalized before the durable record is registered, and from
// there on only its bytes travel, so no later code path can change what that
// record's digest covers: no mutable field survives the freeze.
type frozenCommand struct {
	operation string
	canonical []byte
}

// MarshalJSON makes the frozen bytes the JSON body of the command, so the
// generic upstream path and the fixtures encode a frozen command to exactly the
// bytes its digest covers.
func (frozen frozenCommand) MarshalJSON() ([]byte, error) {
	if len(frozen.canonical) == 0 {
		return nil, errCommandNotEncodable
	}
	return frozen.canonical, nil
}

// bytes is the body the worker receives.
func (frozen frozenCommand) bytes() []byte { return frozen.canonical }

// operationName is the worker command name inside the digested bytes, so the
// name a caller compares and the name the digest covers are one string.
func (frozen frozenCommand) operationName() string { return frozen.operation }

// digest is the logical command identity, and it covers the command alone. The
// {version, request_id} envelope, the worker and binding placement picked and
// the caller's deadline stay outside it: a retry that changes any of them still
// authorizes the same command, and a command sent twice under two request IDs
// keeps one identity.
func (frozen frozenCommand) digest() string { return commandDigest(frozen.canonical) }

func commandDigest(canonical []byte) string {
	hash := sha256.New()
	hash.Write([]byte(commandDigestScheme))
	hash.Write([]byte("\n"))
	hash.Write(canonical)
	return commandDigestPrefix + hex.EncodeToString(hash.Sum(nil))
}

// freezeCommand encodes the complete logical command of a dispatch exactly once,
// after its arguments were normalized, and reads the command name back out of
// those bytes.
func freezeCommand(fields map[string]any) (frozenCommand, error) {
	if err := normalizeCommandArguments(fields); err != nil {
		return frozenCommand{}, err
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return frozenCommand{}, errCommandNotEncodable
	}
	operation, _ := frozenOperation(encoded)
	if operation == "" {
		return frozenCommand{}, errCommandNotEncodable
	}
	return frozenCommand{operation: operation, canonical: encoded}, nil
}

// freezeCommandValue freezes a command that did not come through the dispatch
// switch. beginOperation registers hand-built fixtures, and those carry no
// client-chosen argument spelling; a nil command freezes as the JSON literal
// null, exactly as it hashed before. Production registers only through
// registerOperation, which takes an already frozen command, so nothing in
// service reaches the worker through this tolerance.
func freezeCommandValue(command any) (frozenCommand, error) {
	if frozen, ok := command.(frozenCommand); ok {
		return frozen, nil
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return frozenCommand{}, errCommandNotEncodable
	}
	operation, _ := frozenOperation(encoded)
	return frozenCommand{operation: operation, canonical: encoded}, nil
}

func frozenOperation(encoded []byte) (string, bool) {
	var envelope struct {
		Command string `json:"command"`
	}
	if json.Unmarshal(encoded, &envelope) != nil || envelope.Command == "" {
		return "", false
	}
	return envelope.Command, true
}

// normalizeCommandArguments rewrites the bind arguments of a command into one
// canonical spelling. The client chooses the JSON spelling of a bound value
// while the worker reads the value, so a digest over the spelling would report
// two logical commands for one execution: "007" and "7" bind the same integer.
func normalizeCommandArguments(fields map[string]any) error {
	if present, exists := fields["args"]; exists {
		args, ok := present.([]sqlArgument)
		if !ok {
			return errCommandNotEncodable
		}
		normalized, err := normalizeSQLArguments(args)
		if err != nil {
			return err
		}
		fields["args"] = normalized
	}
	if present, exists := fields["named_args"]; exists {
		named, ok := present.([]namedSQLArgument)
		if !ok {
			return errCommandNotEncodable
		}
		normalized := make([]namedSQLArgument, len(named))
		for index, argument := range named {
			value, err := normalizeSQLArgument(argument.Value)
			if err != nil {
				return err
			}
			normalized[index] = namedSQLArgument{Name: argument.Name, Value: value}
		}
		fields["named_args"] = normalized
	}
	return nil
}

func normalizeSQLArguments(args []sqlArgument) ([]sqlArgument, error) {
	normalized := make([]sqlArgument, len(args))
	for index, argument := range args {
		value, err := normalizeSQLArgument(argument)
		if err != nil {
			return nil, err
		}
		normalized[index] = value
	}
	return normalized, nil
}

// normalizeSQLArgument mirrors the value the worker binds: an integer becomes
// its i64 decimal text, a float becomes the shortest form that parses back to
// the same f64, text is re-encoded from the decoded string, and a blob keeps its
// strict standard base64. The error carries no argument text: a command that
// cannot be canonicalized must not reach a log or a durable record.
func normalizeSQLArgument(argument sqlArgument) (sqlArgument, error) {
	switch argument.Type {
	case "null":
		if len(argument.Value) != 0 || argument.Base64 != "" {
			return argument, errCommandNotEncodable
		}
		return sqlArgument{Type: "null"}, nil
	case "integer":
		var text string
		if json.Unmarshal(argument.Value, &text) != nil || argument.Base64 != "" {
			return argument, errCommandNotEncodable
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return argument, errCommandNotEncodable
		}
		encoded, err := json.Marshal(strconv.FormatInt(value, 10))
		if err != nil {
			return argument, errCommandNotEncodable
		}
		return sqlArgument{Type: "integer", Value: encoded}, nil
	case "float":
		var value float64
		if json.Unmarshal(argument.Value, &value) != nil || argument.Base64 != "" {
			return argument, errCommandNotEncodable
		}
		// A JSON float that parses at all is finite, so 'g' with the shortest
		// round-trip precision stays a valid JSON number.
		return sqlArgument{Type: "float", Value: json.RawMessage(strconv.FormatFloat(value, 'g', -1, 64))}, nil
	case "text":
		var text string
		if json.Unmarshal(argument.Value, &text) != nil || argument.Base64 != "" {
			return argument, errCommandNotEncodable
		}
		encoded, err := json.Marshal(text)
		if err != nil {
			return argument, errCommandNotEncodable
		}
		return sqlArgument{Type: "text", Value: encoded}, nil
	case "blob":
		if len(argument.Value) != 0 {
			return argument, errCommandNotEncodable
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(argument.Base64)
		if err != nil {
			return argument, errCommandNotEncodable
		}
		return sqlArgument{Type: "blob", Base64: base64.StdEncoding.EncodeToString(decoded)}, nil
	}
	return argument, errCommandNotEncodable
}

// validCommandDigest accepts the value domains a stored record may carry: the
// current scheme, or the bare digest written before the scheme existed.
func validCommandDigest(value string) bool {
	return taggedCommandDigest(value) || legacyCommandDigest(value)
}

// taggedCommandDigest accepts only the current scheme. The write path uses it: a
// new record whose digest could never be recomputed would be unusable the moment
// a consumer needs it, so a writer never starts a record in the legacy domain.
func taggedCommandDigest(value string) bool {
	encoded, tagged := strings.CutPrefix(value, commandDigestPrefix)
	if !tagged || len(encoded) != 64 {
		return false
	}
	for _, character := range []byte(encoded) {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// legacyCommandDigest accepts an untagged digest of the width the previous
// version wrote, and nothing else: a record carrying any other scheme is
// rejected at load instead of being read under rules it never promised to obey.
func legacyCommandDigest(value string) bool {
	if strings.Contains(value, ":") {
		return false
	}
	encoded, err := hex.DecodeString(value)
	return err == nil && len(encoded) == 32
}

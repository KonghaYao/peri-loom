package platform

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
)

func NewAPIKey(store StateStore, subject, project, role string, serviceAccount bool) (APIKey, string, error) {
	if store == nil || !subjectPattern.MatchString(subject) || !identifierPattern.MatchString(project) || !validRole(role) {
		return APIKey{}, "", errors.New("invalid API key identity")
	}
	id, err := randomLabel()
	if err != nil {
		return APIKey{}, "", err
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return APIKey{}, "", errors.New("secure key generation failed")
	}
	credential := "plk1_" + id + "_" + base64.RawURLEncoding.EncodeToString(random[:])
	key := APIKey{ID: id, Subject: subject, Project: project, Role: role, Digest: digestHex([]byte(credential)), ServiceAccount: serviceAccount}
	err = store.Update(func(state *State) error {
		if _, ok := state.Projects[project]; !ok {
			return errors.New("API key project missing")
		}
		if _, ok := state.APIKeys[id]; ok {
			return errors.New("API key identifier collision")
		}
		state.APIKeys[id] = key
		return nil
	})
	if err != nil {
		return APIKey{}, "", err
	}
	return key, credential, nil
}

func RevokeAPIKey(store StateStore, id string) error {
	if store == nil || !identifierPattern.MatchString(id) {
		return errors.New("invalid API key identifier")
	}
	return store.Update(func(state *State) error {
		key, ok := state.APIKeys[id]
		if !ok {
			return errors.New("API key missing")
		}
		key.Revoked = true
		state.APIKeys[id] = key
		return nil
	})
}

func BootstrapAPIKey(store StateStore, credential string) error {
	if store == nil || !validCredential(credential) || strings.HasPrefix(credential, "pldb1.") {
		return errors.New("strong bootstrap credential required")
	}
	id, err := randomLabel()
	if err != nil {
		return err
	}
	return store.Update(func(state *State) error {
		if len(state.APIKeys) != 0 {
			return errors.New("bootstrap requires empty API key registry")
		}
		state.APIKeys[id] = APIKey{ID: id, Subject: "bootstrap", Project: "*", Role: "admin", Digest: digestHex([]byte(credential))}
		return nil
	})
}

package nomad

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	// note: this is aliased so that it's more noticeable if someone
	// accidentally swaps it out for math/rand via running goimports
	cryptorand "crypto/rand"

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/structs"
)

type Encrypter struct {
	ciphers      map[string]cipher.AEAD // map of key IDs to ciphers
	keystorePath string
}

func NewEncrypter() *Encrypter {
	// TODO
	err := os.MkdirAll("/var/nomad/server/keystore", 0700)
	if err != nil {
		panic(err) // TODO
	}

	encrypter, err := encrypterFromKeystore("/var/nomad/server/keystore")
	if err != nil {
		panic(err) // TODO
	}

	return encrypter
}

func validateKeyFromStore(rootKey *api.RootKey, id string) error {
	// TODO: how much of these can we reuse from (*Keyring).validateUpdate ?
	if rootKey == nil {
		return fmt.Errorf("root key envelope is missing")
	}
	if rootKey.Meta == nil {
		return fmt.Errorf("root key metadata is required")
	}
	if rootKey.Meta.KeyID == "" || rootKey.Meta.KeyID != id {
		return fmt.Errorf("root key UUID is required and must match key file")
	}
	if rootKey.Meta.Algorithm == "" {
		return fmt.Errorf("algorithm is required")
	}
	return nil
}

func encrypterFromKeystore(keystoreDirectory string) (*Encrypter, error) {

	ciphers := make(map[string]cipher.AEAD)

	loadKeyFromPath := func(path string) error {

		// skip over non-key files; they shouldn't be here but there's
		// no reason to fail startup for it if the administrator has
		// left something there
		if filepath.Ext(path) != ".json" {
			return nil
		}
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		if !helper.IsUUID(id) {
			return nil
		}

		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var rootKey *api.RootKey
		if err := json.Unmarshal(raw, rootKey); err != nil {
			return err
		}
		if err := validateKeyFromStore(rootKey, id); err != nil {
			return err
		}

		key := make([]byte, base64.StdEncoding.DecodedLen(len(rootKey.Key)))
		_, err = base64.StdEncoding.Decode(key, []byte(rootKey.Key))
		if err != nil {
			return fmt.Errorf("could not decode key: %v", err)
		}

		switch rootKey.Meta.Algorithm {
		case api.EncryptionAlgorithmAES256GCM:
			block, err := aes.NewCipher(key)
			if err != nil {
				return fmt.Errorf("could not create cipher: %v", err)
			}
			aead, err := cipher.NewGCM(block)
			if err != nil {
				return fmt.Errorf("could not create cipher: %v", err)
			}
			ciphers[rootKey.Meta.KeyID] = aead
		case api.EncryptionAlgorithmXChaCha20:
			aead, err := chacha20poly1305.NewX(key)
			if err != nil {
				return fmt.Errorf("could not create cipher: %v", err)
			}
			ciphers[rootKey.Meta.KeyID] = aead
		}

		return nil
	}

	err := filepath.Walk(keystoreDirectory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("could not read path %s from keystore: %v", path, err)
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		err = loadKeyFromPath(path)
		if err != nil {
			return fmt.Errorf("could not load key file %s from keystore: %v", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Encrypter{
		ciphers:      ciphers,
		keystorePath: keystoreDirectory,
	}, nil
}

// Encrypt takes the serialized map[string][]byte from
// SecureVariable.UnencryptedData, generates an appropriately-sized nonce
// for the algorithm, and encrypts the data with the ciper for the
// CurrentRootKeyID. The buffer returned includes the nonce.
func (e *Encrypter) Encrypt(unencryptedData []byte, keyID string) []byte {
	// TODO: actually encrypt!
	return unencryptedData
}

// Decrypt takes an encrypted buffer and then root key ID. It extracts
// the nonce, decrypts the content, and returns the cleartext data.
func (e *Encrypter) Decrypt(encryptedData []byte, keyID string) ([]byte, error) {
	// TODO: actually decrypt!
	return encryptedData, nil
}

// GenerateNewRootKey returns a new root key and its metadata.
func (e *Encrypter) GenerateNewRootKey(algorithm structs.EncryptionAlgorithm) (*structs.RootKey, error) {
	meta := structs.NewRootKeyMeta()
	meta.Algorithm = algorithm

	rootKey := &structs.RootKey{
		Meta: meta,
	}

	switch algorithm {
	case structs.EncryptionAlgorithmAES256GCM:
		key := make([]byte, 32)
		if _, err := cryptorand.Read(key); err != nil {
			return nil, err
		}
		rootKey.Key = key

	case structs.EncryptionAlgorithmXChaCha20:
		key := make([]byte, chacha20poly1305.KeySize)
		if _, err := cryptorand.Read(key); err != nil {
			return nil, err
		}
		rootKey.Key = key
	}

	return rootKey, nil
}

func (e *Encrypter) PersistRootKey(rootKey *structs.RootKey) error {
	buf, err := json.Marshal(rootKey)
	if err != nil {
		return err
	}
	path := filepath.Join(e.keystorePath, rootKey.Meta.KeyID+".json")
	err = os.WriteFile(path, buf, 0700)
	if err != nil {
		return err
	}
	return nil
}

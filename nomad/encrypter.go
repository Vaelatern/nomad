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
	"sync"

	// note: this is aliased so that it's more noticeable if someone
	// accidentally swaps it out for math/rand via running goimports

	"golang.org/x/crypto/chacha20poly1305"

	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/structs"
)

// Encrypter is the keyring for secure variables.
type Encrypter struct {
	lock         sync.RWMutex
	keys         map[string]*structs.RootKey // map of key IDs to key material
	ciphers      map[string]cipher.AEAD      // map of key IDs to ciphers
	keystorePath string
}

// NewEncrypter loads or creates a new local keystore and returns an
// encryption keyring with the keys it finds.
func NewEncrypter(keystorePath string) (*Encrypter, error) {
	err := os.MkdirAll(keystorePath, 0700)
	if err != nil {
		return nil, err
	}
	encrypter, err := encrypterFromKeystore(keystorePath)
	if err != nil {
		return nil, err
	}
	return encrypter, nil
}

func encrypterFromKeystore(keystoreDirectory string) (*Encrypter, error) {

	encrypter := &Encrypter{
		ciphers:      make(map[string]cipher.AEAD),
		keys:         make(map[string]*structs.RootKey),
		keystorePath: keystoreDirectory,
	}

	err := filepath.Walk(keystoreDirectory, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("could not read path %s from keystore: %v", path, err)
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
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
		key, err := encrypter.LoadKeyFromStore(path)
		if err != nil {
			return fmt.Errorf("could not load key file %s from keystore: %v", path, err)
		}
		if key.Meta.KeyID != id {
			return fmt.Errorf("root key ID %s must match key file %s", key.Meta.KeyID, path)
		}

		err = encrypter.AddKey(key)
		if err != nil {
			return fmt.Errorf("could not add key file %s to keystore: %v", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return encrypter, nil
}

// Encrypt takes the serialized map[string][]byte from
// SecureVariable.UnencryptedData, generates an appropriately-sized nonce
// for the algorithm, and encrypts the data with the ciper for the
// CurrentRootKeyID. The buffer returned includes the nonce.
func (e *Encrypter) Encrypt(unencryptedData []byte, keyID string) []byte {
	e.lock.RLock()
	defer e.lock.RUnlock()

	// TODO: actually encrypt!
	return unencryptedData
}

// Decrypt takes an encrypted buffer and then root key ID. It extracts
// the nonce, decrypts the content, and returns the cleartext data.
func (e *Encrypter) Decrypt(encryptedData []byte, keyID string) ([]byte, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	// TODO: actually decrypt!
	return encryptedData, nil
}

// AddKey stores the key in the keyring and creates a new cipher for it.
func (e *Encrypter) AddKey(rootKey *structs.RootKey) error {

	if rootKey.Meta == nil {
		return fmt.Errorf("missing metadata")
	}
	var aead cipher.AEAD
	var err error

	switch rootKey.Meta.Algorithm {
	case structs.EncryptionAlgorithmAES256GCM:
		block, err := aes.NewCipher(rootKey.Key)
		if err != nil {
			return fmt.Errorf("could not create cipher: %v", err)
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return fmt.Errorf("could not create cipher: %v", err)
		}
	case structs.EncryptionAlgorithmXChaCha20:
		aead, err = chacha20poly1305.NewX(rootKey.Key)
		if err != nil {
			return fmt.Errorf("could not create cipher: %v", err)
		}
	default:
		return fmt.Errorf("invalid algorithm %s", rootKey.Meta.Algorithm)
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	e.ciphers[rootKey.Meta.KeyID] = aead
	e.keys[rootKey.Meta.KeyID] = rootKey
	return nil
}

// GetKey retrieves the key material by ID from the keyring
func (e *Encrypter) GetKey(keyID string) ([]byte, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()

	key, ok := e.keys[keyID]
	if !ok {
		return []byte{}, fmt.Errorf("no such key %s in keyring", keyID)
	}
	return key.Key, nil
}

// RemoveKey removes a key by ID from the keyring
func (e *Encrypter) RemoveKey(keyID string) error {
	// TODO: should the server remove the serialized file here?
	// TODO: given that it's irreverislbe, should the server *ever*
	// remove the serialized file?
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.ciphers, keyID)
	delete(e.keys, keyID)
	return nil
}

// SaveKeyToStore serializes a root key to the on-disk keystore.
func (e *Encrypter) SaveKeyToStore(rootKey *structs.RootKey) error {
	buf, err := json.Marshal(rootKey)
	if err != nil {
		return err
	}
	path := filepath.Join(e.keystorePath, rootKey.Meta.KeyID+".json")
	err = os.WriteFile(path, buf, 0600)
	if err != nil {
		return err
	}
	return nil
}

// LoadKeyFromStore deserializes a root key from disk.
func (e *Encrypter) LoadKeyFromStore(path string) (*structs.RootKey, error) {

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	storedKey := &struct {
		Meta *structs.RootKeyMetaStub
		Key  string
	}{}
	var rootKey *structs.RootKey
	if err := json.Unmarshal(raw, storedKey); err != nil {
		return nil, err
	}
	meta := &structs.RootKeyMeta{
		Active:     storedKey.Meta.Active,
		KeyID:      storedKey.Meta.KeyID,
		Algorithm:  storedKey.Meta.Algorithm,
		CreateTime: storedKey.Meta.CreateTime,
	}
	if err = meta.Validate(); err != nil {
		return nil, err
	}

	key := make([]byte, base64.StdEncoding.DecodedLen(len(rootKey.Key)))
	_, err = base64.StdEncoding.Decode(key, []byte(rootKey.Key))
	if err != nil {
		return nil, fmt.Errorf("could not decode key: %v", err)
	}

	return &structs.RootKey{
		Meta: meta,
		Key:  key,
	}, nil

}

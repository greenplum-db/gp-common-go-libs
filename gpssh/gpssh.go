package gpssh

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gpfs"
	"golang.org/x/crypto/ssh"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

func init() {
	Default()
}

type User string

const (
	Gpadmin User = "gpadmin"
	Root    User = "root"
)

var Dependency struct {
	CloseSession                  func(session *ssh.Session)
	CreateClientConnection        func(address string, username User, password string) (SshClient, error)
	Dial                          func(network, addr string, config *ssh.ClientConfig) (*ssh.Client, error)
	FileSystem                    gpfs.GpFs
	GetPasswordAuthMethod         func(secret string) ssh.AuthMethod
	GetPublicKey                  func(path string) (ssh.AuthMethod, error)
	GetSshSigner                  func(pem string) (ssh.Signer, error)
	InsecureIgnoreHostKey         func() ssh.HostKeyCallback
	ParsePrivateKey               func(pemBytes []byte) (ssh.Signer, error)
	ParsePrivateKeyWithPassphrase func(pemBytes, passphrase []byte) (ssh.Signer, error)
	PublicKeys                    func(key ...ssh.Signer) ssh.AuthMethod
	RunSshCommand                 func(session *ssh.Session, cmd string) error
}

func Default() {
	Dependency.CloseSession = closeSession
	Dependency.CreateClientConnection = CreateClientConnection
	Dependency.Dial = ssh.Dial
	Dependency.FileSystem = gpfs.New()
	Dependency.GetPasswordAuthMethod = ssh.Password
	Dependency.GetPublicKey = GetPublicKey
	Dependency.GetSshSigner = GetSshSigner
	Dependency.InsecureIgnoreHostKey = ssh.InsecureIgnoreHostKey
	Dependency.ParsePrivateKey = ssh.ParsePrivateKey
	Dependency.ParsePrivateKeyWithPassphrase = ssh.ParsePrivateKeyWithPassphrase
	Dependency.PublicKeys = ssh.PublicKeys
	Dependency.RunSshCommand = runSshCommand
}

func NewGpSshClient(address string, username User, keyPath string) GpSshClient {
	return &GpSshClientImpl{
		Address: address,
		User:    username,
		KeyPath: keyPath,
	}
}

//counterfeiter:generate . GpSshClient
type GpSshClient interface {
	RunSshCommand(command string) ([]byte, error)
}

type GpSshClientImpl struct {
	Address string
	KeyPath string
	User
}

func (s GpSshClientImpl) RunSshCommand(cmd string) ([]byte, error) {
	client, err := Dependency.CreateClientConnection(s.Address, s.User, s.KeyPath)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS session on VM %s: %w", s.Address, err)
	}
	defer Dependency.CloseSession(session)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	if err := Dependency.RunSshCommand(session, cmd); err != nil {
		if stderr.Len() > 0 {
			return nil, fmt.Errorf("%w: %s", err, stderr.String())
		}
		return nil, err
	}

	return stdout.Bytes(), nil
}

func CreateClientConnection(address string, username User, keyPath string) (SshClient, error) {
	authMethod, err := Dependency.GetPublicKey(keyPath)
	if err != nil {
		return nil, err
	}

	config := &ssh.ClientConfig{
		User: string(username),
		Auth: []ssh.AuthMethod{
			authMethod,
		},
		HostKeyCallback: Dependency.InsecureIgnoreHostKey(),
	}

	client, err := Dependency.Dial("tcp", fmt.Sprintf("%s:22", address), config)
	if err != nil {
		return nil, fmt.Errorf("failed to login to VM %s: %w", address, err)
	}

	return client, nil
}

func GetPublicKey(path string) (ssh.AuthMethod, error) {
	b, err := Dependency.FileSystem.Read(path)
	if err != nil {
		return nil, err
	}

	sshSigner, err := Dependency.GetSshSigner(b)
	if err != nil {
		return nil, err
	}

	return Dependency.PublicKeys(sshSigner), nil
}

//counterfeiter:generate . GpSshSigner
type GpSshSigner ssh.Signer

func GetSshSigner(pem string) (ssh.Signer, error) {
	if strings.Contains(pem, "ENCRYPTED") {
		return Dependency.ParsePrivateKeyWithPassphrase([]byte(pem), []byte{})
	} else {
		return Dependency.ParsePrivateKey([]byte(pem))
	}
}

//counterfeiter:generate . SshClient
type SshClient interface {
	Close() error
	NewSession() (*ssh.Session, error)
}

//counterfeiter:generate . SshSession
type SshSession interface {
	Run(cmd string) error
	Close() error
	SetStdout(out io.Writer)
	SetStderr(err io.Writer)
}

func closeSession(session *ssh.Session) {
	defer session.Close()
}

func runSshCommand(session *ssh.Session, cmd string) error {
	return session.Run(cmd)
}

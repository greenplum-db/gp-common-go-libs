package ssh

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	error "github.com/greenplum-db/gp-common-go-libs/error"
	ui "github.com/greenplum-db/gp-common-go-libs/ui"
	vfs "github.com/greenplum-db/gp-common-go-libs/vfs"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

//go:generate go run github.com/maxbrunsfeld/counterfeiter/v6 -generate

//counterfeiter:generate . SshClient
type SshClient interface {
	ChangeLoginPassword(username User, newPassword string) error
	GetAddress() string
	GetPassword() string
	GetUsername() User
	RunSshCommand(cmd string) error
	RunSshCommandReturnOutput(cmd string) (string, string, error)
	SecurelyCopyFile(sourcePath string, destPath string) error
}

type User string

func init() {
	Default()
}

const (
	Gpadmin User = "gpadmin"
	Root    User = "root"
)

var Dependency struct {
	ChangeUserPassword               func(sshClient SshClient, username User, password string) error
	CloseSession                     func(session *ssh.Session)
	CreateClientConnection           func(address string, username User, password string) (SshClient, error)
	CreateClientConnectionImpl       func(address string, username User, authMethod ssh.AuthMethod) (SshClient, error)
	CreateSftpFile                   func(sftpClient SftpClient, path string) (SftpFile, error)
	CreateSftpDir                    func(client SftpClient, path string) error
	Dial                             func(network, addr string, config *ssh.ClientConfig) (*ssh.Client, error)
	GetChecksum                      func(file string) (string, error)
	GetUi                            func() ui.Ui
	GetKeyboardInteractiveAuthMethod func(newPassword string) ssh.AuthMethod
	GetPasswordAuthMethod            func(secret string) ssh.AuthMethod
	InsecureIgnoreHostKey            func() ssh.HostKeyCallback
	KeyboardInteractive              func(challenge ssh.KeyboardInteractiveChallenge) ssh.AuthMethod
	MatchRemoteChecksumPreCopy       func(sshClient SshClient, fullPath string, checksum string) (bool, error)
	MatchRemoteChecksumPostCopy      func(sshClient SshClient, fullPath string, checksum string) (bool, error)
	NewChangePasswordChallengeFunc   func(newPassword string) ssh.KeyboardInteractiveChallenge
	NewError                         func(errorCode constants.ErrorCode, args ...any) error.Error
	NewSftpClient                    func(sshClient SshClient) (SftpClient, error)
	OpenFile                         func(fullPath string, flag int, perm os.FileMode) (vfs.GpvFile, error)
	ReadFile                         func(filePath string) ([]byte, error)
	RunSshCommand                    func(session *ssh.Session, cmd string) error
	RunSshCommandWithOutput          func(g SshClient, cmd string, outWriter io.Writer, errWriter io.Writer) error
	SecurelyCopyFile                 func(g SshClient, sourcePath string, destPath string) error
}

func Default() {
	Dependency.ChangeUserPassword = ChangeUserPassword
	Dependency.CloseSession = closeSession
	Dependency.CreateClientConnection = CreateClientConnection
	Dependency.CreateClientConnectionImpl = CreateClientConnectionImpl
	Dependency.Dial = ssh.Dial
	Dependency.CreateSftpFile = createSftpFile
	Dependency.CreateSftpDir = createSftpDir
	Dependency.GetChecksum = vfs.GetChecksum
	Dependency.GetUi = ui.GetUi
	Dependency.GetKeyboardInteractiveAuthMethod = GetKeyboardInteractiveAuthMethod
	Dependency.GetPasswordAuthMethod = ssh.Password
	Dependency.InsecureIgnoreHostKey = ssh.InsecureIgnoreHostKey
	Dependency.KeyboardInteractive = ssh.KeyboardInteractive
	Dependency.MatchRemoteChecksumPreCopy = MatchRemoteChecksum
	Dependency.MatchRemoteChecksumPostCopy = MatchRemoteChecksum
	Dependency.NewChangePasswordChallengeFunc = NewChangePasswordChallengeFunc
	Dependency.NewError = error.New
	Dependency.NewSftpClient = newSftpClient
	Dependency.OpenFile = vfs.OpenFile
	Dependency.ReadFile = vfs.ReadFile
	Dependency.RunSshCommand = runSshCommand
	Dependency.RunSshCommandWithOutput = RunSshCommandWithOutput
	Dependency.SecurelyCopyFile = SecurelyCopyFile
}

func NewSshClient(address string, username User, password string) SshClient {
	return &SshClientImpl{
		Address:  address,
		Password: password,
		User:     username,
	}
}

type SshClientImpl struct {
	Address  string
	Password string
	User
}

func (g *SshClientImpl) GetAddress() string {
	return g.Address
}

func (g *SshClientImpl) GetPassword() string {
	return g.Password
}

func (g *SshClientImpl) GetUsername() User {
	return g.User
}

func (g *SshClientImpl) ChangeLoginPassword(username User, newPassword string) error {
	ui := Dependency.GetUi()
	ui.DisplayText("Logging-in as %s", username)

	_, err := Dependency.CreateClientConnection(g.Address, username, newPassword)
	if err == nil {
		ui.DisplayText("✔ Logged-in successfully as %s", username)
		return nil
	}

	ui.DisplayText("Changing password for %s", username)

	_, err = Dependency.CreateClientConnectionImpl(g.Address, username, Dependency.GetKeyboardInteractiveAuthMethod(newPassword))
	if err != nil {
		return Dependency.NewError(error.UnableToChangePassword, username)
	}

	ui.DisplayText("✔ Password changed for %s", username)

	return nil
}

func ChangeUserPassword(sshClient SshClient, username User, newPassword string) error {
	ui := Dependency.GetUi()

	changePassforGpadmin := fmt.Sprintf("echo -e \"%s\\n%s\" | passwd %s", newPassword, newPassword, username)

	ui.DisplayText("Changing password for %s", username)

	if err := SshClient.RunSshCommand("sudo -S sh -c '" + changePassforGpadmin + "'"); err != nil {
		return Dependency.NewError(error.UnableToChangePassword, username)
	}

	ui.DisplayText("✔ Password changed for %s", username)

	return nil
}

func (g *SshClientImpl) RunSshCommand(cmd string) error {
	return Dependency.RunSshCommandWithOutput(
		g,
		cmd,
		Dependency.GetUi().GetOutLogWriter(),
		Dependency.GetUi().GetErrLogWriter())
}

func (g *SshClientImpl) RunSshCommandReturnOutput(cmd string) (string, string, error) {
	var stdoutBuff bytes.Buffer
	var stderrBuff bytes.Buffer
	err := Dependency.RunSshCommandWithOutput(g, cmd, &stdoutBuff, &stderrBuff)

	return stdoutBuff.String(), stderrBuff.String(), err
}

func (g *SshClientImpl) SecurelyCopyFile(sourcePath string, destPath string) error {
	ui := Dependency.GetUi()
	remotePath := fmt.Sprintf("%s:%s", g.GetAddress(), destPath)
	ui.DisplayText("Preparing to copy %s to %s", sourcePath, remotePath)

	checksum, err := Dependency.GetChecksum(sourcePath)
	if err != nil {
		return err
	}

	ui.DisplayText("Checking existence of %s and its checksum", remotePath)
	if match, err := Dependency.MatchRemoteChecksumPreCopy(g, destPath, checksum); err != nil {
		return err
	} else if match {
		ui.DisplayText("Checksum matches; %s does not need to be copied", sourcePath)
		return nil
	}

	ui.DisplayText("Copying %s to %s", sourcePath, remotePath)
	if err := Dependency.SecurelyCopyFile(g, sourcePath, destPath); err != nil {
		return err
	}

	ui.DisplayText("Comparing checksums")
	if match, err := Dependency.MatchRemoteChecksumPostCopy(g, destPath, checksum); err != nil {
		return err
	} else if !match {
		return Dependency.NewError(error.ChecksumFailureForTransferredFile, destPath, g.Address)
	}

	ui.DisplayText("✔ Copy complete")

	return nil
}

func MatchRemoteChecksum(sshClient SshClient, fullPath string, checksum string) (bool, error) {
	address := SshClient.GetAddress()

	existsCmd := fmt.Sprintf("test -e %s", fullPath)
	if err := SshClient.RunSshCommand(existsCmd); err != nil {
		return false, nil
	}

	checksumCmd := fmt.Sprintf("sha256sum %s", fullPath)
	output, _, err := SshClient.RunSshCommandReturnOutput(checksumCmd)
	if err != nil {
		return false, Dependency.NewError(error.FailedToRunCommandOverSsh, checksumCmd, address, err)
	}

	// Get the checksum value from the output
	remoteChecksum := strings.Split(output, " ")[0]

	return remoteChecksum == checksum, nil
}

type response struct {
	password []string
	err      error
}

func NewChangePasswordChallengeFunc(newPassword string) ssh.KeyboardInteractiveChallenge {
	oldPassword := constants.DefaultGpvPassword
	questionCount := 0
	responses := []response{
		{[]string{oldPassword}, errors.New("failed to change the default password. hint: has the default password been updated?\n")},
		{[]string{oldPassword}, errors.New("unexpected error occurred when changing the default password\n")},
		{[]string{newPassword}, errors.New("BAD PASSWORD: it is based on a dictionary word\n")},
		{[]string{newPassword}, nil},
	}

	return func(user string, instruction string, questions []string, echos []bool) ([]string, error) {
		// this function will be called for (question count + 1) times.
		// len(questions) will be 0 if there are no more questions.
		// len(questions) will be 1 if there is another question.
		if len(questions) == 0 {
			return []string{}, responses[questionCount-1].err
		}

		questionCount += 1
		if questionCount > len(responses) {
			return []string{}, fmt.Errorf("unexpected prompt: %s", questions[0])
		}
		return responses[questionCount-1].password, nil
	}
}

func CreateClientConnection(address string, username User, password string) (SshClient, error) {
	return Dependency.CreateClientConnectionImpl(address, username, Dependency.GetPasswordAuthMethod(password))
}

func CreateClientConnectionImpl(address string, username User, authMethod ssh.AuthMethod) (SshClient, error) {
	config := &ssh.ClientConfig{
		User: string(username),
		Auth: []ssh.AuthMethod{
			authMethod,
		},
		HostKeyCallback: Dependency.InsecureIgnoreHostKey(),
	}

	client, err := Dependency.Dial("tcp", fmt.Sprintf("%s:22", address), config)
	if err != nil {
		return nil, Dependency.NewError(error.FailedToLoginToVm, address, err)
	}

	return client, nil
}

func GetKeyboardInteractiveAuthMethod(newPassword string) ssh.AuthMethod {
	challengeFunc := Dependency.NewChangePasswordChallengeFunc(newPassword)
	return Dependency.KeyboardInteractive(challengeFunc)
}

// RunSshCommandReturnOutput runs an SSH command remotely, and returns the combined output
// of Stdout and Stderr
func RunSshCommandWithOutput(g SshClient, cmd string, outWriter io.Writer, errWriter io.Writer) error {
	address := g.GetAddress()
	client, err := Dependency.CreateClientConnection(address, g.GetUsername(), g.GetPassword())
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return Dependency.NewError(error.FailedToCreateSshSession, address, err)
	}
	defer Dependency.CloseSession(session)

	session.Stdout = outWriter
	session.Stderr = errWriter

	if err := Dependency.RunSshCommand(session, cmd); err != nil {
		return Dependency.NewError(error.FailedToRunCommandOverSsh, cmd, address, err)
	}

	return nil
}

func SecurelyCopyFile(sshClient SshClient, sourcePath string, destPath string) error {
	address := sshClient.GetAddress()
	username := sshClient.GetUsername()
	password := sshClient.GetPassword()
	sshClient, err := Dependency.CreateClientConnection(address, username, password)
	if err != nil {
		return err
	}
	defer sshClient.Close()

	// open an SFTP client over an existing ssh connection.
	sftpClient, err := Dependency.NewSftpClient(sshClient)
	if err != nil {
		return Dependency.NewError(error.FailedToTransferFile, sourcePath, username, address, destPath, err)
	}
	defer sftpClient.Close()

	// Open the source file
	sourceFile, err := Dependency.OpenFile(sourcePath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destDir := filepath.Dir(destPath)
	if err = Dependency.CreateSftpDir(sftpClient, destDir); err != nil {
		return err
	}

	// Create the destination file
	destFile, err := Dependency.CreateSftpFile(sftpClient, destPath)
	if err != nil {
		return Dependency.NewError(error.FailedToTransferFile, sourcePath, username, address, destPath, err)
	}
	defer destFile.Close()

	// write to file
	if _, err := destFile.ReadFrom(sourceFile); err != nil {
		return Dependency.NewError(error.FailedToTransferFile, sourcePath, username, address, destPath, err)
	}

	return nil
}

func newSftpClient(sshClient SshClient) (SftpClient, error) {
	return sftp.NewClient(SshClient.(*ssh.Client))
}

func createSftpDir(sftpClient SftpClient, path string) error {
	return sftpClient.MkdirAll(path)
}

func createSftpFile(sftpClient SftpClient, path string) (SftpFile, error) {
	return sftpClient.Create(path)
}

//counterfeiter:generate . SftpClient
type SftpClient interface {
	Close() error
	Create(path string) (*sftp.File, error)
	MkdirAll(path string) error
}

//counterfeiter:generate . SftpFile
type SftpFile interface {
	Close() error
	ReadFrom(r io.Reader) (int64, error)
}

//counterfeiter:generate . SshClient
type SshClient interface {
	Close() error
	NewSession() (*ssh.Session, error)
}

func closeSession(session *ssh.Session) {
	defer session.Close()
}

func runSshCommand(session *ssh.Session, cmd string) error {
	return session.Run(cmd)
}

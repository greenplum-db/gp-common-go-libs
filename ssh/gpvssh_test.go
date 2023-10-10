package ssh_test

import (
	"errors"
	"io"
	"io/fs"
	"net"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/greenplum-db/gp-common-go-libs/error"
	"github.com/greenplum-db/gp-common-go-libs/ssh"
	"github.com/greenplum-db/gp-common-go-libs/ssh/sshfakes"
	ui "github.com/greenplum-db/gp-common-go-libs/ui"
	"github.com/greenplum-db/gp-common-go-libs/ui/uifakes"
	"github.com/greenplum-db/gp-common-go-libs/vfs"
	"github.com/greenplum-db/gp-common-go-libs/vfs/vfsfakes"
	"golang.org/x/crypto/ssh"
)

var _ = Describe("ssh", func() {
	var (
		expectedError     error.Error
		expectedErrorCode constants.ErrorCode
		expectedErrorArgs []any
		newErrorCallCount int
	)

	BeforeEach(func() {
		ssh.Default()

		newErrorCallCount = 0
		expectedError = error.New(99999)
		ssh.Dependency.NewError = func(errorCode constants.ErrorCode, args ...any) error.Error {
			newErrorCallCount++
			Expect(errorCode).To(Equal(expectedErrorCode))
			Expect(args).To(Equal(expectedErrorArgs))
			return expectedError
		}
	})

	Context("SshClientImpl", func() {
		var (
			testsshClient  ssh.SshClient
			testSshClient  *sshfakes.FakeSshClient
			testSshSession *ssh.Session
			testUi         *uifakes.Fakeui
		)

		BeforeEach(func() {
			testUi = &uifakes.Fakeui{}
			ssh.Dependency.GetUi = func() ui.Ui {
				return testUi
			}

			testSshClient = &sshfakes.FakeSshClient{}
			testSshSession = &ssh.Session{}
			testSshClient.NewSessionReturns(testSshSession, nil)

			testsshClient = ssh.NewSshClient("test-address", ssh.User("test-user"), "test-password")
		})

		Context("ChangeLoginPassword", func() {
			var (
				keyboardInteractiveCallCount int
				testPasswordAuthMethod       ssh.AuthMethod
			)

			BeforeEach(func() {
				testPasswordAuthMethod = ssh.Password("test-password-auth-method")
				keyboardInteractiveCallCount = 0
				ssh.Dependency.GetKeyboardInteractiveAuthMethod = func(newPassword string) ssh.AuthMethod {
					Expect(newPassword).To(Equal("test-password"))
					keyboardInteractiveCallCount++
					return testPasswordAuthMethod
				}
				ssh.Dependency.CreateClientConnection = func(address string, username ssh.User, password string) (ssh.SshClient, error) {
					Expect(address).To(Equal("test-address"))
					Expect(username).To(Equal(ssh.User("test-user")))
					Expect(password).To(Equal("test-password"))
					return nil, errors.New("failure to login")
				}
				ssh.Dependency.CreateClientConnectionImpl = func(address string, username ssh.User, authMethod ssh.AuthMethod) (ssh.SshClient, error) {
					Expect(address).To(Equal("test-address"))
					Expect(username).To(Equal(ssh.User("test-user")))
					return nil, nil
				}
			})

			When("the password has already been set to the given password", func() {
				It("reports successful login", func() {
					ssh.Dependency.CreateClientConnection = func(address string, username ssh.User, password string) (ssh.SshClient, error) {
						return nil, nil
					}
					Expect(testsshClient.ChangeLoginPassword(ssh.User("test-user"), "test-password")).To(Succeed())
					Expect(testUi.DisplayTextCallCount()).To(Equal(2))
					format, args := testUi.DisplayTextArgsForCall(0)
					Expect(format).To(Equal("Logging-in as %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					format, args = testUi.DisplayTextArgsForCall(1)
					Expect(format).To(Equal("✔ Logged-in successfully as %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
				})
			})

			When("changing the password fails", func() {
				It("returns the error", func() {
					ssh.Dependency.CreateClientConnectionImpl = func(address string, username ssh.User, authMethod ssh.AuthMethod) (ssh.SshClient, error) {
						return nil, errors.New("failure to change password")
					}
					expectedErrorCode = error.UnableToChangePassword
					expectedErrorArgs = []any{ssh.User("test-user")}

					err := testsshClient.ChangeLoginPassword(ssh.User("test-user"), "test-password")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(testUi.DisplayTextCallCount()).To(Equal(2))
					format, args := testUi.DisplayTextArgsForCall(0)
					Expect(format).To(Equal("Logging-in as %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					format, args = testUi.DisplayTextArgsForCall(1)
					Expect(format).To(Equal("Changing password for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					Expect(keyboardInteractiveCallCount).To(Equal(1))
				})
			})

			When("changing the password succeeds", func() {
				It("returns no error", func() {
					Expect(testsshClient.ChangeLoginPassword(ssh.User("test-user"), "test-password")).To(Succeed())
					Expect(testUi.DisplayTextCallCount()).To(Equal(3))
					format, args := testUi.DisplayTextArgsForCall(0)
					Expect(format).To(Equal("Logging-in as %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					format, args = testUi.DisplayTextArgsForCall(1)
					Expect(format).To(Equal("Changing password for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					format, args = testUi.DisplayTextArgsForCall(2)
					Expect(format).To(Equal("✔ Password changed for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					Expect(keyboardInteractiveCallCount).To(Equal(1))
				})
			})
		})

		Context("ChangeUserPassword", func() {
			BeforeEach(func() {
				ssh.Dependency.RunSshCommandWithOutput = func(g ssh.SshClient, cmd string, outWriter, errWriter io.Writer) error {
					return nil
				}
			})

			When("changing the password fails", func() {
				It("returns the error", func() {
					ssh.Dependency.RunSshCommandWithOutput = func(g ssh.SshClient, cmd string, outWriter, errWriter io.Writer) error {
						return errors.New("failure to change password")
					}
					expectedErrorCode = error.UnableToChangePassword
					expectedErrorArgs = []any{ssh.User("test-user")}

					err := ssh.Dependency.ChangeUserPassword(testsshClient, ssh.User("test-user"), "test-password")
					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(testUi.DisplayTextCallCount()).To(Equal(1))
					format, args := testUi.DisplayTextArgsForCall(0)
					Expect(format).To(Equal("Changing password for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
				})
			})

			When("changing the password succeeds", func() {
				It("returns no error", func() {
					Expect(ssh.Dependency.ChangeUserPassword(testsshClient, ssh.User("test-user"), "test-password")).To(Succeed())
					Expect(testUi.DisplayTextCallCount()).To(Equal(2))
					format, args := testUi.DisplayTextArgsForCall(0)
					Expect(format).To(Equal("Changing password for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
					format, args = testUi.DisplayTextArgsForCall(1)
					Expect(format).To(Equal("✔ Password changed for %s"))
					Expect(args).To(Equal([]any{ssh.User("test-user")}))
				})
			})
		})

		Context("RunSshCommand", func() {
			When("running a command over SSH", func() {
				It("passes the correct arguments and returns any error reported", func() {
					testUi := &uifakes.Fakeui{}
					testOutBuff := NewBuffer()
					testOutBuff.Write([]byte("test-output")) // This will make the buffers different for comparison by Ginkgo
					testErrBuff := NewBuffer()
					testErrBuff.Write([]byte("test-errput")) // This will make the buffers different for comparison by Ginkgo
					testUi.GetOutLogWriterReturns(testOutBuff)
					testUi.GetErrLogWriterReturns(testErrBuff)
					ssh.Dependency.GetUi = func() ui.Ui {
						return testUi
					}
					ssh.Dependency.RunSshCommandWithOutput = func(g ssh.SshClient, cmd string, outWriter io.Writer, errWriter io.Writer) error {
						Expect(g).To(Equal(testsshClient))
						Expect(cmd).To(Equal("test-command"))
						Expect(outWriter).To(Equal(testOutBuff))
						Expect(errWriter).To(Equal(testErrBuff))
						return errors.New("error running ssh")
					}

					Expect(testsshClient.RunSshCommand("test-command")).To(MatchError("error running ssh"))
				})
			})
		})

		Context("RunSshCommandReturnOutput", func() {
			When("running a command over SSH", func() {
				It("passes the correct arguments and returns any output and error reported", func() {
					ssh.Dependency.RunSshCommandWithOutput = func(g ssh.SshClient, cmd string, outWriter io.Writer, errWriter io.Writer) error {
						Expect(g).To(Equal(testsshClient))
						Expect(cmd).To(Equal("test-command"))
						outWriter.Write([]byte("test output"))
						errWriter.Write([]byte("test errput"))
						return errors.New("error running ssh")
					}

					output, errput, err := testsshClient.RunSshCommandReturnOutput("test-command")

					Expect(err).To(MatchError("error running ssh"))
					Expect(output).To(Equal("test output"))
					Expect(errput).To(Equal("test errput"))
				})
			})
		})

		Context("SecurelyCopyFile", func() {
			var (
				securelyCopyCallCount int
			)

			BeforeEach(func() {
				securelyCopyCallCount = 0
				ssh.Dependency.GetChecksum = func(fullPath string) (string, error) {
					Expect(fullPath).To(Equal("test-src-path"))
					return "test-checksum", nil
				}
				ssh.Dependency.MatchRemoteChecksumPreCopy = func(c ssh.SshClient, fullPath string, checksum string) (bool, error) {
					Expect(c).To(Equal(testsshClient))
					Expect(fullPath).To(Equal("test-dest-path"))
					Expect(checksum).To(Equal("test-checksum"))
					return false, nil
				}
				ssh.Dependency.SecurelyCopyFile = func(g ssh.SshClient, sourcePath string, destPath string) error {
					Expect(g).To(Equal(testsshClient))
					Expect(sourcePath).To(Equal("test-src-path"))
					Expect(destPath).To(Equal("test-dest-path"))
					securelyCopyCallCount++
					return nil
				}
				ssh.Dependency.MatchRemoteChecksumPostCopy = func(c ssh.SshClient, fullPath string, checksum string) (bool, error) {
					Expect(c).To(Equal(testsshClient))
					Expect(fullPath).To(Equal("test-dest-path"))
					Expect(checksum).To(Equal("test-checksum"))
					return true, nil
				}
			})

			When("getting local file checksum fails", func() {
				It("returns an error", func() {
					ssh.Dependency.GetChecksum = func(_ string) (string, error) {
						return "", errors.New("failed to get checksum")
					}

					Expect(testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")).To(MatchError("failed to get checksum"))
				})
			})

			When("matching the remote checksum fails with an error before copying", func() {
				It("returns the error", func() {
					ssh.Dependency.MatchRemoteChecksumPreCopy = func(_ ssh.SshClient, _ string, _ string) (bool, error) {
						return false, errors.New("connection failed")
					}

					Expect(testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")).To(MatchError("connection failed"))
				})
			})

			When("the remote checksum matches that of the local file", func() {
				It("skips the file transfer and returns no error", func() {
					ssh.Dependency.MatchRemoteChecksumPreCopy = func(_ ssh.SshClient, _ string, _ string) (bool, error) {
						return true, nil
					}

					Expect(testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")).To(Succeed())
					Expect(securelyCopyCallCount).To(Equal(0))
					Expect(testUi.DisplayTextCallCount()).To(Equal(3))
					fmt, args := testUi.DisplayTextArgsForCall(0)
					Expect(fmt).To(Equal("Preparing to copy %s to %s"))
					Expect(args).To(Equal([]any{"test-src-path", "test-address:test-dest-path"}))
					fmt, args = testUi.DisplayTextArgsForCall(1)
					Expect(fmt).To(Equal("Checking existence of %s and its checksum"))
					Expect(args).To(Equal([]any{"test-address:test-dest-path"}))
					fmt, args = testUi.DisplayTextArgsForCall(2)
					Expect(fmt).To(Equal("Checksum matches; %s does not need to be copied"))
					Expect(args).To(Equal([]any{"test-src-path"}))
				})
			})

			When("sending the file fails", func() {
				It("returns an error", func() {
					ssh.Dependency.SecurelyCopyFile = func(_ ssh.SshClient, _ string, _ string) error {
						return errors.New("failed to send the file")
					}

					err := testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")

					Expect(err).To(MatchError("failed to send the file"))
				})
			})

			When("matching the remote checksum fails with an error after copying", func() {
				It("returns the error", func() {
					ssh.Dependency.MatchRemoteChecksumPostCopy = func(_ ssh.SshClient, _ string, _ string) (bool, error) {
						return false, errors.New("post-copy checksum failed")
					}

					err := testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")

					Expect(err).To(MatchError("post-copy checksum failed"))
					Expect(securelyCopyCallCount).To(Equal(1))
				})
			})

			When("the remote checksum does not match that of the local file", func() {
				It("returns an error", func() {
					ssh.Dependency.MatchRemoteChecksumPostCopy = func(_ ssh.SshClient, _ string, _ string) (bool, error) {
						return false, nil
					}
					expectedErrorCode = error.ChecksumFailureForTransferredFile
					expectedErrorArgs = []any{"test-dest-path", "test-address"}

					err := testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(securelyCopyCallCount).To(Equal(1))
				})
			})

			When("the remote checksum matches that of the local file", func() {
				It("returns no error", func() {
					Expect(testsshClient.SecurelyCopyFile("test-src-path", "test-dest-path")).To(Succeed())
					Expect(securelyCopyCallCount).To(Equal(1))
					Expect(testUi.DisplayTextCallCount()).To(Equal(5))
					fmt, args := testUi.DisplayTextArgsForCall(0)
					Expect(fmt).To(Equal("Preparing to copy %s to %s"))
					Expect(args).To(Equal([]any{"test-src-path", "test-address:test-dest-path"}))
					fmt, args = testUi.DisplayTextArgsForCall(1)
					Expect(fmt).To(Equal("Checking existence of %s and its checksum"))
					Expect(args).To(Equal([]any{"test-address:test-dest-path"}))
					fmt, args = testUi.DisplayTextArgsForCall(2)
					Expect(fmt).To(Equal("Copying %s to %s"))
					Expect(args).To(Equal([]any{"test-src-path", "test-address:test-dest-path"}))
					Expect(testUi.DisplayTextArgsForCall(3)).To(Equal("Comparing checksums"))
					Expect(testUi.DisplayTextArgsForCall(4)).To(Equal("✔ Copy complete"))
				})
			})
		})
	})

	Context("ssh client package", func() {
		var (
			testsshClient  *sshfakes.FakesshClient
			testSshClient  *sshfakes.FakeSshClient
			testSshSession *ssh.Session
		)

		BeforeEach(func() {
			testsshClient = &sshfakes.FakesshClient{}
			testsshClient.GetAddressReturns("test-address")
			testsshClient.GetUsernameReturns("test-user")
			testsshClient.GetPasswordReturns("test-password")
			testSshClient = &sshfakes.FakeSshClient{}
			testSshSession = &ssh.Session{}
			testSshClient.NewSessionReturns(testSshSession, nil)
		})

		Context("NewChangePasswordChallengeFunc", func() {
			var funcChallenge ssh.KeyboardInteractiveChallenge
			echos := []bool{}
			questions := []string{
				"Password",
				`You are required to change your password (enforced)
	   ...
	   Password:`,
				"New Password:",
				"Retype new password:",
			}

			BeforeEach(func() {
				funcChallenge = ssh.NewChangePasswordChallengeFunc("foobar")

				ans, err := funcChallenge(string(ssh.Root), "", questions[0:1], echos)

				Expect(ans).To(Equal([]string{"changeme"}))
				Expect(err).ToNot(HaveOccurred())
			})

			When("the default VM root password has been modified", func() {
				It("returns an error", func() {
					ans, err := funcChallenge(string(ssh.Root), "", []string{}, echos)

					Expect(ans).To(Equal([]string{}))
					Expect(err).To(MatchError("failed to change the default password. hint: has the default password been updated?\n"))
				})
			})

			When("the default password has not changed", func() {
				BeforeEach(func() {
					ans, err := funcChallenge(string(ssh.Root), "", questions[1:2], echos)

					Expect(ans).To(Equal([]string{"changeme"}))
					Expect(err).ToNot(HaveOccurred())
				})

				When("the prompt for changing the old password fails for some reason", func() {
					It("returns an error", func() {
						ans, err := funcChallenge(string(ssh.Root), "", []string{}, echos)

						Expect(ans).To(Equal([]string{}))
						Expect(err).To(MatchError("unexpected error occurred when changing the default password\n"))
					})
				})

				When("the prompt for changing the old password passes", func() {
					BeforeEach(func() {
						ans, err := funcChallenge(string(ssh.Root), "", questions[2:3], echos)

						Expect(ans).To(Equal([]string{"foobar"}))
						Expect(err).ToNot(HaveOccurred())
					})

					When("the new password is invalid", func() {
						It("returns an error", func() {
							ans, err := funcChallenge(string(ssh.Root), "", []string{}, echos)

							Expect(ans).To(Equal([]string{}))
							Expect(err).To(MatchError("BAD PASSWORD: it is based on a dictionary word\n"))
						})
					})

					When("the new password is valid", func() {
						It("changes the password successfully", func() {
							ans, err := funcChallenge(string(ssh.Root), "", questions[3:4], echos)

							Expect(ans).To(Equal([]string{"foobar"}))
							Expect(err).ToNot(HaveOccurred())

							ans, err = funcChallenge(string(ssh.Root), "", []string{}, echos)

							Expect(ans).To(Equal([]string{}))
							Expect(err).ToNot(HaveOccurred())
						})
					})

					When("the prompt has more than four questions", func() {
						It("reports an error", func() {
							ans, err := funcChallenge(string(ssh.Root), "", questions[3:4], echos)

							Expect(ans).To(Equal([]string{"foobar"}))
							Expect(err).ToNot(HaveOccurred())

							ans, err = funcChallenge(string(ssh.Root), "", []string{"unexpected question"}, echos)

							Expect(ans).To(Equal([]string{}))
							Expect(err).To(MatchError("unexpected prompt: unexpected question"))
						})
					})
				})
			})
		})

		Context("CreateClientConnection", func() {
			When("dialing over ssh fails", func() {
				It("returns an error", func() {
					testClient := &ssh.Client{}
					gpamCallCount := 0
					ccciCallCount := 0
					ssh.Dependency.GetPasswordAuthMethod = func(secret string) ssh.AuthMethod {
						gpamCallCount++
						Expect(secret).To(Equal("test-password"))
						return ssh.Password(secret)
					}
					ssh.Dependency.CreateClientConnectionImpl = func(address string, username ssh.User, authMethod ssh.AuthMethod) (ssh.SshClient, error) {
						Expect(address).To(Equal("test-address"))
						Expect(username).To(Equal(ssh.User("test-user")))
						ccciCallCount++
						return testClient, errors.New("pass-through error")
					}

					SshClient, err := ssh.CreateClientConnection("test-address", ssh.User("test-user"), "test-password")

					Expect(err).To(MatchError("pass-through error"))
					Expect(SshClient).To(Equal(testClient))
					Expect(gpamCallCount).To(Equal(1))
					Expect(ccciCallCount).To(Equal(1))
				})
			})
		})

		Context("CreateClientConnectionImpl", func() {
			When("dialing over ssh fails", func() {
				It("returns an error", func() {
					ssh.Dependency.InsecureIgnoreHostKey = func() ssh.HostKeyCallback {
						return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
							return errors.New("callaback error")
						}
					}
					testErr := errors.New("failed to dial")
					ssh.Dependency.Dial = func(network string, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
						Expect(network).To(Equal("tcp"))
						Expect(addr).To(Equal("1.2.3.4:22"))
						Expect(config).ToNot(BeNil())
						Expect(config.User).To(Equal("test-user"))
						Expect(len(config.Auth)).To(Equal(1))
						Expect(config.HostKeyCallback("", nil, nil)).To(MatchError("callaback error"))

						return nil, testErr
					}
					expectedErrorCode = error.FailedToLoginToVm
					expectedErrorArgs = []any{"1.2.3.4", testErr}

					_, err := ssh.CreateClientConnectionImpl("1.2.3.4", "test-user", ssh.Password("test-password"))

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("dialing over ssh succeeds", func() {
				It("returns no error", func() {
					var testClient *ssh.Client

					ssh.Dependency.InsecureIgnoreHostKey = func() ssh.HostKeyCallback {
						return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
							return errors.New("callaback error")
						}
					}
					ssh.Dependency.Dial = func(network string, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
						Expect(network).To(Equal("tcp"))
						Expect(addr).To(Equal("1.2.3.4:22"))
						Expect(config).ToNot(BeNil())
						Expect(config.User).To(Equal("test-user"))
						Expect(len(config.Auth)).To(Equal(1))
						Expect(config.HostKeyCallback("", nil, nil)).To(MatchError("callaback error"))

						testClient = &ssh.Client{}

						return testClient, nil
					}

					client, err := ssh.CreateClientConnectionImpl("1.2.3.4", "test-user", ssh.Password("test-password"))

					Expect(err).ToNot(HaveOccurred())
					Expect(client).To(Equal(testClient))
				})
			})
		})

		Context("GetKeyboardInteractiveAuthMethod", func() {
			When("a password is supplied", func() {
				It("returns auth method", func() {
					ssh.Dependency.NewChangePasswordChallengeFunc = func(newPassword string) ssh.KeyboardInteractiveChallenge {
						Expect(newPassword).To(Equal("test-password"))
						return func(user string, instruction string, questions []string, echos []bool) ([]string, error) {
							return []string{"test-answer"}, errors.New("test-error")
						}
					}
					ssh.Dependency.KeyboardInteractive = func(challenge ssh.KeyboardInteractiveChallenge) ssh.AuthMethod {
						answers, err := challenge("", "", nil, nil)

						Expect(err).To(MatchError("test-error"))
						Expect(answers).To(Equal([]string{"test-answer"}))

						return ssh.Password("untest-password")
					}

					ssh.GetKeyboardInteractiveAuthMethod("test-password")
				})
			})
		})

		Context("MatchRemoteChecksum", func() {
			When("it fails to verify the existence of the remote file", func() {
				It("returns false", func() {
					testsshClient.RunSshCommandReturns(errors.New("failed to verify file existence"))

					match, err := ssh.MatchRemoteChecksum(testsshClient, "remote-file.rpm", "test-checksum")

					Expect(err).ToNot(HaveOccurred())
					Expect(match).To(BeFalse())
					Expect(testsshClient.RunSshCommandCallCount()).To(Equal(1))
					Expect(testsshClient.RunSshCommandArgsForCall(0)).To(Equal("test -e remote-file.rpm"))
					Expect(testsshClient.RunSshCommandReturnOutputCallCount()).To(Equal(0))
				})
			})

			When("it fails to execute the checksum command remotely", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to get checksum of file")
					testsshClient.RunSshCommandReturnOutputReturns("test-out", "test-err", testErr)
					expectedErrorCode = error.FailedToRunCommandOverSsh
					expectedErrorArgs = []any{"sha256sum remote-file.rpm", "test-address", testErr}

					_, err := ssh.MatchRemoteChecksum(testsshClient, "remote-file.rpm", "test-checksum")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(testsshClient.RunSshCommandCallCount()).To(Equal(1))
					Expect(testsshClient.RunSshCommandArgsForCall(0)).To(Equal("test -e remote-file.rpm"))
					Expect(testsshClient.RunSshCommandReturnOutputCallCount()).To(Equal(1))
					Expect(testsshClient.RunSshCommandReturnOutputArgsForCall(0)).To(Equal("sha256sum remote-file.rpm"))
				})
			})

			DescribeTable("happy paths for matching checksums for remote file",
				func(checksum string, expectedMatch bool) {
					testsshClient.RunSshCommandReturnOutputReturns("test-checksum ignore the rest of me", "test-error nothing to see here", nil)

					match, err := ssh.MatchRemoteChecksum(testsshClient, "remote-file.rpm", checksum)

					Expect(err).ToNot(HaveOccurred())
					Expect(match).To(Equal(expectedMatch))
					Expect(testsshClient.RunSshCommandCallCount()).To(Equal(1))
					Expect(testsshClient.RunSshCommandArgsForCall(0)).To(Equal("test -e remote-file.rpm"))
					Expect(testsshClient.RunSshCommandReturnOutputCallCount()).To(Equal(1))
					Expect(testsshClient.RunSshCommandReturnOutputArgsForCall(0)).To(Equal("sha256sum remote-file.rpm"))
				},
				Entry("matching checksum", "test-checksum", true),
				Entry("non-matching checksum", "test-checkall", false),
			)
		})

		Context("RunSshCommand", func() {
			var (
				closeSessionCallCount int
				errBuff               *Buffer
				outBuff               *Buffer
			)

			BeforeEach(func() {
				closeSessionCallCount = 0
				outBuff = NewBuffer()
				errBuff = NewBuffer()

				ssh.Dependency.CreateClientConnection = func(address string, username ssh.User, password string) (ssh.SshClient, error) {
					Expect(address).To(Equal("test-address"))
					Expect(username).To(Equal(ssh.User("test-user")))
					Expect(password).To(Equal("test-password"))
					return testSshClient, nil
				}
				ssh.Dependency.CloseSession = func(s *ssh.Session) {
					Expect(s).To(Equal(testSshSession))
					closeSessionCallCount++
				}
				ssh.Dependency.RunSshCommand = func(s *ssh.Session, cmd string) error {
					s.Stdout.Write([]byte("test-output"))
					s.Stderr.Write([]byte("test-errput"))
					Expect(s).To(Equal(testSshSession))
					Expect(s.Stdout).To(Equal(outBuff))
					Expect(s.Stderr).To(Equal(errBuff))
					Expect(cmd).To(Equal("test-command"))
					return nil
				}
			})

			When("creating a client connection fails", func() {
				It("returns an error", func() {
					ssh.Dependency.CreateClientConnection = func(_ string, _ ssh.User, _ string) (ssh.SshClient, error) {
						return nil, errors.New("failed to create connection")
					}

					err := ssh.RunSshCommandWithOutput(testsshClient, "test-command", outBuff, errBuff)

					Expect(err).To(MatchError("failed to create connection"))
				})
			})

			When("getting a session fails", func() {
				It("returns an error", func() {
					testErr := errors.New("new session failed")
					testSshClient.NewSessionReturns(nil, testErr)
					expectedErrorCode = error.FailedToCreateSshSession
					expectedErrorArgs = []any{"test-address", testErr}

					err := ssh.RunSshCommandWithOutput(testsshClient, "test-command", outBuff, errBuff)

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("running the command over ssh fails", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to run cmd")
					ssh.Dependency.RunSshCommand = func(_ *ssh.Session, _ string) error {
						return testErr
					}
					expectedErrorCode = error.FailedToRunCommandOverSsh
					expectedErrorArgs = []any{"test-command", "test-address", testErr}

					err := ssh.RunSshCommandWithOutput(testsshClient, "test-command", outBuff, errBuff)

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
					Expect(closeSessionCallCount).To(Equal(1))
				})
			})

			When("running the command over ssh succeeds", func() {
				It("returns no error", func() {
					Expect(ssh.RunSshCommandWithOutput(testsshClient, "test-command", outBuff, errBuff)).To(Succeed())
					Expect(testSshClient.CloseCallCount()).To(Equal(1))
					Expect(closeSessionCallCount).To(Equal(1))
					Expect(string(outBuff.Contents())).To(Equal("test-output"))
					Expect(string(errBuff.Contents())).To(Equal("test-errput"))
				})
			})
		})

		Context("SecurelyCopyFile", func() {
			var (
				testDestFile   *sshfakes.FakeSftpFile
				testsshClient  *sshfakes.FakesshClient
				testSftpClient *sshfakes.FakeSftpClient
				testSourceFile *vfsfakes.FakeGpvFile
			)

			BeforeEach(func() {
				passwordAuthMethod := ssh.Password("foo")
				testsshClient = &sshfakes.FakesshClient{}
				testsshClient.GetAddressReturns("test-address")
				testsshClient.GetUsernameReturns("test-user")
				testsshClient.GetPasswordReturns("test-password")
				testSftpClient = &sshfakes.FakeSftpClient{}
				testDestFile = &sshfakes.FakeSftpFile{}
				testSourceFile = &vfsfakes.FakeGpvFile{}
				testSshClient.NewSessionReturns(testSshSession, nil)

				ssh.Dependency.GetPasswordAuthMethod = func(secret string) ssh.AuthMethod {
					Expect(secret).To(Equal("test-password"))
					return passwordAuthMethod
				}
				ssh.Dependency.CreateClientConnection = func(address string, username ssh.User, password string) (ssh.SshClient, error) {
					Expect(address).To(Equal("test-address"))
					Expect(username).To(Equal(ssh.User("test-user")))
					Expect(password).To(Equal("test-password"))
					return testSshClient, nil
				}
				ssh.Dependency.NewSftpClient = func(SshClient ssh.SshClient) (ssh.SftpClient, error) {
					Expect(SshClient).To(Equal(testSshClient))
					return testSftpClient, nil
				}
				ssh.Dependency.OpenFile = func(fullPath string, flag int, perm os.FileMode) (vfs.GpvFile, error) {
					Expect(fullPath).To(Equal("test-source-path"))
					Expect(flag).To(Equal(os.O_RDONLY))
					Expect(perm).To(Equal(fs.FileMode(0)))
					return testSourceFile, nil
				}
				ssh.Dependency.CreateSftpFile = func(sftpClient ssh.SftpClient, path string) (ssh.SftpFile, error) {
					Expect(sftpClient).To(Equal(testSftpClient))
					Expect(path).To(Equal("test-dest-path"))
					return testDestFile, nil
				}
			})

			When("a client connection for SSH cannot be created", func() {
				It("returns an error", func() {
					ssh.Dependency.CreateClientConnection = func(address string, username ssh.User, password string) (ssh.SshClient, error) {
						return nil, errors.New("failed to create SSH client")
					}

					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).To(MatchError("failed to create SSH client"))
				})
			})

			When("a client connection for SFTP cannot be created", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to create SFTP client")
					ssh.Dependency.NewSftpClient = func(SshClient ssh.SshClient) (ssh.SftpClient, error) {
						return nil, testErr
					}
					expectedErrorCode = error.FailedToTransferFile
					expectedErrorArgs = []any{"test-source-path", ssh.User("test-user"), "test-address", "test-dest-path", testErr}

					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("the local file cannot be opened", func() {
				It("returns an error", func() {
					ssh.Dependency.OpenFile = func(fullPath string, flag int, perm os.FileMode) (vfs.GpvFile, error) {
						return nil, errors.New("open file failed")
					}

					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).To(MatchError("open file failed"))
				})
			})

			When("the remote file cannot be opened for creation", func() {
				It("returns an error", func() {
					testErr := errors.New("create file failed")
					ssh.Dependency.CreateSftpFile = func(sftpClient ssh.SftpClient, path string) (ssh.SftpFile, error) {
						return nil, testErr
					}
					expectedErrorCode = error.FailedToTransferFile
					expectedErrorArgs = []any{"test-source-path", ssh.User("test-user"), "test-address", "test-dest-path", testErr}

					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("the remote file cannot be filled with the contents of the local file", func() {
				It("returns an error", func() {
					testErr := errors.New("failed to copy contents")
					testDestFile.ReadFromReturns(0, testErr)
					expectedErrorCode = error.FailedToTransferFile
					expectedErrorArgs = []any{"test-source-path", ssh.User("test-user"), "test-address", "test-dest-path", testErr}

					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).To(MatchError(expectedError))
					Expect(newErrorCallCount).To(Equal(1))
				})
			})

			When("the remote file may be filled with the contents of the local file", func() {
				It("succeeds", func() {
					err := ssh.SecurelyCopyFile(testsshClient, "test-source-path", "test-dest-path")

					Expect(err).ToNot(HaveOccurred())
					Expect(testSshClient.CloseCallCount()).To(Equal(1))
					Expect(testSftpClient.CloseCallCount()).To(Equal(1))
					Expect(testSourceFile.CloseCallCount()).To(Equal(1))
					Expect(testDestFile.CloseCallCount()).To(Equal(1))
				})
			})
		})
	})
})

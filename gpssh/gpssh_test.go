package gpssh_test

import (
	"errors"
	"net"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"golang.org/x/crypto/ssh"

	"github.com/greenplum-db/gp-common-go-libs/gpssh"
	"github.com/greenplum-db/gp-common-go-libs/gpssh/gpsshfakes"
)

var _ = Describe("GPV SSH", func() {
	BeforeEach(func() {
		gpssh.Default()
	})

	Context("GpSshImpl", func() {
		var (
			testGpSshClient gpssh.GpSshClient
			testSshSession  *ssh.Session
			testSshClient   *gpsshfakes.FakeSshClient
		)

		BeforeEach(func() {
			testGpSshClient = gpssh.NewGpSshClient("test-address", "test-user", "test-path")
			testSshSession = &ssh.Session{}
			testSshClient = &gpsshfakes.FakeSshClient{}
			testSshClient.NewSessionReturns(testSshSession, nil)
		})

		Context("RunSshCommand", func() {
			var (
				closeSessionCallCount int
			)

			BeforeEach(func() {
				closeSessionCallCount = 0
				gpssh.Dependency.CreateClientConnection = func(address string, username gpssh.User, sshKeyPath string) (gpssh.SshClient, error) {
					Expect(address).To(Equal("test-address"))
					Expect(username).To(Equal(gpssh.User("test-user")))
					Expect(sshKeyPath).To(Equal("test-path"))
					return testSshClient, nil
				}
				testSshClient.NewSessionReturns(testSshSession, nil)
				gpssh.Dependency.CloseSession = func(session *ssh.Session) {
					Expect(session).To(Equal(testSshSession))
					closeSessionCallCount++
				}
				gpssh.Dependency.RunSshCommand = func(session *ssh.Session, cmd string) error {
					Expect(session).To(Equal(testSshSession))
					Expect(cmd).To(Equal("test-command"))
					session.Stdout.Write([]byte("test-output"))
					return nil
				}
			})

			When("a client connection cannot be established", func() {
				It("returns an error", func() {
					gpssh.Dependency.CreateClientConnection = func(_ string, _ gpssh.User, _ string) (gpssh.SshClient, error) {
						return nil, errors.New("create connection failed")
					}

					_, err := testGpSshClient.RunSshCommand("test-command")

					Expect(err).To(MatchError("create connection failed"))
				})
			})

			When("creating a new SSH session fails", func() {
				It("returns an error", func() {
					testSshClient.NewSessionReturns(nil, errors.New("create session failed"))

					_, err := testGpSshClient.RunSshCommand("test-command")

					Expect(err).To(MatchError("failed to create TLS session on VM test-address: create session failed"))
				})
			})

			When("running the SSH command fails", func() {
				When("the command's output is empty", func() {
					It("returns an error", func() {
						gpssh.Dependency.RunSshCommand = func(_ *ssh.Session, _ string) error {
							return errors.New("failed, no output")
						}

						_, err := testGpSshClient.RunSshCommand("test-command")

						Expect(err).To(MatchError("failed, no output"))
					})
				})

				When("the command's output is not empty", func() {
					It("returns an error containing the original error and the command's error output", func() {
						gpssh.Dependency.RunSshCommand = func(session *ssh.Session, _ string) error {
							session.Stderr.Write([]byte("test-failed-output"))
							return errors.New("failed, with output")
						}

						_, err := testGpSshClient.RunSshCommand("test-command")

						Expect(err).To(MatchError("failed, with output: test-failed-output"))
					})
				})
			})

			When("running the SSH command succeeds", func() {
				It("returns the command's output and no error", func() {
					output, err := testGpSshClient.RunSshCommand("test-command")

					Expect(err).ToNot(HaveOccurred())
					Expect(string(output)).To(Equal("test-output"))
					Expect(testSshClient.CloseCallCount()).To(Equal(1))
					Expect(closeSessionCallCount).To(Equal(1))
				})
			})
		})
	})

	DescribeTable("Constants",
		func(value gpssh.User, expectedValue gpssh.User) {
			Expect(value).To(Equal(expectedValue))
		},
		Entry("gpadmin", gpssh.Gpadmin, gpssh.User("gpadmin")),
		Entry("root", gpssh.Root, gpssh.User("root")),
	)

	Context("CreateClientConnection", func() {
		var (
			testAuthMethod ssh.AuthMethod
			testClient     *ssh.Client
		)

		BeforeEach(func() {
			testAuthMethod = ssh.Password("test-password")

			gpssh.Dependency.GetPublicKey = func(path string) (ssh.AuthMethod, error) {
				Expect(path).To(Equal("test-path"))
				return testAuthMethod, nil
			}
			gpssh.Dependency.Dial = func(network string, addr string, config *ssh.ClientConfig) (*ssh.Client, error) {
				Expect(network).To(Equal("tcp"))
				Expect(addr).To(Equal("test-address:22"))
				Expect(config).ToNot(BeNil())
				Expect(config.User).To(Equal("test-user"))
				Expect(len(config.Auth)).To(Equal(1))
				Expect(config.HostKeyCallback("", nil, nil)).To(MatchError("callaback error"))

				testClient = &ssh.Client{}

				return testClient, nil
			}
			gpssh.Dependency.InsecureIgnoreHostKey = func() ssh.HostKeyCallback {
				return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
					return errors.New("callaback error")
				}
			}
		})

		When("fetching the public key fails", func() {
			It("returns an error", func() {
				gpssh.Dependency.GetPublicKey = func(_ string) (ssh.AuthMethod, error) {
					return nil, errors.New("failed to get public key")
				}

				_, err := gpssh.CreateClientConnection("test-address", "test-user", "test-path")

				Expect(err).To(MatchError("failed to get public key"))
			})
		})

		When("dialing over ssh fails", func() {
			It("returns an error", func() {
				gpssh.Dependency.Dial = func(_ string, _ string, _ *ssh.ClientConfig) (*ssh.Client, error) {
					return nil, errors.New("failed to dial")
				}

				_, err := gpssh.CreateClientConnection("test-address", "test-user", "test-path")

				Expect(err).To(MatchError("failed to login to VM test-address: failed to dial"))
			})
		})

		When("dialing over ssh succeeds", func() {
			It("returns no error", func() {
				client, err := gpssh.CreateClientConnection("test-address", "test-user", "test-path")

				Expect(err).ToNot(HaveOccurred())
				Expect(client).To(Equal(testClient))
			})
		})
	})

	Context("GetSshSigner", func() {
		When("the content of a private key is encrypted", func() {
			It("parses the private key and returns any ssh.Signer and error encountered", func() {
				testSigner := &gpsshfakes.FakeGpSshSigner{}
				gpssh.Dependency.ParsePrivateKeyWithPassphrase = func(pemBytes []byte, passphrase []byte) (ssh.Signer, error) {
					Expect(string(pemBytes)).To(Equal("test-PEM-ENCRYPTED"))
					Expect(string(passphrase)).To(Equal(""))
					return testSigner, errors.New("parse encrypted error")
				}

				signer, err := gpssh.GetSshSigner("test-PEM-ENCRYPTED")

				Expect(err).To(MatchError("parse encrypted error"))
				Expect(signer).To(Equal(testSigner))
			})
		})

		When("the content of a private key is not encrypted", func() {
			It("parses the private key and returns any ssh.Signer and error encountered", func() {
				testSigner := &gpsshfakes.FakeGpSshSigner{}
				gpssh.Dependency.ParsePrivateKey = func(pemBytes []byte) (ssh.Signer, error) {
					Expect(string(pemBytes)).To(Equal("test-PEM-unencrypted"))
					return testSigner, errors.New("parse unencrypted error")
				}

				signer, err := gpssh.GetSshSigner("test-PEM-unencrypted")

				Expect(err).To(MatchError("parse unencrypted error"))
				Expect(signer).To(Equal(testSigner))
			})
		})
	})

	Context("NewgpsshClient", func() {
		When("a new GPV SSH client is requested", func() {
			It("is created with the expected contents", func() {
				expectedGpSshClient := &gpssh.GpSshClientImpl{
					Address: "test-address",
					KeyPath: "test-ssh-key-path",
					User:    gpssh.User("test-user"),
				}

				Expect(gpssh.NewGpSshClient("test-address", "test-user", "test-ssh-key-path")).To(Equal(expectedGpSshClient))
			})
		})
	})

	Context("ReadPublicKey", func() {
		var (
			publicKeysCallCount int
			testAuthMethod      ssh.AuthMethod
			testFileSystem      *gpvfsfakes.FakeGpvFs
			testSigner          *gpsshfakes.FakeGpSshSigner
		)

		BeforeEach(func() {
			testSigner = &gpsshfakes.FakeGpSshSigner{}
			testAuthMethod = ssh.Password("test-password")
			publicKeysCallCount = 0

			testFileSystem = &gpvfsfakes.FakeGpvFs{}
			gpssh.Dependency.FileSystem = testFileSystem

			testFileSystem.ReadReturns("test-PEM-content", nil)
			gpssh.Dependency.GetSshSigner = func(pem string) (ssh.Signer, error) {
				Expect(pem).To(Equal("test-PEM-content"))
				return testSigner, nil
			}
			gpssh.Dependency.PublicKeys = func(key ...ssh.Signer) ssh.AuthMethod {
				Expect(key).To(Equal([]ssh.Signer{testSigner}))
				publicKeysCallCount++
				return testAuthMethod
			}
		})

		When("readiing the private key from disk fails", func() {
			It("reports an error", func() {
				testFileSystem.ReadReturns("", errors.New("failed to read private key"))

				_, err := gpssh.GetPublicKey("test-path")

				Expect(err).To(MatchError("failed to read private key"))
			})
		})

		When("parsing the private key fails", func() {
			It("reports an error", func() {
				gpssh.Dependency.GetSshSigner = func(_ string) (ssh.Signer, error) {
					return nil, errors.New("failed to parse private key")
				}

				_, err := gpssh.GetPublicKey("test-path")

				Expect(err).To(MatchError("failed to parse private key"))
			})
		})

		When("parsing the private key succeeds", func() {
			It("returns a public key and no error", func() {
				_, err := gpssh.GetPublicKey("test-path")

				Expect(err).ToNot(HaveOccurred())
				Expect(publicKeysCallCount).To(Equal(1))
				// How do we test this properly?
				// Expect(authMethod).To(Equal(testAuthMethod))
			})
		})
	})
})

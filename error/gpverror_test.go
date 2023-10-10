package error_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/greenplum-db/gp-common-go-libs/error"
)

var _ = Describe("error package functions", func() {
	Context("New", func() {
		When("a new error.error is created", func() {
			It("matches an independently created struct", func() {
				expectedErr := &error.error{
					constants.ErrorCode(9999),
					errors.New("unknown error"),
				}

				Expect(error.New(9999)).To(Equal(expectedErr))
			})
		})
	})

	Context("NewInternalError", func() {
		When("a new error.InternalError is created", func() {
			It("matches an independently created struct", func() {
				expectedErr := &error.InternalError{
					errors.New("test-error"),
				}

				Expect(error.NewInternalError("test-error")).To(Equal(expectedErr))
			})
		})
	})
})

var _ = Describe("error", func() {
	var testErr *error.error

	BeforeEach(func() {
		testErr = &error.error{
			constants.ErrorCode(4321),
			errors.New("test-error"),
		}
	})

	Context("Error", func() {
		When("the function is called", func() {
			It("returns a formatted string representation of the error", func() {
				Expect(testErr.Error()).To(Equal("ERROR[4321] test-error"))
			})
		})
	})

	Context("GetCode", func() {
		When("the function is called", func() {
			It("returns the error code", func() {
				Expect(testErr.GetCode()).To(Equal(constants.ErrorCode(4321)))
			})
		})
	})

	Context("GetErr", func() {
		When("the function is called", func() {
			It("returns a string representation of the embedded error", func() {
				Expect(testErr.GetErr()).To(MatchError(errors.New("test-error")))
			})
		})
	})
})

var _ = Describe("InternalError", func() {
	var testErr *error.InternalError

	BeforeEach(func() {
		testErr = &error.InternalError{
			errors.New("test-error"),
		}
	})

	Context("Error", func() {
		When("the function is called", func() {
			It("returns a formatted string representation of the error", func() {
				Expect(testErr.Error()).To(Equal("test-error"))
			})
		})
	})

	Context("GetCode", func() {
		When("the function is called", func() {
			It("returns the error code", func() {
				Expect(testErr.GetCode()).To(Equal(constants.ErrorCode(9999)))
			})
		})
	})

	Context("GetErr", func() {
		When("the function is called", func() {
			It("returns a string representation of the embedded error", func() {
				Expect(testErr.GetErr()).To(MatchError(errors.New("test-error")))
			})
		})
	})
})

var _ = Describe("error", func() {
	DescribeTable("New",
		func(errorCode constants.ErrorCode, expectedErrorText string, args ...any) {
			err := error.New(errorCode, args...)

			error, ok := err.(error.Error)
			Expect(ok).To(BeTrue())
			Expect(error.GetErr()).To(MatchError(expectedErrorText))
			Expect(error.GetCode()).To(Equal(errorCode))
		},

		Entry("unknown error type", constants.ErrorCode(999), "unknown error"),
		Entry("unimplemented function error", error.NotImplemented, "not implemented"),
		Entry("unhandled error", error.UnhandledError, "unhandled error: foobar", errors.New("foobar")),
		Entry("invalid value", error.InvalidValue, "invalid value: 'FUBAR'", "FUBAR"),
		Entry("invalid option", error.InvalidOption, "invalid value: 'BARFU' not in [o1 o2 o3]", "BARFU", []string{"o1", "o2", "o3"}),
		Entry("invalid config", error.FailedToValidateConfig, "failed to validate configuration; please see output above for details"),
		Entry("invalid post request", error.FailedToCreateHttpPostRequest, "failed to create http post request: failed to compile", errors.New("failed to compile")),
		Entry("failed to send post request", error.FailedToSendHttpPostRequest, "failed to send http post request: connection failed", errors.New("connection failed")),
		Entry("unexpected http status", error.UnexpectedHttpStatusCode, "unexpected http status code: 500", 500),
		Entry("unable to change password", error.UnableToChangePassword, "unable to change password for user test-user", "test-user"),
		Entry("checksum of securely transferred file failed", error.ChecksumFailureForTransferredFile, "the checksum of test-file-path on test-address does not match that of the local file", "test-file-path", "test-address"),
		Entry("calculating checksum of local file failed", error.FailedToComputeChecksumForFile, "failed to compute SHA256 for test-file-path: failed to pop popcorn", "test-file-path", errors.New("failed to pop popcorn")),
		Entry("config file not found", error.ConfigFileNotFound, "configuration file not found; please run \"gpv config init\""),
		Entry("failed to transfer file", error.FailedToTransferFile, "failed to transfer file src to user@address:dest due to: barfy", "src", "user", "address", "dest", errors.New("barfy")),
		Entry("failed to get user input", error.FailedToGetUserInput, "failed to get user input: bad typing skills", errors.New("bad typing skills")),
		Entry("executing an external command fails", error.ExternalCommandFailed, "executing `/foo/command` failed: exec failure", "/foo/command", errors.New("exec failure")),
		Entry("marshaling of config struct fails", error.FailedToMarshal, "unexpected failure to marshal information due to: some marshaling failure", errors.New("some marshaling failure")),
		Entry("invalid number of hosts", error.InvalidNumberOfHosts, "expected to find 4 hosts, but found 3", 4, 3),
		Entry("invalid number of nics per host", error.InvalidNumberOfNicsPerHost, "expected to find 4 nics on host foobarker.com, but found 3", 4, "foobarker.com", 3),
		Entry("invalid nic speed", error.InvalidNicSpeed, "expected speed of 5Mbps, but got 2Mbps for vmnic vmnic4 on host foobarker.com", 5, 2, "vmnic4", "foobarker.com"),
		Entry("mismatched vmnics to switch", error.MismatchedVmnicToSwitch, "invalid vmnic configuration: vmnics must be on the same switch; Vmnic vmnic2 has two different switches: switch and eroo", "vmnic2", "switch", "eroo"),
		Entry("mismatched vmnic speeds", error.MismatchedVmnicSpeed, "invalid vmnic configuration: vmnics must have same link speed; Vmnic vmnic1 has two different speeds: 25Mbps and 100Mbps", "vmnic1", 25, 100),
		Entry("mismatched vmnic configuration", error.MismatchedVmnicConfiguration, "invalid vmnic configuration: vmnic0 and vmnic3 must be on the same switch", "vmnic0", "vmnic3"),
		Entry("invalid vmnic combination", error.InvalidVmnicCombination, "invalid vmnic configuration: vmnic0/vmnic3 and vmnic1/vmnic2 cannot be on the same switch"),
		Entry("invalid number of arguments", error.InvalidNumberOfArguments, "invalid number of arguments: expected 1, but got 2", 1, 2),
		Entry("mismatch in switch and datacenter", error.MismatchedSwitchToDatacenter, "switch switch-1 was found, but is not in datacenter datacenter-2", "switch-1", "datacenter-2"),
		Entry("failed to extract switch configuration", error.UnableToExtractSwitchConfigInfo, "unable to extract configuration for vSphere Distributed Virtual Switch switch-1", "switch-1"),
		Entry("failure to retrieve a switch from vCenter", error.UnableToRetrieveSwitch, "unable to retrieve vSphere Distributed Virtual Switch switch-1: sad failure", "switch-1", errors.New("sad failure")),
		Entry("failed to find the distributed switch", error.NoSwitchFound, "unable to find vSphere Distributed Virtual Switch switch-1", "switch-1"),
		Entry("too many switches were returned", error.SwitchNotUnique, "vSphere Distributed Switch switch-1 is not unique", "switch-1"),
		Entry("failed to fetch the list of datacenters", error.UnableToRetrieveDatacenter, "unable to retrieve datacenter test-datacenter: fetch failed", "test-datacenter", errors.New("fetch failed")),
		Entry("unable to retrieve list of hosts", error.UnableToRetrieveHosts, "unable to retrieve hosts from cluster my-cluster: badness", "my-cluster", errors.New("badness")),
		Entry("no hosts found in cluster", error.NoHostsFound, "no hosts found in cluster my-cluster", "my-cluster"),
		Entry("unable to find cluster", error.ClusterNotFound, "cannot find cluster my-cluster on datacenter my-datacenter", "my-cluster", "my-datacenter"),
		Entry("wrong network type for switch", error.WrongNetworkTypeForSwitch, "test-switch is not a vSphere Distributed Virtual Switch", "test-switch"),
		Entry("unable to retrieve switch properties", error.UnableToRetrieveSwitchProperties, "unable to fetch properties for vSphere Distributed Virtual Switch test-switch: network failure", "test-switch", errors.New("network failure")),
		Entry("unable to retrieve host", error.UnableToRetrieveHostDetails, "unable to fetch host details for cluster test-cluster: discombobulation", "test-cluster", errors.New("discombobulation")),
		Entry("unable to find physical nic info", error.PhysicalNicInfoNotFound, "unable to find network information for physical nic my-pnic", "my-pnic"),
		Entry("govmomi error occurred", error.GovmomiError, "govmomi error occurred: network error", errors.New("network error")),
		Entry("failed to login to vcenter", error.FailedToLogin, "failed to login to vCenter foo.example.com: network error", "foo.example.com", errors.New("network error")),
		Entry("failed to power-on virtual machine", error.FailedToPowerOnVm, "failed to power-on VM test-vm: general failure", "test-vm", errors.New("general failure")),
		Entry("failed to login to VM", error.FailedToLoginToVm, "failed to login to VM test-vm: general error", "test-vm", errors.New("general error")),
		Entry("failed to power-off virtual machine", error.FailedToShutdownVm, "failed to shutdown VM test-vm: general failure", "test-vm", errors.New("general failure")),
		Entry("failed to read password", error.FailedToReadPassword, "failed to read password: general failure", errors.New("general failure")),
		Entry("failed to create TLS session on VM", error.FailedToCreateSshSession, "failed to create TLS session on VM test-vm: general error", "test-vm", errors.New("general error")),
		Entry("failed to run command via TLS on VM", error.FailedToRunCommandOverSsh, "failed to run `/foo/bar` on VM base-vm: general error", "/foo/bar", "base-vm", errors.New("general error")),
		Entry("unable to decode password", error.UnableToDecodePassword, "unable to decode password"),
		Entry("failed to retrieve segment count", error.FailedToRetrieveSegmentCount, "unable to retrieve number of segments from Greenplum"),
		Entry("failed to marshal tfvars", error.FailedToMarshalJson, "unexpected failure to marshal JSON due to: out of memory", errors.New("out of memory")),
		Entry("file system issue", error.FileSystemIssue, "unexpected file system issue: out of space", errors.New("out of space")),
		Entry("failed to read file", error.FailedToReadFile, "unable to read file /blah: out of time", "/blah", errors.New("out of time")),
		Entry("unable to unmarshal file", error.FailedToUnmarshalFile, "unable to deserialize file /blah: out of mind", "/blah", errors.New("out of mind")),
		Entry("invalid URI", error.InvalidUri, `invalid URI: "g.c\"`, `g.c\`),
		Entry("invalid IP", error.InvalidIp, "invalid IPv4 address: 1.2.3", "1.2.3"),
		Entry("invalid IPv4 netmask", error.InvalidNetmask, "invalid IPv4 netmask: 1.2.3.4", "1.2.3.4"),
		Entry("unable to parse vCenter URI", error.UnableToParseVcenterUri, `could not parse vCenter URI "foo.bar"`, "foo.bar"),
		Entry("no public IPs found", error.NoPublicIpsFound, `no public IPs found in config.yml. Please run "gpv config init"`),
		Entry("no NTP servers found", error.NoNtpServersFound, `no NTP servers found in config.yml. Please run "gpv config init"`),
		Entry("cannot use DEFAULT_GPV_PASSWORD as password", error.DefaultPasswordChangeme, "cannot use 'changeme' as password"),
		Entry("failed to validate greenplum deployment", error.FailedToValidateGreenplumDeployment, "failed to validate greenplum deployment due to ssh error", "ssh error"),
		Entry("number of segments doesn't match", error.IncorrectSegmentCount, "the number of segments and masters does not match the config: expected 8, but got 12", "8", "12"),
		Entry("wrong number of IPs for mirrored", error.WrongIpCountForMirrored, "expected 2 IPs for the mirrored deployment, but received 1", 1),
		Entry("wrong number of IPs for mirrorless", error.WrongIpCountForMirrorless, "expected 1 IP for the mirrorless deployment, but received 2", 2),
		Entry("invalid deployment type", error.InvalidDeploymentType, "the given deployment type is not one of [mirrored, mirrorless]; please re-run \"gpv config init\" or \"gpv config set database deployment-type\""),
		Entry("failed to parse cidr", error.FailedToParseCidr, "failed to parse CIDR: 1.1.1.1/", "1.1.1.1/"),
		Entry("base VM already exists", error.BaseVmAlreadyExists, "failed to clone template: base VM test-base-name already exists", "test-base-name"),
		Entry("ovf property cannot be found", error.OvfPropertyNotFound, "ovf property with key=mtu cannot be found", "mtu"),
		Entry("ethernet device cannot be found", error.EthernetDeviceNotFound, "ethernet device not found"),
		Entry("add mangle endpoint failed", error.FailedToAddEndpointCredential, "failed to add stress testing endpoint credential due to: test-error", errors.New("test-error")),
		Entry("add mangle credentials failed", error.FailedToAddRemoteMachineCredential, "failed to add stress testing remote machine credential due to: test-error", errors.New("test-error")),
		Entry("kill postgres process failed", error.FailedToKillPostgresProcess, "failed to kill the postgres process due to: test-error", errors.New("test-error")),
		Entry("unable to extract Greenplum process identifier", error.FailedToGetGpdbProcessIdentifier, "unable to extract Greenplum process identifier from package name: test-package", "test-package"),
		Entry("unexpected mangle response", error.UnexpectedMangleResponse, "unexpected mangle response with status code 500: some-description", 500, "some-description"),
	)
})

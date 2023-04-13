package cluster_test

import (
	"database/sql/driver"
	"fmt"
	"os"
	"os/user"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cluster tests")
}

var (
	connection *dbconn.DBConn
	mock       sqlmock.Sqlmock
	logfile    *gbytes.Buffer
)

func expectPathToExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Fail(fmt.Sprintf("Expected %s to exist", path))
	}
}

var _ = BeforeSuite(func() {
	_, _, _, _, logfile = testhelper.SetupTestEnvironment()
})

var _ = BeforeEach(func() {
	connection, mock = testhelper.CreateAndConnectMockDB(1)
})

var _ = Describe("cluster/cluster tests", func() {
	coordinatorSeg := cluster.SegConfig{DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1", Role: "p"}
	localSegOne := cluster.SegConfig{DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{DbID: 3, ContentID: 1, Port: 20001, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	localSegTwo := cluster.SegConfig{DbID: 4, ContentID: 2, Port: 20002, Hostname: "localhost", DataDir: "/data/gpseg2"}
	remoteSegTwo := cluster.SegConfig{DbID: 5, ContentID: 3, Port: 20003, Hostname: "remotehost2", DataDir: "/data/gpseg3"}
	standbyCoordinator := cluster.SegConfig{DbID: 6, ContentID: -1, Port: 5432, Hostname: "standbycoordinatorhost", DataDir: "/data/gpseg-1", Role: "m"}
	standbyCoordinatorOnSegHost := cluster.SegConfig{DbID: 6, ContentID: -1, Port: 5432, Hostname: "remotehost1", DataDir: "/data/gpseg-1", Role: "m"}
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
	})
	Describe("ConstructSSHCommand", func() {
		It("constructs a local ssh command", func() {
			cmd := cluster.ConstructSSHCommand(true, "some-host", "ls")
			Expect(cmd).To(Equal([]string{"bash", "-c", "ls"}))
		})
		It("constructs a remote ssh command", func() {
			cmd := cluster.ConstructSSHCommand(false, "some-host", "ls")
			Expect(cmd).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@some-host", "ls"}))
		})
	})
	Describe("GetSegmentConfiguration", func() {
		header := []string{"contentid", "hostname", "datadir"}
		localSegOne := []driver.Value{"0", "localhost", "/data/gpseg0"}
		localSegTwo := []driver.Value{"1", "localhost", "/data/gpseg1"}
		remoteSegOne := []driver.Value{"2", "remotehost", "/data/gpseg2"}

		It("returns only primaries for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns only primaries for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns only primaries for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
		It("returns primaries and mirrors for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns primaries and mirrors for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns primaries and mirrors for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
		It("returns mirrors for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns mirrors for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns mirrors for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection, true, true)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
	})

	Describe("GenerateSSHCommandList", func() {
		coordinatorSegCmd := []string{"bash", "-c", "ls"}
		localSegCmd := []string{"bash", "-c", "ls"}
		remoteSegOneCmd := []string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}
		remoteSegTwoCmd := []string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost2", "ls"}
		standbyCoordinatorCmd := []string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@standbycoordinatorhost", "ls"}
		DescribeTable("GenerateSSHCommandList with segments", func(scope cluster.Scope, includeCoordinator bool, numLocalSegments int, numRemoteSegments int) {
			segments := []cluster.SegConfig{coordinatorSeg}
			expectedCommands := []cluster.ShellCommand{}
			if includeCoordinator {
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -1, "", coordinatorSegCmd))
			}
			if numLocalSegments >= 1 {
				segments = append(segments, localSegOne)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, 0, "", localSegCmd))
			}
			if numLocalSegments >= 2 {
				segments = append(segments, localSegTwo)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, 2, "", localSegCmd))
			}
			if numRemoteSegments >= 1 {
				segments = append(segments, remoteSegOne)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, 1, "", remoteSegOneCmd))
			}
			if numRemoteSegments >= 2 {
				segments = append(segments, remoteSegTwo)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, 3, "", remoteSegTwoCmd))
			}

			testCluster := cluster.NewCluster(segments)
			commandList := testCluster.GenerateSSHCommandList(scope, func(_ int) string {
				return "ls"
			})
			Expect(commandList).To(Equal(expectedCommands))
		},
			Entry("Returns a list of ssh commands for the coordinator, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, true, 0, 0),
			Entry("Returns a list of ssh commands for the coordinator, excluding coordinator", cluster.ON_SEGMENTS, false, 0, 0),
			Entry("Returns a list of ssh commands for one segment, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, true, 0, 1),
			Entry("Returns a list of ssh commands for one segment, excluding coordinator", cluster.ON_SEGMENTS, false, 0, 1),
			Entry("Returns a list of ssh commands for two segments on the same host, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, true, 2, 0),
			Entry("Returns a list of ssh commands for two segments on the same host, excluding coordinator", cluster.ON_SEGMENTS, false, 2, 0),
			Entry("Returns a list of ssh commands for three segments on different hosts, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, true, 1, 2),
			Entry("Returns a list of ssh commands for three segments on different hosts, excluding coordinator", cluster.ON_SEGMENTS, false, 1, 2),
		)

		DescribeTable("GenerateSSHCommandList with hosts", func(scope cluster.Scope, includeCoordinator bool, includeMirrors bool, standby cluster.SegConfig, numLocalSegments int, numRemoteSegments int) {
			segments := []cluster.SegConfig{coordinatorSeg, standby}
			expectedCommands := []cluster.ShellCommand{}
			if includeCoordinator {
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -2, "localhost", coordinatorSegCmd))
			}
			if includeMirrors {
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -2, "standbycoordinatorhost", standbyCoordinatorCmd))
			}
			if numLocalSegments >= 1 {
				segments = append(segments, localSegOne)
				if !includeCoordinator {
					expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -2, "localhost", localSegCmd))
				}
			}
			if numLocalSegments >= 2 {
				segments = append(segments, localSegTwo)
			}
			if numRemoteSegments >= 1 {
				segments = append(segments, remoteSegOne)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -2, "remotehost1", remoteSegOneCmd))
			}
			if numRemoteSegments >= 2 {
				segments = append(segments, remoteSegTwo)
				expectedCommands = append(expectedCommands, cluster.NewShellCommand(scope, -2, "remotehost2", remoteSegTwoCmd))
			}

			testCluster := cluster.NewCluster(segments)
			commandList := testCluster.GenerateSSHCommandList(scope, func(_ string) string {
				return "ls"
			})
			Expect(commandList).To(Equal(expectedCommands))
		},
			Entry("returns a list of ssh commands for the coordinator host, including the coordinator host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, true, false, standbyCoordinator, 0, 0),
			Entry("returns a list of ssh commands for the coordinator host, excluding the coordinator host", cluster.ON_HOSTS, false, false, standbyCoordinator, 0, 0),
			Entry("returns a list of ssh commands for the coordinator host, including the coordinator and standby coordinator hosts", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR|cluster.INCLUDE_MIRRORS, true, true, standbyCoordinator, 0, 0),
			Entry("returns a list of ssh commands for the coordinator host, excluding the coordinator host and including standby coordinator host", cluster.ON_HOSTS|cluster.EXCLUDE_COORDINATOR|cluster.INCLUDE_MIRRORS, false, true, standbyCoordinator, 0, 0),
			Entry("returns a list of ssh commands for the coordinator host, including the coordinator host and not skipping the standby/segment host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR|cluster.EXCLUDE_MIRRORS, true, false, standbyCoordinatorOnSegHost, 0, 2),
			Entry("returns a list of ssh commands for one host, including the coordinator host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, true, false, standbyCoordinator, 0, 1),
			Entry("returns a list of ssh commands for one host, excluding the coordinator host", cluster.ON_HOSTS, false, false, standbyCoordinator, 0, 1),
			Entry("returns a list of ssh commands for one host containing two segments, including the coordinator host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, true, false, standbyCoordinator, 2, 0),
			Entry("returns a list of ssh commands for one host containing two segments, excluding the coordinator host", cluster.ON_HOSTS, false, false, standbyCoordinator, 2, 0),
			Entry("returns a list of ssh commands for one local host and two remote hosts, including the coordinator host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, true, false, standbyCoordinator, 0, 2),
			Entry("returns a list of ssh commands for one local host and two remote hosts, excluding the coordinator host", cluster.ON_HOSTS, false, false, standbyCoordinator, 0, 2),
		)
	})
	Describe("ExecuteLocalCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/gp_common_go_libs_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/gp_common_go_libs_test")
		})
		It("runs the specified command", func() {
			testCluster := cluster.Cluster{}
			commandStr := "touch /tmp/gp_common_go_libs_test/foo"
			testCluster.Executor = &cluster.GPDBExecutor{}
			testCluster.ExecuteLocalCommand(commandStr)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
		})
		It("returns any error generated by the specified command", func() {
			testCluster := cluster.Cluster{}
			commandStr := "some-non-existent-command /tmp/gp_common_go_libs_test/foo"
			testCluster.Executor = &cluster.GPDBExecutor{}
			output, err := testCluster.ExecuteLocalCommand(commandStr)

			Expect(output).To(Equal("bash: some-non-existent-command: command not found\n"))
			Expect(err.Error()).To(Equal("exit status 127"))
		})
	})
	Describe("ExecuteClusterCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/gp_common_go_libs_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/gp_common_go_libs_test")
		})
		It("runs commands specified by command slice", func() {
			testCluster := cluster.Cluster{}
			commandList := []cluster.ShellCommand{
				cluster.NewShellCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, -1, "", []string{"touch", "/tmp/gp_common_go_libs_test/foo"}),
				cluster.NewShellCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, 0, "", []string{"touch", "/tmp/gp_common_go_libs_test/baz"}),
			}
			for _, cmd := range commandList {
				Expect(cmd.Completed).To(BeFalse())
			}

			testCluster.Executor = &cluster.GPDBExecutor{}
			clusterOutput := testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, commandList)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			expectPathToExist("/tmp/gp_common_go_libs_test/baz")
			Expect(clusterOutput.NumErrors).To(Equal(0))
			Expect(len(clusterOutput.FailedCommands)).To(Equal(0))
			for _, cmd := range clusterOutput.Commands {
				Expect(cmd.Completed).To(BeTrue())
			}
		})
		It("returns any errors generated by any of the commands", func() {
			testCluster := cluster.Cluster{}
			commandList := []cluster.ShellCommand{
				cluster.NewShellCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, -1, "", []string{"touch", "/tmp/gp_common_go_libs_test/foo"}),
				cluster.NewShellCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, 0, "", []string{"some-non-existent-command"}),
			}
			testCluster.Executor = &cluster.GPDBExecutor{}
			clusterOutput := testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, commandList)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			Expect(clusterOutput.NumErrors).To(Equal(1))
			Expect(len(clusterOutput.FailedCommands)).To(Equal(1))
			Expect(clusterOutput.FailedCommands[0].Error.Error()).To(Equal("exec: \"some-non-existent-command\": executable file not found in $PATH"))
			for _, cmd := range clusterOutput.Commands {
				Expect(cmd.Completed).To(BeTrue())
			}
			for _, cmd := range clusterOutput.FailedCommands {
				Expect(cmd.Completed).To(BeTrue())
			}
		})
	})
	Describe("CheckClusterError", func() {
		var (
			remoteOutput *cluster.RemoteOutput
			failedCmd    cluster.ShellCommand
		)
		BeforeEach(func() {
			failedCmd = cluster.ShellCommand{
				Scope:         0, // The appropriate scope will be set in each test
				Content:       1,
				Host:          "remotehost1",
				Command:       nil,
				CommandString: "this is the command",
				Stderr:        "exit status 1",
				Error:         errors.Errorf("command error"),
			}
			remoteOutput = &cluster.RemoteOutput{
				Scope:          0,
				NumErrors:      1,
				Commands:       []cluster.ShellCommand{failedCmd},
				FailedCommands: []*cluster.ShellCommand{&failedCmd},
			}
		})
		DescribeTable("CheckClusterError", func(scope cluster.Scope, includeCoordinator bool, perSegment bool, remote bool) {
			remoteOutput.Scope = scope
			remoteOutput.Commands[0].Scope = scope
			remoteOutput.FailedCommands[0].Scope = scope
			errStr := "1 segment"
			debugStr := "segment 1 on host remotehost1"
			var generatorFunc interface{}
			generatorFunc = func(contentID int) string { return "Error received" }
			if !perSegment {
				errStr = "1 host"
				debugStr = "host remotehost1"
				generatorFunc = func(host string) string { return "Error received" }
			}
			if !remote {
				errStr = "coordinator for " + errStr
			}
			defer testhelper.ShouldPanicWithMessage(fmt.Sprintf("Got an error on %s. See gbytes.Buffer for a complete list of errors.", errStr))
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Command was: this is the command`))
			defer Expect(logfile).To(gbytes.Say(fmt.Sprintf(`\[DEBUG\]:-Error received on %s with error command error: exit status 1`, debugStr)))
			testCluster.CheckClusterError(remoteOutput, "Got an error", generatorFunc)
		},
			Entry("prints error messages for a per-segment command, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, true, true, true),
			Entry("prints error messages for a per-segment command, excluding coordinator", cluster.ON_SEGMENTS, false, true, true),
			Entry("prints error messages for a per-host command, including the coordinator host", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, true, false, true),
			Entry("prints error messages for a per-host command, excluding the coordinator host", cluster.ON_HOSTS, false, false, true),
			Entry("prints error messages for commands executed on coordinator to segments, including coordinator", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR|cluster.ON_LOCAL, true, true, false),
			Entry("prints error messages for commands executed on coordinator to segments, excluding coordinator", cluster.ON_SEGMENTS|cluster.ON_LOCAL, false, true, false),
			Entry("prints error messages for commands executed on coordinator to hosts, including coordinator", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR|cluster.ON_LOCAL, true, false, false),
			Entry("prints error messages for commands executed on coordinator to hosts, excluding coordinator", cluster.ON_HOSTS|cluster.ON_LOCAL, false, false, false),
		)
	})
	Describe("LogFatalClusterError", func() {
		It("logs an error for 1 segment (with coordinator)", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 1 segment. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR, 1)
		})
		It("logs an error for more than 1 segment", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 2 segments. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS, 2)
		})
		It("logs an error for 1 host", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 1 host. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_HOSTS, 1)
		})
		It("logs an error for more than 1 host (with coordinator)", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 2 hosts. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_HOSTS|cluster.INCLUDE_COORDINATOR, 2)
		})
	})
	Describe("NewCluster", func() {
		It("sets up the configuration for a single-host, single-segment cluster", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne})
			Expect(len(newCluster.ContentIDs)).To(Equal(2))
			Expect(len(newCluster.Hostnames)).To(Equal(1))
			Expect(newCluster.Segments[0].DataDir).To(Equal("/data/gpseg-1"))
			Expect(newCluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(newCluster.Segments[1].DataDir).To(Equal("/data/gpseg0"))
			Expect(newCluster.GetHostForContent(0)).To(Equal("localhost"))
		})
		It("sets up the configuration for a single-host, multi-segment cluster", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne, localSegTwo})
			Expect(len(newCluster.ContentIDs)).To(Equal(3))
			Expect(len(newCluster.Hostnames)).To(Equal(1))
			Expect(newCluster.Segments[0].DataDir).To(Equal("/data/gpseg-1"))
			Expect(newCluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(newCluster.Segments[1].DataDir).To(Equal("/data/gpseg0"))
			Expect(newCluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(newCluster.Segments[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(newCluster.GetHostForContent(2)).To(Equal("localhost"))
		})
		It("sets up the configuration for a multi-host, multi-segment cluster", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, localSegOne, remoteSegTwo})
			Expect(len(newCluster.ContentIDs)).To(Equal(3))
			Expect(len(newCluster.Hostnames)).To(Equal(2))
			Expect(newCluster.Segments[0].DataDir).To(Equal("/data/gpseg-1"))
			Expect(newCluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(newCluster.Segments[1].DataDir).To(Equal("/data/gpseg0"))
			Expect(newCluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(newCluster.Segments[2].DataDir).To(Equal("/data/gpseg3"))
			Expect(newCluster.GetHostForContent(3)).To(Equal("remotehost2"))
		})
		It("ensures that modifying a segment value in Segments is reflected in ByContent and ByHost", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg})
			newCluster.Segments[0].DataDir = "/new/dir"
			Expect(newCluster.GetDirForContent(-1)).To(Equal("/new/dir"))
			Expect(newCluster.GetDirsForHost("localhost")[0]).To(Equal("/new/dir"))
		})
		It("ensures that modifying a segment value in ByContent is reflected in Segments and ByHost", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg})
			newCluster.ByContent[-1][0].DataDir = "/new/dir"
			Expect(newCluster.Segments[0].DataDir).To(Equal("/new/dir"))
			Expect(newCluster.GetDirsForHost("localhost")[0]).To(Equal("/new/dir"))
		})
		It("ensures that modifying a segment value in ByHost is reflected in Segments and ByContent", func() {
			newCluster := cluster.NewCluster([]cluster.SegConfig{coordinatorSeg})
			newCluster.ByHost["localhost"][0].DataDir = "/new/dir"
			Expect(newCluster.Segments[0].DataDir).To(Equal("/new/dir"))
			Expect(newCluster.GetDirForContent(-1)).To(Equal("/new/dir"))
		})
	})
	Describe("Accessor functions", func() {
		var mirrorCluster *cluster.Cluster
		BeforeEach(func() {
			primary := cluster.SegConfig{DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/primary/gpseg0"}
			mirror := cluster.SegConfig{DbID: 3, ContentID: 0, Port: 21000, Hostname: "otherhost", DataDir: "/data/mirror/gpseg0"}
			mirrorCluster = cluster.NewCluster([]cluster.SegConfig{coordinatorSeg, primary, mirror})
		})
		It("returns primary information for a segment by default", func() {
			Expect(mirrorCluster.GetDbidForContent(0)).To(Equal(2))
			Expect(mirrorCluster.GetPortForContent(0)).To(Equal(20000))
			Expect(mirrorCluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(mirrorCluster.GetDirForContent(0)).To(Equal("/data/primary/gpseg0"))
		})
		It("returns primary information for a segment if primary information is requested", func() {
			Expect(mirrorCluster.GetDbidForContent(0, "p")).To(Equal(2))
			Expect(mirrorCluster.GetPortForContent(0, "p")).To(Equal(20000))
			Expect(mirrorCluster.GetHostForContent(0, "p")).To(Equal("localhost"))
			Expect(mirrorCluster.GetDirForContent(0, "p")).To(Equal("/data/primary/gpseg0"))
		})
		It("returns mirror information for a segment if mirror information is requested", func() {
			Expect(mirrorCluster.GetDbidForContent(0, "m")).To(Equal(3))
			Expect(mirrorCluster.GetPortForContent(0, "m")).To(Equal(21000))
			Expect(mirrorCluster.GetHostForContent(0, "m")).To(Equal("otherhost"))
			Expect(mirrorCluster.GetDirForContent(0, "m")).To(Equal("/data/mirror/gpseg0"))
		})
		It("returns information for a host", func() {
			Expect(mirrorCluster.GetDbidsForHost("localhost")).To(Equal([]int{1, 2}))
			Expect(mirrorCluster.GetContentsForHost("localhost")).To(Equal([]int{-1, 0}))
			Expect(mirrorCluster.GetPortsForHost("localhost")).To(Equal([]int{5432, 20000}))
			Expect(mirrorCluster.GetDirsForHost("localhost")).To(Equal([]string{"/data/gpseg-1", "/data/primary/gpseg0"}))
		})
	})
})

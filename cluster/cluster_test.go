package cluster_test

import (
	"database/sql/driver"
	"fmt"
	"os"
	"os/user"
	"reflect"
	"testing"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
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
	stdOut     *gbytes.Buffer
	stdErr     *gbytes.Buffer
)

func expectPathToExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Fail(fmt.Sprintf("Expected %s to exist", path))
	}
}

var _ = BeforeSuite(func() {
	_, _, stdOut, stdErr, logfile = testhelper.SetupTestEnvironment()
	gplog.SetVerbosity(gplog.LOGVERBOSE)
})

var _ = BeforeEach(func() {
	connection, mock = testhelper.CreateAndConnectMockDB(1)
})

var _ = Describe("cluster/cluster tests", func() {
	masterSeg := cluster.SegConfig{DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{DbID: 3, ContentID: 1, Port: 20001, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	localSegTwo := cluster.SegConfig{DbID: 4, ContentID: 2, Port: 20002, Hostname: "localhost", DataDir: "/data/gpseg2"}
	remoteSegTwo := cluster.SegConfig{DbID: 5, ContentID: 3, Port: 20003, Hostname: "remotehost2", DataDir: "/data/gpseg3"}
	var (
		testCluster  *cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
	})
	Describe("ConstructCopyCommand", func() {
		It("constructs a copy-to-remote src command", func() {
			cmd, _ := cluster.ConstructCopyCommand("testHost", "srcPath", "targetHost", "targetPath")
			Expect(cmd).To(Equal([]string{"rsync", "-az", "ssh -o StrictHostKeyChecking=no", "srcPath", "testUser@targetHost:targetPath"}))
		})
		It("constructs a copy-from-remote src command", func() {
			cmd, _ := cluster.ConstructCopyCommand("srcHost", "srcPath", "testHost", "targetPath")
			Expect(cmd).To(Equal([]string{"rsync", "-az", "ssh -o StrictHostKeyChecking=no", "testUser@srcHost:srcPath", "targetPath"}))
		})
		It("constructs a copy-from-remote src command", func() {
			_, err := cluster.ConstructCopyCommand("srcHost", "srcPath", "targetHost", "targetPath")
			Expect(err.Error()).To(Equal("Cannot copy between two remote servers"))
		})
	})
	Describe("ConstructSSHCommand", func() {
		It("constructs an ssh command", func() {
			cmd, _ := cluster.ConstructSSHCommand("some-host", "ls")
			Expect(cmd).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@some-host", "ls"}))
		})
	})
	Describe("GetSegmentConfiguration", func() {
		header := []string{"contentid", "hostname", "datadir"}
		localSegOne := []driver.Value{"0", "localhost", "/data/gpseg0"}
		localSegTwo := []driver.Value{"1", "localhost", "/data/gpseg1"}
		remoteSegOne := []driver.Value{"2", "remotehost", "/data/gpseg2"}

		It("returns a configuration for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results, err := cluster.GetSegmentConfiguration(connection)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a single-host, multi-segment cluster", func() {
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
		It("returns a configuration for a multi-host, multi-segment cluster", func() {
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
	})
	Describe("Generating and Executing Commands", func() {
		masterSegmentCopyPath := func(contentID int) string {
			return fmt.Sprintf("/home/testUser/dataOnMaster/%d", contentID)
		}
		remoteSegmentCopyPath := func(contentID int) string {
			return fmt.Sprintf("/home/testUser/dataOnSegment/%d", contentID)
		}
		masterHostCopyPath := func(hostname string) string {
			return fmt.Sprintf("/home/testUser/dataOnMaster/%s", hostname)
		}
		remoteHostCopyPath := func(hostname string) string {
			return fmt.Sprintf("/home/testUser/dataOnSegment/%s", hostname)
		}
		sshHostCommand := func(hostname string) string {
			return fmt.Sprintf("echo %s", hostname)
		}
		sshSegmentCommand := func(contentID int) string {
			return fmt.Sprintf("echo %d", contentID)
		}
		Describe("GenerateAndExecuteSegment*", func() {
			BeforeEach(func() {
				testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, localSegTwo, remoteSegOne, remoteSegTwo})
				testCluster.Executor = testExecutor
			})
			Describe("GenerateAndExecuteSegmentCommand", func() {
				It("Generates a remoteOutput with the correct commands in it", func() {
					_, err := testCluster.GenerateAndExecuteSegmentCommand("test verbose message", sshSegmentCommand, true)
					defer Expect(stdOut).To(gbytes.Say("test verbose message"))
					Expect(err).To(BeNil())
					Expect(testExecutor.ClusterCommands[0]).To(Equal(map[int][]string{
						-1: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@localhost",
							"echo -1"},
						0: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@localhost",
							"echo 0"},
						1: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@remotehost1",
							"echo 1"},
						2: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@localhost",
							"echo 2"},
						3: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@remotehost2",
							"echo 3"},
					}))
				})
			})
		})
		Describe("GenerateAndExecuteSegmentCopy", func() {
			It("Generates a remoteOutput with the correct commands in it", func() {
				testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, remoteSegOne, remoteSegTwo})
				testCluster.Executor = testExecutor
				_, err := testCluster.GenerateAndExecuteSegmentCopy("test verbose message", masterSegmentCopyPath, remoteSegmentCopyPath, cluster.TO_MASTER)
				Expect(err).To(BeNil())
				defer Expect(stdOut).To(gbytes.Say("test verbose message"))
				Expect(testExecutor.ClusterCommands[0]).To(Equal(map[int][]string{
					1: {"rsync",
						"-az",
						"ssh -o StrictHostKeyChecking=no",
						"testUser@remotehost1:/home/testUser/dataOnSegment/1",
						"/home/testUser/dataOnMaster/1"},
					3: {"rsync",
						"-az",
						"ssh -o StrictHostKeyChecking=no",
						"testUser@remotehost2:/home/testUser/dataOnSegment/3",
						"/home/testUser/dataOnMaster/3"},
				}))
			})
		})
		Describe("GenerateAndExecuteHost*", func() {
			BeforeEach(func() {
				testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, remoteSegOne, remoteSegTwo})
				testCluster.Executor = testExecutor
			})
			Describe("GenerateAndExecuteHostCommand", func() {
				It("Generates a remoteOutput with the correct commands in it", func() {
					_, err := testCluster.GenerateAndExecuteHostCommand("test verbose message", sshHostCommand, true)
					defer Expect(stdOut).To(gbytes.Say("test verbose message"))
					Expect(err).To(BeNil())
					Expect(testExecutor.ClusterCommands[0]).To(Equal(map[int][]string{
						-1: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@localhost",
							"echo localhost"},
						1: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@remotehost1",
							"echo remotehost1"},
						3: {"ssh",
							"-o",
							"StrictHostKeyChecking=no",
							"testUser@remotehost2",
							"echo remotehost2"},
					}))
				})
			})
			Describe("GenerateAndExecuteHostCopy", func() {
				It("Generates a remoteOutput with the correct commands in it", func() {
					_, err := testCluster.GenerateAndExecuteHostCopy("test verbose message", masterHostCopyPath, remoteHostCopyPath, cluster.TO_MASTER)
					defer Expect(stdOut).To(gbytes.Say("test verbose message"))
					Expect(err).To(BeNil())
					Expect(testExecutor.ClusterCommands[0]).To(Equal(map[int][]string{
						1: {"rsync",
							"-az",
							"ssh -o StrictHostKeyChecking=no",
							"testUser@remotehost1:/home/testUser/dataOnSegment/remotehost1",
							"/home/testUser/dataOnMaster/remotehost1"},
						3: {"rsync",
							"-az",
							"ssh -o StrictHostKeyChecking=no",
							"testUser@remotehost2:/home/testUser/dataOnSegment/remotehost2",
							"/home/testUser/dataOnMaster/remotehost2"},
					}))
				})
				It("Errors if an invalid direction is used", func() {
					remoteOut, err := testCluster.GenerateAndExecuteHostCopy("test verbose message",
						masterHostCopyPath,
						remoteHostCopyPath,
						42)
					Expect(stdErr).To(gbytes.Say("Copy commands must use cluster.TO_MASTER or cluster.FROM_MASTER"))
					Expect(remoteOut).To(BeNil())
					Expect(err.Error()).To(Equal("Copy commands must use cluster.TO_MASTER or cluster.FROM_MASTER"))
				})
			})
		})
		Describe("GenerateAndExecuteCommandMap", func() {
			var hostCopyCommands *cluster.HostCommands
			var segmentCopyCommands *cluster.SegmentCommands
			var hostSSHCommands *cluster.HostCommands
			var segmentSSHCommands *cluster.SegmentCommands
			BeforeEach(func() {
				hostCopyCommands = &cluster.HostCommands{
					Commands:    make(map[int][]string),
					Hostnames:   make(map[string]int),
					MasterPath:  masterHostCopyPath,
					RemotePath:  remoteHostCopyPath,
					Direction:   cluster.TO_MASTER,
					Scope:       cluster.ON_HOSTS,
					CommandType: cluster.COPY}
				segmentCopyCommands = &cluster.SegmentCommands{
					Commands:    make(map[int][]string),
					MasterPath:  masterSegmentCopyPath,
					RemotePath:  remoteSegmentCopyPath,
					Direction:   cluster.TO_MASTER,
					Scope:       cluster.ON_SEGMENTS,
					CommandType: cluster.COPY}
				hostSSHCommands = &cluster.HostCommands{
					Commands:    make(map[int][]string),
					Hostnames:   make(map[string]int),
					SSHCommand:  sshHostCommand,
					Scope:       cluster.ON_HOSTS,
					CommandType: cluster.SSH}
				segmentSSHCommands = &cluster.SegmentCommands{
					Commands:    make(map[int][]string),
					SSHCommand:  sshSegmentCommand,
					Scope:       cluster.ON_SEGMENTS,
					CommandType: cluster.SSH}
			})
			SetUpAndTest := func(segConfigs []cluster.SegConfig, commands []cluster.Commands, length int, responseExpected []bool) {
				testCluster = cluster.NewCluster(segConfigs)
				testCluster.Executor = testExecutor

				for _, cmd := range commands {
					_, _ = testCluster.GenerateAndExecuteCommandMap(cmd)
					commandMap := cmd.GetCommands()
					var results, expectedResults [][]string
					Expect(len(commandMap)).To(Equal(length))
					for index, seg := range segConfigs {
						if commandMap[seg.ContentID] != nil {
							results = append(results, commandMap[seg.ContentID])
						}
						if !responseExpected[index] {
							continue
						}
						switch cmd.GetCommandType() {
						case cluster.COPY:
							var source string
							var target string
							switch reflect.TypeOf(cmd) {
							case reflect.TypeOf(&cluster.HostCommands{}):
								switch cmd.GetDirection() {
								case cluster.TO_MASTER:
									source = fmt.Sprintf("testUser@%s:/home/testUser/dataOnSegment/%s", seg.Hostname, seg.Hostname)
									target = fmt.Sprintf("/home/testUser/dataOnMaster/%s", seg.Hostname)
								case cluster.FROM_MASTER:
									source = fmt.Sprintf("/home/testUser/dataOnMaster/%s", seg.Hostname)
									target = fmt.Sprintf("testUser@%s:/home/testUser/dataOnSegment/%s", seg.Hostname, seg.Hostname)
								}
							case reflect.TypeOf(&cluster.SegmentCommands{}):
								switch cmd.GetDirection() {
								case cluster.TO_MASTER:
									source = fmt.Sprintf("testUser@%s:/home/testUser/dataOnSegment/%d", seg.Hostname, seg.ContentID)
									target = fmt.Sprintf("/home/testUser/dataOnMaster/%d", seg.ContentID)
								case cluster.FROM_MASTER:
									source = fmt.Sprintf("/home/testUser/dataOnMaster/%d", seg.ContentID)
									target = fmt.Sprintf("testUser@%s:/home/testUser/dataOnSegment/%d", seg.Hostname, seg.ContentID)
								}
							}
							expectedResults = append(expectedResults, []string{"rsync", "-az", "ssh -o StrictHostKeyChecking=no", source, target})
						case cluster.SSH:
							var target string
							var command string
							switch reflect.TypeOf(cmd) {
							case reflect.TypeOf(&cluster.HostCommands{}):
								target = fmt.Sprintf("testUser@%s", seg.Hostname)
								command = fmt.Sprintf("echo %s", seg.Hostname)
							case reflect.TypeOf(&cluster.SegmentCommands{}):
								target = fmt.Sprintf("testUser@%s", seg.Hostname)
								command = fmt.Sprintf("echo %d", seg.ContentID)
							}
							expectedResults = append(expectedResults, []string{"ssh", "-o", "StrictHostKeyChecking=no", target, command})
						}
					}
					Expect(results).To(Equal(expectedResults))
				}
			}
			It("Generates an SSH command map for master, including master", func() {
				segmentSSHCommands.Scope = cluster.ON_SEGMENTS_AND_MASTER
				hostSSHCommands.Scope = cluster.ON_HOSTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg},
					[]cluster.Commands{hostSSHCommands, segmentSSHCommands},
					1,
					[]bool{true},
				)
			})
			It("Generates a COPY/SSH command map for master, excluding master", func() {
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg},
					[]cluster.Commands{hostCopyCommands,
						segmentCopyCommands,
						hostSSHCommands,
						segmentSSHCommands},
					0,
					[]bool{false},
				)
			})
			It("Generates an SSH command map for a remote segment, including master", func() {
				segmentSSHCommands.Scope = cluster.ON_SEGMENTS_AND_MASTER
				hostSSHCommands.Scope = cluster.ON_HOSTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, remoteSegOne},
					[]cluster.Commands{hostSSHCommands,
						segmentSSHCommands},
					2,
					[]bool{true, true},
				)
			})
			It("Generates an SSH/COPY command map for a remote segment, excluding master", func() {
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, remoteSegOne},
					[]cluster.Commands{hostCopyCommands,
						segmentCopyCommands,
						hostSSHCommands,
						segmentSSHCommands},
					1,
					[]bool{false, true},
				)
			})
			It("Generates a map of SSH commands for two segments on master's host, including master", func() {
				segmentSSHCommands.Scope = cluster.ON_SEGMENTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne},
					[]cluster.Commands{segmentSSHCommands},
					2,
					[]bool{true, true},
				)
				hostSSHCommands.Scope = cluster.ON_HOSTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne},
					[]cluster.Commands{hostSSHCommands},
					1,
					[]bool{false, true},
				)
			})
			It("Generates a map of SSH/COPY commands for two segments on master's host, excluding master", func() {
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne},
					[]cluster.Commands{hostCopyCommands,
						segmentCopyCommands,
						hostSSHCommands,
						segmentSSHCommands},
					1,
					[]bool{false, true},
				)
			})
			It("Generates a map of SSH commands for three segments on three different hosts, including master", func() {
				segmentSSHCommands.Scope = cluster.ON_SEGMENTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{segmentSSHCommands},
					3,
					[]bool{true, true, true},
				)
				hostSSHCommands.Scope = cluster.ON_HOSTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{hostSSHCommands},
					3,
					[]bool{true, true, true},
				)
			})
			It("Generates a map of SSH/COPY commands for three segments on three different hosts, excluding master", func() {
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{hostCopyCommands,
						segmentCopyCommands,
						hostSSHCommands,
						segmentSSHCommands},
					2,
					[]bool{false, true, true},
				)
			})
			It("Generates a map of SSH commands for	two local segments and two remote segments on different hosts, including master", func() {
				segmentSSHCommands.Scope = cluster.ON_SEGMENTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{segmentSSHCommands},
					4,
					[]bool{true, true, true, true},
				)
				hostSSHCommands.Scope = cluster.ON_HOSTS_AND_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{hostSSHCommands},
					3,
					[]bool{false, true, true, true},
				)
			})
			It("Generates a map of SSH commands for	two local segments and two remote segments on different hosts, excluding master", func() {
				SetUpAndTest(
					[]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne, remoteSegTwo},
					[]cluster.Commands{hostCopyCommands,
						segmentCopyCommands,
						hostSSHCommands,
						segmentSSHCommands},
					3,
					[]bool{false, true, true, true},
				)
			})
			It("Generates copy commands to master", func() {
				hostCopyCommands.Direction = cluster.TO_MASTER
				segmentCopyCommands.Direction = cluster.TO_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{remoteSegOne},
					[]cluster.Commands{hostCopyCommands, segmentCopyCommands},
					1,
					[]bool{true},
				)
			})
			It("Generates copy commands from master", func() {
				hostCopyCommands.Direction = cluster.FROM_MASTER
				segmentCopyCommands.Direction = cluster.FROM_MASTER
				SetUpAndTest(
					[]cluster.SegConfig{remoteSegOne},
					[]cluster.Commands{hostCopyCommands, segmentCopyCommands},
					1,
					[]bool{true},
				)
			})
		})
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
		It("runs commands specified by command map", func() {
			testCluster := cluster.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/gp_common_go_libs_test/foo"},
				0:  {"touch", "/tmp/gp_common_go_libs_test/baz"},
			}
			testCluster.Executor = &cluster.GPDBExecutor{}
			testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS_AND_MASTER, commandMap)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			expectPathToExist("/tmp/gp_common_go_libs_test/baz")
		})
		It("returns any errors generated by any of the commands", func() {
			testCluster := cluster.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/gp_common_go_libs_test/foo"},
				0:  {"some-non-existent-command"},
			}
			testCluster.Executor = &cluster.GPDBExecutor{}
			clusterOutput := testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS_AND_MASTER, commandMap)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			Expect(clusterOutput.NumErrors).To(Equal(1))
			Expect(clusterOutput.Errors[0].Error()).To(Equal("exec: \"some-non-existent-command\": executable file not found in $PATH"))
		})
	})
	Describe("CheckClusterError", func() {
		It("prints error messages for a command executed on a per-segment basis", func() {
			remoteOutput := &cluster.RemoteOutput{
				Scope:     cluster.ON_SEGMENTS,
				NumErrors: 1,
				Stderrs: map[int]string{
					1: "exit status 1",
				},
				Errors: map[int]error{
					1: errors.Errorf("ssh error"),
				},
				CmdStrs: map[int]string{
					1: "this is the command",
				},
			}
			defer testhelper.ShouldPanicWithMessage("Got an error on 1 segment. See gbytes.Buffer for a complete list of errors.")
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Command was: this is the command`))
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Error received on segment 1 on host remotehost1 with error ssh error: exit status 1`))
			testCluster.CheckClusterError(remoteOutput, "Got an error", func(contentID int) string {
				return "Error received"
			})
		})
		It("prints error messages for a command executed on a per-host basis", func() {
			remoteOutput := &cluster.RemoteOutput{
				Scope:     cluster.ON_HOSTS,
				NumErrors: 1,
				Stderrs: map[int]string{
					1: "exit status 1",
				},
				Errors: map[int]error{
					1: errors.Errorf("ssh error"),
				},
			}
			defer testhelper.ShouldPanicWithMessage("Got an error on 1 host. See gbytes.Buffer for a complete list of errors.")
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Error received on host remotehost1 with error ssh error: exit status 1`))
			testCluster.CheckClusterError(remoteOutput, "Got an error", func(contentID int) string {
				return "Error received"
			})
		})
		It("prints error messages for a command executed on a per-host and master basis", func() {
			remoteOutput := &cluster.RemoteOutput{
				Scope:     cluster.ON_HOSTS_AND_MASTER,
				NumErrors: 1,
				Stderrs: map[int]string{
					1: "exit status 1",
				},
				Errors: map[int]error{
					1: errors.Errorf("ssh error"),
				},
			}
			defer testhelper.ShouldPanicWithMessage("Got an error on 1 host. See gbytes.Buffer for a complete list of errors.")
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Error received on host remotehost1 with error ssh error: exit status 1`))
			testCluster.CheckClusterError(remoteOutput, "Got an error", func(contentID int) string {
				return "Error received"
			})
		})
	})
	Describe("LogFatalClusterError", func() {
		It("logs an error for 1 segment (with master)", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 1 segment. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS_AND_MASTER, 1)
		})
		It("logs an error for more than 1 segment", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 2 segments. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS, 2)
		})
		It("logs an error for 1 host", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 1 host. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_HOSTS, 1)
		})
		It("logs an error for more than 1 host (with master)", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 2 hosts. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_HOSTS_AND_MASTER, 2)
		})
	})
	Describe("cluster setup and accessor functions", func() {
		It("returns content dir for a single-host, single-segment cluster", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			Expect(len(cluster.GetContentList())).To(Equal(2))
			Expect(cluster.Segments[-1].DataDir).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.Segments[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
		})
		It("sets up the configuration for a single-host, multi-segment cluster", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, localSegTwo})
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.Segments[-1].DataDir).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.Segments[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.Segments[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(cluster.GetHostForContent(2)).To(Equal("localhost"))
		})
		It("sets up the configuration for a multi-host, multi-segment cluster", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegTwo})
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.Segments[-1].DataDir).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.Segments[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.Segments[3].DataDir).To(Equal("/data/gpseg3"))
			Expect(cluster.GetHostForContent(3)).To(Equal("remotehost2"))
		})
	})
})

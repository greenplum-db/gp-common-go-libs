package cluster

/*
 * This file contains structs and functions related to interacting
 * with files and directories, both locally and remotely over SSH.
 */

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

type Executor interface {
	ExecuteLocalCommand(commandStr string) (string, error)
	ExecuteClusterCommand(scope int, commandMap map[int][]string) *RemoteOutput
}

// GPDBExecutor only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

type Cluster struct {
	ContentIDs []int
	Segments   map[int]SegConfig
	Executor
}

type SegConfig struct {
	DbID      int
	ContentID int
	Port      int
	Hostname  string
	DataDir   string
}

/*
 * We pass values from this enum into GenerateAndExecuteCommand to define the
 * scope for remote command execution.
 * - ON_SEGMENTS: Execute on each segment, excluding the master.
 * - ON_SEGMENTS_AND_MASTER: Execute on each segment, including the master.
 * - ON_HOSTS: Execute on each host, excluding the master host.
 * - ON_HOSTS_AND_MASTER: Execute on each host, including the master host.
 */
const (
	ON_SEGMENTS = iota
	ON_SEGMENTS_AND_MASTER
	ON_HOSTS
	ON_HOSTS_AND_MASTER
)

/*
 * We pass values from this enum into GenerateAndExecuteCopy to define the
 * scope for remote command execution.
 * - FROM_MASTER: Copy files from specified path on master to segments.
 * - TO_MASTER: Copy files from specified path on segments to master.
 */
const (
	TO_MASTER = iota
	FROM_MASTER
)

type RemoteOutput struct {
	Scope     int
	NumErrors int
	Stdouts   map[int]string
	Stderrs   map[int]string
	Errors    map[int]error
	CmdStrs   map[int]string
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) *Cluster {
	cluster := Cluster{}
	cluster.Segments = make(map[int]SegConfig, len(segConfigs))
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.Segments[seg.ContentID] = seg
	}
	cluster.Executor = &GPDBExecutor{}
	return &cluster
}

const (
	COPY = iota
	SSH
)

type Commands interface {
	GenerateCopyCommand(*Cluster, int) error
	GenerateSSHCommand(*Cluster, int) error
	GetCommandType() int
	GetCommands() map[int][]string
	GetDirection() int
	GetScope() int
}

type HostCommands struct {
	Commands    map[int][]string
	Hostnames   map[string]int
	MasterPath  func(string) string
	RemotePath  func(string) string
	SSHCommand  func(string) string
	Direction   int
	Scope       int
	CommandType int
}

type SegmentCommands struct {
	Commands    map[int][]string
	MasterPath  func(int) string
	RemotePath  func(int) string
	SSHCommand  func(int) string
	Direction   int
	Scope       int
	CommandType int
}

func (h *HostCommands) GetScope() int {
	return h.Scope
}

func (s *SegmentCommands) GetScope() int {
	return s.Scope
}

func (h *HostCommands) GetCommandType() int {
	return h.CommandType
}

func (s *SegmentCommands) GetCommandType() int {
	return s.CommandType
}

func (h *HostCommands) GetCommands() map[int][]string {
	return h.Commands
}

func (s *SegmentCommands) GetCommands() map[int][]string {
	return s.Commands
}

func (h *HostCommands) GetDirection() int {
	return h.Direction
}

func (s *SegmentCommands) GetDirection() int {
	return s.Direction
}

func (h *HostCommands) GenerateCopyCommand(cluster *Cluster, contentID int) error {
	contentHost := cluster.GetHostForContent(contentID)
	h.Hostnames[contentHost]++
	if h.Hostnames[contentHost] > 1 {
		return nil
	}
	var err error
	h.Commands[contentID], err = cluster.copyCommand(contentID, h.MasterPath(contentHost), h.RemotePath(contentHost), h.Direction)
	return err
}

func (s *SegmentCommands) GenerateCopyCommand(cluster *Cluster, contentID int) error {
	var err error
	s.Commands[contentID], err = cluster.copyCommand(contentID, s.MasterPath(contentID), s.RemotePath(contentID), s.Direction)
	return err
}

func (c *Cluster) copyCommand(contentID int, masterPath string, segmentPath string, direction int) ([]string, error) {
	masterHost, err := operating.System.Hostname()
	if err != nil {
		return nil, err
	}
	var cmd []string
	segmentHost := c.GetHostForContent(contentID)
	switch direction {
	case TO_MASTER:
		cmd, err = ConstructCopyCommand(segmentHost, segmentPath, masterHost, masterPath)
	case FROM_MASTER:
		cmd, err = ConstructCopyCommand(masterHost, masterPath, segmentHost, segmentPath)
	}
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (h *HostCommands) GenerateSSHCommand(cluster *Cluster, contentID int) error {
	contentHost := cluster.GetHostForContent(contentID)
	h.Hostnames[contentHost]++
	if h.Hostnames[contentHost] > 1 {
		return nil
	}
	var err error
	h.Commands[contentID], err = ConstructSSHCommand(cluster.GetHostForContent(contentID), h.SSHCommand(contentHost))
	return err
}

func (s *SegmentCommands) GenerateSSHCommand(cluster *Cluster, contentID int) error {
	var err error
	s.Commands[contentID], err = ConstructSSHCommand(cluster.GetHostForContent(contentID), s.SSHCommand(contentID))
	return err
}

func (c *Cluster) GenerateAndExecuteCommandMap(commands Commands) (*RemoteOutput, error) {
	var err error
	for contentID := range c.Segments {
		if contentID == -1 && (commands.GetScope() == ON_SEGMENTS || commands.GetScope() == ON_HOSTS) {
			continue
		}
		switch commands.GetCommandType() {
		case COPY:
			err = commands.GenerateCopyCommand(c, contentID)
		case SSH:
			err = commands.GenerateSSHCommand(c, contentID)
		}
	}
	return c.ExecuteClusterCommand(commands.GetScope(), commands.GetCommands()), err
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	output, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return string(output), err
}

func newRemoteOutput(scope int, numIDs int) *RemoteOutput {
	stdout := make(map[int]string, numIDs)
	stderr := make(map[int]string, numIDs)
	err := make(map[int]error, numIDs)
	cmdStr := make(map[int]string, numIDs)
	return &RemoteOutput{Scope: scope, NumErrors: 0, Stdouts: stdout, Stderrs: stderr, Errors: err, CmdStrs: cmdStr}
}

func (executor *GPDBExecutor) ExecuteClusterCommand(scope int, commandMap map[int][]string) *RemoteOutput {
	length := len(commandMap)
	finished := make(chan int)
	contentIDs := make([]int, length)
	i := 0
	for key := range commandMap {
		contentIDs[i] = key
		i++
	}
	output := newRemoteOutput(scope, length)
	stdouts := make([]string, length)
	stderrs := make([]string, length)
	errors := make([]error, length)
	for i, contentID := range contentIDs {
		go func(index int, segCommand []string) {
			var stderr bytes.Buffer
			cmd := exec.Command(segCommand[0], segCommand[1:]...)
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			stdouts[index] = string(out)
			stderrs[index] = stderr.String()
			errors[index] = err
			finished <- index
		}(i, commandMap[contentID])
	}
	for i := 0; i < length; i++ {
		index := <-finished
		id := contentIDs[index]
		output.Stdouts[id] = stdouts[index]
		output.Stderrs[id] = stderrs[index]
		output.Errors[id] = errors[index]
		output.CmdStrs[id] = strings.Join(commandMap[id], " ")
		if output.Errors[id] != nil {
			output.NumErrors++
		}
	}
	return output
}

/*
 * GenerateAndExecuteHostCommand, GenerateAndExecuteSegmentCommand, and CheckClusterError are generic wrapper functions
 * to simplify execution of shell commands on remote hosts.
 */
func (c *Cluster) GenerateAndExecuteHostCommand(verboseMsg string, execFunc func(string) string, includeMaster bool) (*RemoteOutput, error) {
	scope := ON_HOSTS
	if includeMaster {
		scope = ON_HOSTS_AND_MASTER
	}
	gplog.Verbose(verboseMsg)
	commands := &HostCommands{
		Commands:    make(map[int][]string),
		Hostnames:   make(map[string]int),
		SSHCommand:  execFunc,
		Scope:       scope,
		CommandType: SSH,
	}
	return c.GenerateAndExecuteCommandMap(commands)
}

func (c *Cluster) GenerateAndExecuteSegmentCommand(verboseMsg string, execFunc func(int) string, includeMaster bool) (*RemoteOutput, error) {
	scope := ON_SEGMENTS
	if includeMaster {
		scope = ON_SEGMENTS_AND_MASTER
	}
	gplog.Verbose(verboseMsg)
	commands := &SegmentCommands{
		Commands:    make(map[int][]string),
		SSHCommand:  execFunc,
		Scope:       scope,
		CommandType: SSH,
	}
	return c.GenerateAndExecuteCommandMap(commands)
}

func (c *Cluster) GenerateAndExecuteHostCopy(verboseMsg string, masterPathFunc func(string) string, remotePathFunc func(string) string, direction int) (*RemoteOutput, error) {
	gplog.Verbose(verboseMsg)
	if direction != TO_MASTER && direction != FROM_MASTER {
		err := errors.New("Copy commands must use cluster.TO_MASTER or cluster.FROM_MASTER")
		gplog.Error(err.Error(), "")
		return nil, err
	}
	commands := &HostCommands{
		Commands:    make(map[int][]string),
		Hostnames:   make(map[string]int),
		MasterPath:  masterPathFunc,
		RemotePath:  remotePathFunc,
		Direction:   direction,
		Scope:       ON_HOSTS,
		CommandType: COPY}
	return c.GenerateAndExecuteCommandMap(commands)
}

func (c *Cluster) GenerateAndExecuteSegmentCopy(verboseMsg string, masterPathFunc func(int) string, remotePathFunc func(int) string, direction int) (*RemoteOutput, error) {
	gplog.Verbose(verboseMsg)
	if direction != TO_MASTER && direction != FROM_MASTER {
		err := errors.New("Copy commands must use cluster.TO_MASTER or cluster.FROM_MASTER")
		gplog.Error(err.Error(), "")
		return nil, err
	}
	commands := &SegmentCommands{
		Commands:    make(map[int][]string),
		MasterPath:  masterPathFunc,
		RemotePath:  remotePathFunc,
		Direction:   direction,
		Scope:       ON_SEGMENTS,
		CommandType: COPY}
	return c.GenerateAndExecuteCommandMap(commands)
}

func (c *Cluster) CheckClusterError(remoteOutput *RemoteOutput, finalErrMsg string, messageFunc func(contentID int) string, noFatal ...bool) {
	if remoteOutput.NumErrors == 0 {
		return
	}

	for contentID, err := range remoteOutput.Errors {
		if err != nil {
			segMsg := ""
			if remoteOutput.Scope != ON_HOSTS && remoteOutput.Scope != ON_HOSTS_AND_MASTER {
				segMsg = fmt.Sprintf("on segment %d ", contentID)
			}
			gplog.Verbose("%s %son host %s with error %s: %s", messageFunc(contentID), segMsg, c.GetHostForContent(contentID), err, remoteOutput.Stderrs[contentID])
			gplog.Verbose("Command was: %s", remoteOutput.CmdStrs[contentID])
		}
	}
	if len(noFatal) == 1 && noFatal[0] == true {
		gplog.Error(finalErrMsg)
	} else {
		LogFatalClusterError(finalErrMsg, remoteOutput.Scope, remoteOutput.NumErrors)
	}
}

func LogFatalClusterError(errMessage string, scope int, numErrors int) {
	segMsg := "segment"
	if scope == ON_HOSTS || scope == ON_HOSTS_AND_MASTER {
		segMsg = "host"
	}
	if numErrors != 1 {
		segMsg += "s"
	}
	gplog.Fatal(errors.Errorf("%s on %d %s. See %s for a complete list of errors.", errMessage, numErrors, segMsg, gplog.GetLogFilePath()), "")
}

func (c *Cluster) GetContentList() []int {
	return c.ContentIDs
}

func (c *Cluster) GetDbidForContent(contentID int) int {
	return c.Segments[contentID].DbID
}

func (c *Cluster) GetPortForContent(contentID int) int {
	return c.Segments[contentID].Port
}

func (c *Cluster) GetHostForContent(contentID int) string {
	return c.Segments[contentID].Hostname
}

func (c *Cluster) GetDirForContent(contentID int) string {
	return c.Segments[contentID].DataDir
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *dbconn.DBConn) ([]SegConfig, error) {
	query := ""
	if connection.Version.Before("6") {
		query = `
SELECT
	s.dbid,
	s.content as contentid,
	s.port,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	dbid,
	content as contentid,
	port,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content;`
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func MustGetSegmentConfiguration(connection *dbconn.DBConn) []SegConfig {
	segConfigs, err := GetSegmentConfiguration(connection)
	gplog.FatalOnError(err)
	return segConfigs
}

func ConstructSSHCommand(host string, cmd string) ([]string, error) {
	currentUser, err := operating.System.CurrentUser()
	if err != nil {
		return nil, err
	}
	user := currentUser.Username
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", user, host), cmd}, nil
}

func ConstructCopyCommand(srcHost string, srcPath string, targetHost string, targetPath string) ([]string, error) {
	currentUser, err := operating.System.CurrentUser()
	if err != nil {
		return nil, err
	}
	currentHost, err := operating.System.Hostname()
	if err != nil {
		return nil, err
	}
	user := currentUser.Username
	if targetHost != currentHost {
		if srcHost != currentHost {
			return nil, errors.New("Cannot copy between two remote servers")
		}
		return []string{"rsync", "-az", "ssh -o StrictHostKeyChecking=no", srcPath, fmt.Sprintf("%s@%s:%s", user, targetHost, targetPath)}, nil
	}
	return []string{"rsync", "-az", "ssh -o StrictHostKeyChecking=no", fmt.Sprintf("%s@%s:%s", user, srcHost, srcPath), targetPath}, nil
}

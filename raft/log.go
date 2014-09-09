package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// FSM represents the state machine that the log is applied to.
type FSM interface {
	Apply(*LogEntry) error
	Snapshot(io.Writer) error
	Restore(io.Reader) error
}

const logEntryHeaderSize = 8 + 8 + 8 // sz+index+term

// State represents whether the log is a follower, candidate, or leader.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Log represents a replicated log of commands.
type Log struct {
	mu sync.Mutex

	id     uint64  // log identifier
	path   string  // data directory
	state  State   // current node state
	config *Config // cluster configuration

	currentTerm uint64 // current election term
	votedFor    uint64 // candidate voted for in current election term

	leaderID     uint64 // the current leader
	currentIndex uint64 // highest entry written to disk
	commitIndex  uint64 // highest entry to be committed
	appliedIndex uint64 // highest entry to applied to state machine

	nextIndex  map[uint64]uint64 // next entry to send to each follower
	matchIndex map[uint64]uint64 // highest known replicated entry for each follower

	reader  io.ReadCloser // incoming stream from leader
	writers []io.Writer   // outgoing streams to followers

	segment *segment // TODO(benbjohnson): support multiple segments

	// Network address to the reach the log.
	URL *url.URL

	// The state machine that log entries will be applied to.
	FSM FSM

	// The transport used to communicate with other nodes in the cluster.
	// If nil, then the DefaultTransport is used.
	Transport Transport

	// The amount of time between Append Entries RPC calls from the leader to
	// its followers.
	HeartbeatTimeout time.Duration

	// The amount of time before a follower attempts an election.
	ElectionTimeout time.Duration

	// Clock is an abstraction of the time package. By default it will use
	// a real-time clock but a mock clock can be used for testing.
	Clock clock.Clock

	// Rand returns a random number.
	Rand func() int64
}

// Path returns the data path of the Raft log.
// Returns an empty string if the log is closed.
func (l *Log) Path() string { return l.path }

// Opened returns true if the log is currently open.
func (l *Log) Opened() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.opened()
}

func (l *Log) opened() bool { return l.path != "" }

// State returns the current state.
func (l *Log) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Open initializes the log from a path.
// If the path does not exist then it is created.
func (l *Log) Open(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Validate initial log state.
	if l.opened() {
		return ErrAlreadyOpen
	}

	// Create directory, if not exists.
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	l.path = path

	// Setup default clock & random source.
	if l.Clock == nil {
		l.Clock = clock.New()
	}
	if l.Rand == nil {
		l.Rand = rand.Int63
	}

	// Initialize log identifier.
	if err := l.init(); err != nil {
		_ = l.close()
		return err
	}

	// Read config.
	if err := l.restoreConfig(); err != nil {
		_ = l.close()
		return err
	}

	// TEMP(benbjohnson): Create empty log segment.
	l.segment = &segment{
		path:  filepath.Join(l.path, "1.log"),
		index: 0,
	}

	// TODO(benbjohnson): Open log segments.

	// TODO(benbjohnson): Replay latest log.

	return nil
}

// Close closes the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.close()
}

func (l *Log) close() error {
	// TODO(benbjohnson): Shutdown all goroutines.

	// Close the segments.
	_ = l.segment.Close()

	// Clear log info.
	l.id = 0
	l.path = ""

	return nil
}

// init reads the log identifier from file.
// If the file does not exist then an identifier is generated.
func (l *Log) init() error {
	path := filepath.Join(l.path, "id")

	// Generate identifier and write to file if it doesn't exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		l.id = uint64(l.Rand())
		return ioutil.WriteFile(path, []byte(strconv.FormatUint(l.id, 10)), 0600)
	}

	// Read identifier from disk.
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	// Parse identifier.
	l.id, err = strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return err
	}

	return nil
}

// restoreConfig reads the configuration from disk and marshals it into a Config.
func (l *Log) restoreConfig() error {
	// Read config from disk.
	f, err := os.Open(filepath.Join(l.path, "config"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	defer func() { _ = f.Close() }()

	// Marshal file to a config type.
	var config *Config
	if f != nil {
		if err := json.NewDecoder(f).Decode(&config); err != nil {
			return err
		}
	}

	// Set config.
	l.config = config

	return nil
}

// Initialize a new log.
// Returns an error if log data already exists.
func (l *Log) Initialize() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Return error if entries already exist.
	if l.currentIndex > 0 {
		return ErrLogExists
	} else if l.URL == nil {
		return ErrURLRequired
	}

	// Generate a new configuration with one node.
	config := &Config{Nodes: []*Node{&Node{ID: l.id, URL: l.URL}}}

	// Generate new 8-hex digit cluster identifier.
	config.ClusterID = uint64(l.Rand())

	// Automatically promote to leader.
	l.currentTerm = 1
	l.state = Leader

	// Set initial configuration.
	b, _ := json.Marshal(&config)
	if err := l.apply(LogEntryConfig, b); err != nil {
		return err
	}

	return nil
}

// demote moves the log from a candidate or leader state to a follower state.
func (l *Log) demote() {
	l.state = Follower
}

// Apply executes a command against the log.
// This function returns once the command has been committed to the log.
func (l *Log) Apply(command []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.apply(LogEntryCommand, command)
}

func (l *Log) apply(typ LogEntryType, command []byte) error {
	// Do not apply if this node is not the leader.
	if l.state != Leader {
		return ErrNotLeader
	}

	// Create log entry.
	l.currentIndex++
	e := LogEntry{
		Type:  typ,
		Index: l.currentIndex,
		Term:  l.currentTerm,
		Data:  command,
	}

	// Append to the current log segment.
	if err := l.segment.append(&e); err != nil {
		return err
	}

	// TODO(benbjohnson): Wait for consensus.

	// Apply to FSM.
	if err := l.FSM.Apply(&e); err != nil {
		return err
	}

	// TODO(benbjohnson): Add callback.

	return nil
}

// Heartbeat establishes dominance by the current leader.
// Returns the current term and highest written log entry index.
func (l *Log) Heartbeat(term, commitIndex, leaderID uint64) (currentIndex, currentTerm uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return 0, 0, ErrClosed
	}

	// Ignore if the incoming term is less than the log's term.
	if term < l.currentTerm {
		return l.currentTerm, l.currentIndex, nil
	}

	if term > l.currentTerm {
		// TODO(benbjohnson): stepdown
		l.currentTerm = term
	}
	l.commitIndex = commitIndex
	l.leaderID = leaderID

	return l.currentTerm, l.currentIndex, nil
}

// RequestVote requests a vote from the log.
func (l *Log) RequestVote(term, candidateID, lastLogIndex, lastLogTerm uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return 0, ErrClosed
	}

	// Deny vote if:
	//   1. Candidate is requesting a vote from an earlier term. (§5.1)
	//   2. Already voted for a different candidate in this term. (§5.2)
	//   3. Candidate log is less up-to-date than local log. (§5.4)
	if term < l.currentTerm {
		return l.currentTerm, ErrStaleTerm
	} else if term == l.currentTerm && l.votedFor != 0 && l.votedFor != candidateID {
		return l.currentTerm, ErrAlreadyVoted
	} else if lastLogTerm < l.currentTerm {
		return l.currentTerm, ErrOutOfDateLog
	} else if lastLogTerm == l.currentTerm && lastLogIndex < l.currentIndex {
		return l.currentTerm, ErrOutOfDateLog
	}

	// Vote for candidate.
	l.votedFor = candidateID

	// TODO(benbjohnson): Update term.

	return l.currentTerm, nil
}

// WriteTo attaches a writer to the log from a given index.
// The index specified must be a committed index.
func (l *Log) WriteTo(w io.Writer, term, index uint64) error {
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Check if log is closed.
		if !l.opened() {
			return ErrClosed
		}

		// Step down if from a higher term.
		if term > l.currentTerm {
			l.demote()
		}

		// Do not begin streaming if:
		//   1. Node is not the leader.
		//   2. Term is earlier than current term.
		//   3. Index is after the commit index.
		if l.state != Leader {
			return ErrNotLeader
		} else if index > l.commitIndex {
			return ErrUncommittedIndex
		}

		// Add writer.
		l.writers = append(l.writers, w)

		return nil
	}()
	if err != nil {
		return err
	}

	// TODO(benbjohnson): Write snapshot, if index is unavailable.

	// Write segment to the writer.
	if err := l.segment.writeTo(w, index); err != nil {
		return err
	}

	return nil
}

// ReadFrom continually reads log entries from a reader.
func (l *Log) ReadFrom(r io.ReadCloser) error {
	l.mu.Lock()

	// Check if log is closed.
	if !l.opened() {
		l.mu.Unlock()
		return ErrClosed
	}

	// Remove previous reader, if one exists.
	if l.reader != nil {
		_ = l.reader.Close()
	}

	// Set new reader.
	l.reader = r
	l.mu.Unlock()

	// If a nil reader is passed in then exit.
	if r == nil {
		return nil
	}

	// TODO(benbjohnson): Check first byte for snapshot marker.

	// Continually decode entries.
	dec := NewLogEntryDecoder(r)
	for {
		// Decode single entry.
		var e LogEntry
		if err := dec.Decode(&e); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Append entry to the log.
		if err := l.segment.append(&e); err != nil {
			return err
		}
	}
}

// append requests a vote from the log.
func (l *Log) append(e *LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if log is closed.
	if !l.opened() {
		return ErrClosed
	}

	// TODO(benbjohnson): Write to the end of the log.

	return nil
}

// Elect increments the log's term and forces an election.
// This function does not guarentee that this node will become the leader.
func (l *Log) Elect() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.elect()
}

func (l *Log) elect() error {
	l.state = Candidate
	// TODO(benbjohnson): Hold election.
	return nil
}

// LogEntryType serves as an internal marker for log entries.
// Non-command entry types are handled by the library itself.
type LogEntryType uint8

const (
	LogEntryCommand LogEntryType = iota
	LogEntryNop
	LogEntryConfig
)

// LogEntry represents a single command within the log.
type LogEntry struct {
	Type  LogEntryType
	Index uint64
	Term  uint64
	Data  []byte
}

// EncodedHeader returns the encoded header for the entry.
func (e *LogEntry) EncodedHeader() []byte {
	var b [logEntryHeaderSize]byte
	binary.BigEndian.PutUint64(b[0:8], (uint64(e.Type)<<60)|uint64(len(e.Data)))
	binary.BigEndian.PutUint64(b[8:16], e.Index)
	binary.BigEndian.PutUint64(b[16:24], e.Term)
	return b[:]
}

// LogEntryEncoder encodes entries to a writer.
type LogEntryEncoder struct {
	w io.Writer
}

// NewLogEntryEncoder returns a new instance of the LogEntryEncoder that
// will encode to a writer.
func NewLogEntryEncoder(w io.Writer) *LogEntryEncoder {
	return &LogEntryEncoder{w: w}
}

// Encode writes a log entry to the encoder's writer.
func (enc *LogEntryEncoder) Encode(e *LogEntry) error {
	// Write header.
	if _, err := enc.w.Write(e.EncodedHeader()); err != nil {
		return err
	}

	// Write data.
	if _, err := enc.w.Write(e.Data); err != nil {
		return err
	}
	return nil
}

// LogEntryDecoder decodes entries from a reader.
type LogEntryDecoder struct {
	r io.Reader
}

// NewLogEntryDecoder returns a new instance of the LogEntryDecoder that
// will decode from a reader.
func NewLogEntryDecoder(r io.Reader) *LogEntryDecoder {
	return &LogEntryDecoder{r: r}
}

// Decode reads a log entry from the decoder's reader.
func (dec *LogEntryDecoder) Decode(e *LogEntry) error {
	// Read header.
	var b [logEntryHeaderSize]byte
	if _, err := io.ReadFull(dec.r, b[:]); err != nil {
		return err
	}
	sz := binary.BigEndian.Uint64(b[0:8])
	e.Type, sz = LogEntryType(sz>>60), sz&0x0FFFFFFF
	e.Index = binary.BigEndian.Uint64(b[8:16])
	e.Term = binary.BigEndian.Uint64(b[16:24])

	// Read data.
	data := make([]byte, sz)
	if _, err := io.ReadFull(dec.r, data); err != nil {
		return err
	}
	e.Data = data

	return nil
}

// segment represents a contiguous subset of the log.
// The segment can be represented on-disk and/or in-memory.
type segment struct {
	mu sync.RWMutex

	path    string  // path of segment on-disk
	sealed  bool    // true if entries committed and cannot change.
	index   uint64  // starting index
	offsets []int64 // byte offset of each index

	f   *os.File // on-disk representation
	buf []byte   // in-memory cache, nil means uncached

	writers []*segmentWriter // segment tailing
}

// Close closes the segment.
func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeWriters()
	return nil
}

func (s *segment) closeWriters() {
	for _, w := range s.writers {
		w.Close()
	}
}

// seal sets the segment as sealed.
func (s *segment) seal() {
	s.mu.Lock()
	defer s.mu.Lock()

	// Seal off segment.
	s.sealed = true

	// Close all tailing writers.
	for _, w := range s.writers {
		w.Close()
	}
}

// append writes a set of entries to the segment.
func (s *segment) append(e *LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Encode header and record offset.
	header := e.EncodedHeader()
	offset := int64(len(s.buf))

	// TODO(benbjohnson): Write to the file, if available.

	// Write to the cache, if available.
	s.buf = append(s.buf, header...)
	s.buf = append(s.buf, e.Data...)

	// Save offset.
	s.offsets = append(s.offsets, offset)

	return nil
}

// truncate removes all entries after a given index.
func (s *segment) truncate(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// TODO(benbjohnson): Truncate the file, if available.
	// TODO(benbjohnson): Truncate the cache, if available.

	return nil
}

// writerTo writes to a writer from a given log index.
func (s *segment) writeTo(w io.Writer, index uint64) error {
	var writer *segmentWriter
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// TODO(benbjohnson): Create buffered output to prevent blocking.

		// Catch up writer to the end of the segment.
		offset := s.offsets[index-s.index]
		if _, err := w.Write(s.buf[offset:]); err != nil {
			return err
		}

		// Flush, if applicable.
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}

		// Wrap writer and append to segment to tail.
		// If segment is already closed then simply close the channel immediately.
		writer = &segmentWriter{w, make(chan error)}
		if s.sealed {
			writer.Close()
		} else {
			s.writers = append(s.writers, writer)
		}

		return nil
	}()
	if err != nil {
		return err
	}

	// Wait for segment to finish writing.
	return <-writer.ch
}

// segmentWriter wraps writers to provide a channel for close notification.
type segmentWriter struct {
	w  io.Writer
	ch chan error
}

func (w *segmentWriter) Close() {
	close(w.ch)
}

// Config represents the configuration for the log.
type Config struct {
	// Cluster identifier. Used to prevent separate clusters from
	// accidentally communicating with one another.
	ClusterID uint64 `json:"clusterID,omitempty"`

	// List of nodes in the cluster.
	Nodes []*Node `json:"nodes,omitempty"`
}

// Node represents a single machine in the raft cluster.
type Node struct {
	ID  uint64   `json:"id"`
	URL *url.URL `json:"url,omitempty"`
}

// nodeJSONMarshaler represents the JSON serialized form of the Node type.
type nodeJSONMarshaler struct {
	ID  uint64 `json:"id"`
	URL string `json:"url,omitempty"`
}

// MarshalJSON encodes the node into a JSON-formatted byte slice.
func (n *Node) MarshalJSON() ([]byte, error) {
	var o nodeJSONMarshaler
	o.ID = n.ID
	if n.URL != nil {
		o.URL = n.URL.String()
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-formatted byte slice into a node.
func (n *Node) UnmarshalJSON(data []byte) error {
	// Unmarshal into temporary type.
	var o nodeJSONMarshaler
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Convert values to a node.
	n.ID = o.ID
	if o.URL != "" {
		u, err := url.Parse(o.URL)
		if err != nil {
			return err
		}
		n.URL = u
	}

	return nil
}

func assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("asser failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

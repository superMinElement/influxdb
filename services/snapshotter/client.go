package snapshotter

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"archive/tar"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tcp"
	"path/filepath"
	"strconv"
	"strings"
)

// Client provides an API for the snapshotter service.
type Client struct {
	host string
}

// NewClient returns a new *Client.
func NewClient(host string) *Client {
	return &Client{host: host}
}

// takes a request object, writes a Base64 encoding to the tcp connection, and then sends the request to the snapshotter service.
// returns a mapping of the uploaded metadata shardID's to actual shardID's on the destination system.
func (c *Client) UpdateMeta(req *Request, upStream io.Reader) (map[uint64]uint64, error) {
	var err error

	var b bytes.Buffer

	// Connect to snapshotter service.
	conn, err := tcp.Dial("tcp", c.host, MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{byte(req.Type)}); err != nil {
		return nil, err
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	if n, err := io.Copy(conn, upStream); (err != nil && err != io.EOF) || n != req.UploadSize {
		return nil, fmt.Errorf("error uploading file: err=%v, n=%d, uploadSize: %d", err, n, req.UploadSize)
	}

	if n, err := b.ReadFrom(conn); err != nil || n == 0 {
		return nil, fmt.Errorf("updating metadata on influxd service failed: err=%v, n=%d", err, n)
	}

	resp := b.Bytes()

	if err != nil {
		return nil, err
	}

	header, npairs, err := c.decodeUintPair(resp[:16])
	if err != nil {
		return nil, err
	}

	if npairs == 0 {
		return nil, fmt.Errorf("DB metadata not changed. database may already exist")
	}

	pairs := resp[16:]

	if header != BackupMagicHeader {
		return nil, fmt.Errorf("Response did not contain the proper header tag.")
	}

	if (len(pairs)/8)%2 != 0 {
		return nil, fmt.Errorf("expected an even number of integer pairs in update meta repsonse")
	}

	shardIDMap := make(map[uint64]uint64)
	for i := 0; i < int(npairs); i++ {
		offset := i * 16
		k, v, err := c.decodeUintPair(pairs[offset : offset+16])
		if err != nil {
			return nil, err
		}
		shardIDMap[k] = v
	}

	return shardIDMap, nil
}

func (c *Client) decodeUintPair(bits []byte) (uint64, uint64, error) {
	if len(bits) != 16 {
		return 0, 0, fmt.Errorf("array must have exactly 16 bytes")
	}
	v1 := binary.BigEndian.Uint64(bits[:8])
	v2 := binary.BigEndian.Uint64(bits[8:16])
	return v1, v2, nil
}

func (c *Client) UploadShard(shardID, newShardID uint64, destinationDatabase, restoreRetention string, tr *tar.Reader) error {

	conn, err := tcp.Dial("tcp", c.host, MuxHeader)
	defer conn.Close()
	if err != nil {
		return err
	}

	if _, err := conn.Write([]byte{byte(RequestShardUpdate)}); err != nil {
		return err
	}

	var shardBytes [8]byte
	binary.BigEndian.PutUint64(shardBytes[:], newShardID)
	if _, err := conn.Write(shardBytes[:]); err != nil {
		return err
	}

	tw := tar.NewWriter(conn)
	defer tw.Close()

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		names := strings.Split(hdr.Name, string(filepath.Separator))

		if destinationDatabase == "" {
			destinationDatabase = names[0]
		}

		if restoreRetention == "" {
			restoreRetention = names[1]
		}

		filepathArgs := []string{destinationDatabase, restoreRetention, strconv.FormatUint(newShardID, 10)}
		filepathArgs = append(filepathArgs, names[3:]...)
		hdr.Name = filepath.ToSlash(filepath.Join(filepathArgs...))

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if _, err := io.Copy(tw, tr); err != nil {
			return err
		}
	}

	return nil
}

// MetastoreBackup returns a snapshot of the meta store.
func (c *Client) MetastoreBackup() (*meta.Data, error) {
	req := &Request{
		Type: RequestMetastoreBackup,
	}

	b, err := c.doRequest(req)
	if err != nil {
		return nil, err
	}

	// Check the magic.
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != BackupMagicHeader {
		return nil, errors.New("invalid metadata received")
	}
	i := 8

	// Size of the meta store bytes.
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]

	// Unpack meta data.
	var data meta.Data
	if err := data.UnmarshalBinary(metaBytes); err != nil {
		return nil, fmt.Errorf("unmarshal: %s", err)
	}

	return &data, nil
}

// doRequest sends a request to the snapshotter service and returns the result.
func (c *Client) doRequest(req *Request) ([]byte, error) {
	// Connect to snapshotter service.
	conn, err := tcp.Dial("tcp", c.host, MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write the request
	_, err = conn.Write([]byte{byte(req.Type)})
	if err != nil {
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	// Read snapshot from the connection
	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)

	return buf.Bytes(), err
}

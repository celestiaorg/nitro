package celestia

import (
	"bytes"
	"encoding/binary"
)

// BlobPointer contains the reference to the data blob on Celestia
type BlobPointer struct {
	BlockHeight  uint64
	Start        uint64
	SharesLength uint64
	Key          uint64
	NumLeaves    uint64
	TxCommitment []byte
	DataRoot     []byte
	SideNodes    [][]byte
}

// MarshalBinary encodes the BlobPointer to binary
// serialization format: height + start + end + commitment + data root
func (b *BlobPointer) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Writing fixed-size values
	if err := binary.Write(buf, binary.BigEndian, b.BlockHeight); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.Start); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.SharesLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.Key); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, b.NumLeaves); err != nil {
		return nil, err
	}

	// Writing variable-size values
	if err := writeBytes(buf, b.TxCommitment); err != nil {
		return nil, err
	}
	if err := writeBytes(buf, b.DataRoot); err != nil {
		return nil, err
	}

	// Writing slice of slices
	if err := binary.Write(buf, binary.BigEndian, uint64(len(b.SideNodes))); err != nil {
		return nil, err
	}
	for _, sideNode := range b.SideNodes {
		if err := writeBytes(buf, sideNode); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// writeBytes writes a length-prefixed byte slice to the buffer
func writeBytes(buf *bytes.Buffer, data []byte) error {
	if err := binary.Write(buf, binary.BigEndian, uint64(len(data))); err != nil {
		return err
	}
	if _, err := buf.Write(data); err != nil {
		return err
	}
	return nil
}

// UnmarshalBinary decodes the binary to BlobPointer
// serialization format: height + start + end + commitment + data root
func (b *BlobPointer) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)

	// Reading fixed-size values
	if err := binary.Read(buf, binary.BigEndian, &b.BlockHeight); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &b.Start); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &b.SharesLength); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &b.Key); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &b.NumLeaves); err != nil {
		return err
	}

	// Reading variable-size values
	var err error
	if b.TxCommitment, err = readBytes(buf); err != nil {
		return err
	}
	if b.DataRoot, err = readBytes(buf); err != nil {
		return err
	}

	// Reading slice of slices
	var sideNodesLen uint64
	if err := binary.Read(buf, binary.BigEndian, &sideNodesLen); err != nil {
		return err
	}
	b.SideNodes = make([][]byte, sideNodesLen)
	for i := uint64(0); i < sideNodesLen; i++ {
		if b.SideNodes[i], err = readBytes(buf); err != nil {
			return err
		}
	}

	return nil
}

// readBytes reads a length-prefixed byte slice from the buffer
func readBytes(buf *bytes.Reader) ([]byte, error) {
	var length uint64
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := buf.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}

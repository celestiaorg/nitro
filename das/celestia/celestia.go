package celestia

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/spf13/pflag"

	openrpc "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
	"github.com/ethereum/go-ethereum/log"
	"github.com/tendermint/tendermint/rpc/client/http"
)

type DAConfig struct {
	Enable        bool    `koanf:"enable"`
	IsPoster      bool    `koanf:"is-poster"`
	GasPrice      float64 `koanf:"gas-price"`
	Rpc           string  `koanf:"rpc"`
	TendermintRPC string  `koanf:"tendermint-rpc"`
	NamespaceId   string  `koanf:"namespace-id"`
	AuthToken     string  `koanf:"auth-token"`
}

// CelestiaMessageHeaderFlag indicates that this data is a Blob Pointer
// which will be used to retrieve data from Celestia
const CelestiaMessageHeaderFlag byte = 0x0c

func IsCelestiaMessageHeaderByte(header byte) bool {
	return (CelestiaMessageHeaderFlag & header) > 0
}

type CelestiaDA struct {
	Cfg       DAConfig
	Client    *openrpc.Client
	Trpc      *http.HTTP
	Namespace share.Namespace
}

// These are currently only used to generate the types for the arbitrum-orbit-sdk
func CelestiaDAConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", false, "Enable Celestia DA")
	f.Bool(prefix+".is-poster", false, "Node is batch poster node")
	f.Float64(prefix+".gas-price", 0.1, "Gas for Celestia transactions")
	f.String(prefix+".rpc", "", "Rpc endpoint for celestia-node")
	f.String(prefix+".tendermint-rpc", "", "Tendermint RPC endpoint, only used when the node is a batch poster")
	f.String(prefix+".namespace-id", "", "Celestia Namespace to post data to")
	f.String(prefix+".auth-token", "", "Auth token for Celestia Node")
}

func NewCelestiaDA(cfg DAConfig) (*CelestiaDA, error) {
	daClient, err := openrpc.NewClient(context.Background(), cfg.Rpc, cfg.AuthToken)
	if err != nil {
		return nil, err
	}

	if cfg.NamespaceId == "" {
		return nil, errors.New("namespace id cannot be blank")
	}
	nsBytes, err := hex.DecodeString(cfg.NamespaceId)
	if err != nil {
		return nil, err
	}

	namespace, err := share.NewBlobNamespaceV0(nsBytes)
	if err != nil {
		return nil, err
	}

	var trpc *http.HTTP
	if cfg.IsPoster {
		trpc, err = http.New(cfg.TendermintRPC, "/websocket")
		if err != nil {
			log.Error("Unable to establish connection with celestia-core tendermint rpc")
			return nil, err
		}
		err = trpc.Start()
		if err != nil {
			return nil, err
		}
	}

	return &CelestiaDA{
		Cfg:       cfg,
		Client:    daClient,
		Trpc:      trpc,
		Namespace: namespace,
	}, nil
}

func (c *CelestiaDA) Store(ctx context.Context, message []byte) ([]byte, error) {

	dataBlob, err := blob.NewBlobV0(c.Namespace, message)
	if err != nil {
		log.Warn("Error creating blob", "err", err)
		return nil, err
	}

	commitment, err := blob.CreateCommitment(dataBlob)
	if err != nil {
		log.Warn("Error creating commitment", "err", err)
		return nil, err
	}

	height, err := c.Client.Blob.Submit(ctx, []*blob.Blob{dataBlob}, openrpc.GasPrice(c.Cfg.GasPrice))
	if err != nil {
		log.Warn("Blob Submission error", "err", err)
		return nil, err
	}
	if height == 0 {
		log.Warn("Unexpected height from blob response", "height", height)
		return nil, errors.New("unexpected response code")
	}

	proofs, err := c.Client.Blob.GetProof(ctx, height, c.Namespace, commitment)
	if err != nil {
		log.Warn("Error retrieving proof", "err", err)
		return nil, err
	}

	included, err := c.Client.Blob.Included(ctx, height, c.Namespace, proofs, commitment)
	if err != nil || !included {
		log.Warn("Error checking for inclusion", "err", err, "proof", proofs)
		return nil, err
	}
	log.Info("Succesfully posted blob", "height", height, "commitment", hex.EncodeToString(commitment))

	// we fetch the blob so that we can get the correct start index in the square
	blob, err := c.Client.Blob.Get(ctx, height, c.Namespace, commitment)
	if err != nil {
		return nil, err
	}
	if blob.Index <= 0 {
		log.Warn("Unexpected index from blob response", "index", blob.Index)
		return nil, errors.New("unexpected response code")
	}

	header, err := c.Client.Header.GetByHeight(ctx, height)
	if err != nil {
		log.Warn("Header retrieval error", "err", err)
		return nil, err
	}

	sharesLength := uint64(0)
	for _, proof := range *proofs {
		sharesLength += uint64(proof.End()) - uint64(proof.Start())
	}

	txCommitment, dataRoot := [32]byte{}, [32]byte{}
	copy(txCommitment[:], commitment)

	copy(dataRoot[:], header.DataHash)

	// Row roots give us the length of the EDS
	squareSize := uint64(len(header.DAH.RowRoots))
	// ODS size
	odsSize := squareSize / 2

	blobIndex := uint64(blob.Index)
	// startRow
	startRow := blobIndex / squareSize
	if odsSize*startRow > blobIndex {
		// return an empty batch
		return nil, fmt.Errorf("storing Celestia information, odsSize*startRow=%v was larger than blobIndex=%v", odsSize*startRow, blob.Index)
	}
	startIndexOds := blobIndex - odsSize*startRow
	blobPointer := BlobPointer{
		BlockHeight:  height,
		Start:        startIndexOds,
		SharesLength: sharesLength,
		TxCommitment: txCommitment,
		DataRoot:     dataRoot,
	}
	log.Info("Posted blob to height and dataRoot", "height", blobPointer.BlockHeight, "dataRoot", hex.EncodeToString(blobPointer.DataRoot[:]))

	blobPointerData, err := blobPointer.MarshalBinary()
	if err != nil {
		log.Warn("BlobPointer MashalBinary error", "err", err)
		return nil, err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, CelestiaMessageHeaderFlag)
	if err != nil {
		log.Warn("batch type byte serialization failed", "err", err)
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, blobPointerData)
	if err != nil {
		log.Warn("blob pointer data serialization failed", "err", err)
		return nil, err
	}

	serializedBlobPointerData := buf.Bytes()
	log.Trace("celestia.CelestiaDA.Store", "serialized_blob_pointer", serializedBlobPointerData)
	return serializedBlobPointerData, nil
}

type SquareData struct {
	RowRoots    [][]byte
	ColumnRoots [][]byte
	Rows        [][][]byte
	// Refers to the square size of the extended data square
	SquareSize uint64
	StartRow   uint64
	EndRow     uint64
}

func (c *CelestiaDA) Read(ctx context.Context, blobPointer *BlobPointer) ([]byte, *SquareData, error) {
	blob, err := c.Client.Blob.Get(ctx, blobPointer.BlockHeight, c.Namespace, blobPointer.TxCommitment[:])
	if err != nil {
		return nil, nil, err
	}
	log.Info("Read blob for height", "height", blobPointer.BlockHeight, "blob", blob.Data)

	header, err := c.Client.Header.GetByHeight(ctx, blobPointer.BlockHeight)
	if err != nil {
		return nil, nil, err
	}

	eds, err := c.Client.Share.GetEDS(ctx, header)
	if err != nil {
		return nil, nil, err
	}

	squareSize := uint64(eds.Width())
	odsSize := squareSize / 2

	startRow := blobPointer.Start / odsSize

	if blobPointer.Start >= odsSize*odsSize {
		log.Error("startIndexOds >= odsSize*odsSize", "startIndexOds", blobPointer.Start, "odsSize*odsSize", odsSize*odsSize)
		return []byte{}, nil, nil
	}

	if blobPointer.Start+blobPointer.SharesLength < 1 {
		log.Error("startIndexOds+blobPointer.SharesLength < 1", "startIndexOds+blobPointer.SharesLength", blobPointer.Start+blobPointer.SharesLength)
		return []byte{}, nil, nil
	}

	endIndexOds := blobPointer.Start + blobPointer.SharesLength - 1
	if endIndexOds >= odsSize*odsSize {
		log.Error("endIndexOds >= odsSize*odsSize", "endIndexOds", endIndexOds, "odsSize*odsSize", odsSize*odsSize)
		return []byte{}, nil, nil
	}

	endRow := endIndexOds / odsSize

	if endRow > odsSize || startRow > odsSize {
		log.Error("endRow > odsSize || startRow > odsSize", "endRow", endRow, "startRow", startRow, "odsSize", odsSize)
		return []byte{}, nil, nil
	}

	startColumn := blobPointer.Start % odsSize
	endColumn := endIndexOds % odsSize

	if startRow == endRow && startColumn > endColumn+1 {
		log.Error("startColumn > endColumn+1 on the same row", "startColumn", startColumn, "endColumn+1 ", endColumn+1)
		return []byte{}, nil, nil
	}

	rows := [][][]byte{}
	for i := startRow; i <= endRow; i++ {
		rows = append(rows, eds.Row(uint(i)))
	}

	squareData := SquareData{
		RowRoots:    header.DAH.RowRoots,
		ColumnRoots: header.DAH.ColumnRoots,
		Rows:        rows,
		SquareSize:  squareSize,
		StartRow:    startRow,
		EndRow:      endRow,
	}

	return blob.Data, &squareData, nil
}

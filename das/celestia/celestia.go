package celestia

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/big"
	"time"

	"github.com/offchainlabs/nitro/arbutil"
	wrapper "github.com/offchainlabs/nitro/das/celestia/wrapper"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	openrpc "github.com/rollkit/celestia-openrpc"
	"github.com/rollkit/celestia-openrpc/types/blob"
	"github.com/rollkit/celestia-openrpc/types/share"
	"github.com/tendermint/tendermint/rpc/client/http"
)

type DAConfig struct {
	Enable            bool   `koanf:"enable"`
	Rpc               string `koanf:"rpc"`
	TendermintRPC     string `koanf:"tendermint-rpc"`
	NamespaceId       string `koanf:"namespace-id"`
	AuthToken         string `koanf:"auth-token"`
	AppGrpc           string `koanf:"app-grpc"`
	BlobstreamAddress string `koanf:"blobstream-address"`
}

// CelestiaMessageHeaderFlag indicates that this data is a Blob Pointer
// which will be used to retrieve data from Celestia
const CelestiaMessageHeaderFlag byte = 0x0c

func IsCelestiaMessageHeaderByte(header byte) bool {
	return (CelestiaMessageHeaderFlag & header) > 0
}

// Add Tendermint RPC for Full node Endpoint
type CelestiaDA struct {
	Cfg               DAConfig
	Client            *openrpc.Client
	Trpc              *http.HTTP
	Namespace         share.Namespace
	BlobstreamWrapper *wrapper.Wrappers
}

func NewCelestiaDA(cfg DAConfig, l1Interface arbutil.L1Interface) (*CelestiaDA, error) {
	log.Info("Auth token in New Celestia DA", "token", cfg.AuthToken)
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

	trpc, err := http.New(cfg.TendermintRPC, "/websocket")
	if err != nil {
		log.Error("Unable to establish connection with celestia-core tendermint rpc")
		return nil, err
	}
	err = trpc.Start()
	if err != nil {
		return nil, err
	}

	bStreamWrapper, err := wrapper.NewWrappers(common.HexToAddress(cfg.BlobstreamAddress), l1Interface)
	if err != nil {
		return nil, err
	}

	return &CelestiaDA{
		Cfg:               cfg,
		Client:            daClient,
		Trpc:              trpc,
		Namespace:         namespace,
		BlobstreamWrapper: bStreamWrapper,
	}, nil
}

func (c *CelestiaDA) Store(ctx context.Context, message []byte) (*BlobPointer, bool, error) {

	dataBlob, err := blob.NewBlobV0(c.Namespace, message)
	if err != nil {
		log.Warn("Error creating blob", "err", err)
		return nil, false, err
	}

	commitment, err := blob.CreateCommitment(dataBlob)
	if err != nil {
		log.Warn("Error creating commitment", "err", err)
		return nil, false, err
	}
	height, err := c.Client.Blob.Submit(ctx, []*blob.Blob{dataBlob}, openrpc.DefaultSubmitOptions())
	if err != nil {
		log.Warn("Blob Submission error", "err", err)
		return nil, false, err
	}
	if height == 0 {
		log.Warn("Unexpected height from blob response", "height", height)
		return nil, false, errors.New("unexpected response code")
	}

	// how long do we have to wait to retrieve a proof?
	//log.Info("Retrieving Proof from Celestia", "height", height, "commitment", commitment)
	proofs, err := c.Client.Blob.GetProof(ctx, height, c.Namespace, commitment)
	if err != nil {
		log.Warn("Error retrieving proof", "err", err)
		return nil, false, err
	}

	included, err := c.Client.Blob.Included(ctx, height, c.Namespace, proofs, commitment)
	if err != nil {
		log.Warn("Error checking for inclusion", "err", err, "proof", proofs)
		return nil, included, err
	}

	header, err := c.Client.Header.GetByHeight(ctx, height)
	if err != nil {
		log.Warn("Header retrieval error", "err", err)
		return nil, included, err
	}

	var startIndex uint64
	sharesLength := uint64(0)
	for i, proof := range *proofs {
		if i == 0 {
			startIndex = uint64(proof.Start())
		}
		sharesLength += uint64(proof.End()) - uint64(proof.Start())
	}

	txCommitment, dataRoot := [32]byte{}, [32]byte{}
	copy(txCommitment[:], commitment)

	log.Info("Trying header.DataHash (Store)", "header.DataHash.Bytes()", header.DataHash)
	copy(dataRoot[:], header.DataHash)
	log.Info("Data root", "dataRoot", dataRoot)
	log.Info("Commitment (Store)", "commitment", txCommitment)

	blobPointer := BlobPointer{
		BlockHeight:  height,
		Start:        startIndex,
		SharesLength: sharesLength,
		TxCommitment: txCommitment,
		DataRoot:     dataRoot,
	}

	return &blobPointer, included, nil

}

func (c *CelestiaDA) Serialize(blobPointer *BlobPointer) ([]byte, error) {
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

	header, err := c.Client.Header.GetByHeight(ctx, blobPointer.BlockHeight)
	if err != nil {
		return nil, nil, err
	}

	eds, err := c.Client.Share.GetEDS(ctx, header)
	if err != nil {
		return nil, nil, err
	}

	squareSize := uint64(eds.Width())
	odsSquareSize := squareSize / 2
	startRow := blobPointer.Start / odsSquareSize
	endRow := (blobPointer.Start + blobPointer.SharesLength) / odsSquareSize

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

func (c *CelestiaDA) Verify(ctx context.Context, blobPointer *BlobPointer, beginBlock uint64, endBlock uint64) (bool, error) {

	// Get tRPC interface and query /data_root_inclusion_proof
	inclusionProof, err := c.Trpc.DataRootInclusionProof(ctx, blobPointer.BlockHeight, beginBlock, endBlock)
	if err != nil {
		log.Warn("DataRootInclusionProof error", "err", err)
		return false, err
	}

	sideNodes := make([][32]byte, len(inclusionProof.Proof.Aunts))
	for i, aunt := range inclusionProof.Proof.Aunts {
		sideNodes[i] = *(*[32]byte)(aunt)
	}

	blobPointer.Key = uint64(inclusionProof.Proof.Index)
	blobPointer.NumLeaves = uint64(inclusionProof.Proof.Total)
	blobPointer.SideNodes = sideNodes

	tuple := wrapper.DataRootTuple{
		Height:   big.NewInt(int64(blobPointer.BlockHeight)),
		DataRoot: blobPointer.DataRoot,
	}

	proof := wrapper.BinaryMerkleProof{
		SideNodes: blobPointer.SideNodes,
		Key:       big.NewInt(int64(blobPointer.Key)),
		NumLeaves: big.NewInt(int64(blobPointer.NumLeaves)),
	}

	for {
		time.Sleep(time.Second * 5)
		stateEventNonce, err := c.BlobstreamWrapper.StateEventNonce(&bind.CallOpts{})
		if err != nil {
			log.Info("Error querying state event nonce", "err", err)
			return false, err
		}

		nonce := stateEventNonce.Uint64()
		if nonce > blobPointer.TupleRootNonce {
			break
		}
	}

	valid, err := c.BlobstreamWrapper.VerifyAttestation(
		&bind.CallOpts{},
		big.NewInt(int64(blobPointer.TupleRootNonce)),
		tuple,
		proof,
	)
	if err != nil {
		log.Warn("Error verifying attestation", "err", err)
		return false, nil
	}

	return valid, nil
}

func (c *CelestiaDA) WaitForHeight(ctx context.Context, height uint64) error {
	for {
		// Sleep for 5 seconds
		time.Sleep(time.Second * 5)
		networkHead, err := c.Client.Header.LocalHead(ctx)
		if err != nil {
			return err
		}

		if networkHead.Height() >= height {
			break
		}
	}
	return nil
}

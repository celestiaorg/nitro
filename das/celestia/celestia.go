package celestia

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/big"

	"github.com/offchainlabs/nitro/arbutil"
	blobstreamx "github.com/succinctlabs/blobstreamx/bindings"

	openrpc "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/tendermint/tendermint/rpc/client/http"
)

type DAConfig struct {
	Enable             bool   `koanf:"enable"`
	IsPoster           bool   `koanf:"isPoster"`
	Rpc                string `koanf:"rpc"`
	TendermintRPC      string `koanf:"tendermint-rpc"`
	NamespaceId        string `koanf:"namespace-id"`
	AuthToken          string `koanf:"auth-token"`
	AppGrpc            string `koanf:"app-grpc"`
	BlobstreamXAddress string `koanf:"blobstreamx-address"`
	BlockDrift         uint64 `koanf:"block-drift"`
}

// CelestiaMessageHeaderFlag indicates that this data is a Blob Pointer
// which will be used to retrieve data from Celestia
const CelestiaMessageHeaderFlag byte = 0x0c

func IsCelestiaMessageHeaderByte(header byte) bool {
	return (CelestiaMessageHeaderFlag & header) > 0
}

type CelestiaDA struct {
	Cfg         DAConfig
	Client      *openrpc.Client
	Trpc        *http.HTTP
	Namespace   share.Namespace
	BlobstreamX *blobstreamx.BlobstreamX
}

func NewCelestiaDA(cfg DAConfig, l1Interface arbutil.L1Interface) (*CelestiaDA, error) {
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

	blobstreamx, err := blobstreamx.NewBlobstreamX(common.HexToAddress(cfg.BlobstreamXAddress), l1Interface)
	if err != nil {
		return nil, err
	}

	if cfg.BlockDrift == 0 {
		cfg.BlockDrift = 400
	}

	return &CelestiaDA{
		Cfg:         cfg,
		Client:      daClient,
		Trpc:        trpc,
		Namespace:   namespace,
		BlobstreamX: blobstreamx,
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

	height, err := c.Client.Blob.Submit(ctx, []*blob.Blob{dataBlob}, 0.3)
	if err != nil {
		log.Warn("Blob Submission error", "err", err)
		return nil, false, err
	}
	if height == 0 {
		log.Warn("Unexpected height from blob response", "height", height)
		return nil, false, errors.New("unexpected response code")
	}

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

	// we fetch the blob so that we can get the correct start index in the square
	blob, err := c.Client.Blob.Get(ctx, height, c.Namespace, commitment)
	if err != nil {
		return nil, false, err
	}
	if blob.Index <= 0 {
		log.Warn("Unexpected index from blob response", "index", blob.Index)
		return nil, false, errors.New("unexpected response code")
	}

	header, err := c.Client.Header.GetByHeight(ctx, height)
	if err != nil {
		log.Warn("Header retrieval error", "err", err)
		return nil, included, err
	}

	sharesLength := uint64(0)
	for _, proof := range *proofs {
		sharesLength += uint64(proof.End()) - uint64(proof.Start())
	}

	txCommitment, dataRoot := [32]byte{}, [32]byte{}
	copy(txCommitment[:], commitment)

	copy(dataRoot[:], header.DataHash)

	blobPointer := BlobPointer{
		BlockHeight:  height,
		Start:        uint64(blob.Index),
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
	odsSquareSize := squareSize / 2

	startRow := blobPointer.Start / squareSize

	endRow := (blobPointer.Start + blobPointer.SharesLength + odsSquareSize) / squareSize

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

func (c *CelestiaDA) Verify(ctx context.Context, blobPointer *BlobPointer) (bool, error) {

	eventsChan := make(chan *blobstreamx.BlobstreamXDataCommitmentStored, 100)
	subscription, err := c.BlobstreamX.WatchDataCommitmentStored(
		&bind.WatchOpts{
			Context: ctx,
		},
		eventsChan,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return false, err
	}
	defer subscription.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case err := <-subscription.Err():
			return false, err
		case event := <-eventsChan:

			inclusionProof, err := c.Trpc.DataRootInclusionProof(ctx, blobPointer.BlockHeight, event.StartBlock, event.StartBlock)
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
			blobPointer.ProofNonce = event.ProofNonce.Uint64()

			tuple := blobstreamx.DataRootTuple{
				Height:   big.NewInt(int64(blobPointer.BlockHeight)),
				DataRoot: blobPointer.DataRoot,
			}

			proof := blobstreamx.BinaryMerkleProof{
				SideNodes: blobPointer.SideNodes,
				Key:       big.NewInt(int64(blobPointer.Key)),
				NumLeaves: big.NewInt(int64(blobPointer.NumLeaves)),
			}

			valid, err := c.BlobstreamX.VerifyAttestation(
				&bind.CallOpts{},
				big.NewInt(event.ProofNonce.Int64()),
				tuple,
				proof,
			)

			if err != nil {
				log.Warn("Error verifying attestation", "err", err)
				return false, nil
			}

			return valid, nil
		}
	}

}

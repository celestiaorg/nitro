package celestia

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/spf13/pflag"

	openrpc "github.com/celestiaorg/celestia-openrpc"
	"github.com/celestiaorg/celestia-openrpc/types/blob"
	"github.com/celestiaorg/celestia-openrpc/types/share"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/solgen/go/celestiagen"

	blobstreamx "github.com/succinctlabs/blobstreamx/bindings"
	"github.com/tendermint/tendermint/rpc/client/http"
)

type DAConfig struct {
	Enable          bool             `koanf:"enable"`
	GasPrice        float64          `koanf:"gas-price"`
	Rpc             string           `koanf:"rpc"`
	NamespaceId     string           `koanf:"namespace-id"`
	AuthToken       string           `koanf:"auth-token"`
	ValidatorConfig *ValidatorConfig `koanf:"validator-config"`
}

type ValidatorConfig struct {
	TendermintRPC  string `koanf:"tendermint-rpc"`
	EthClient      string `koanf:"eth-ws"`
	BlobstreamAddr string `koanf:"blobstream"`
	MaxIterations  uint64 `koan:"max-iterations"`
}

// CelestiaMessageHeaderFlag indicates that this data is a Blob Pointer
// which will be used to retrieve data from Celestia
const CelestiaMessageHeaderFlag byte = 0x63

func hasBits(checking byte, bits byte) bool {
	return (checking & bits) == bits
}

func IsCelestiaMessageHeaderByte(header byte) bool {
	return hasBits(header, CelestiaMessageHeaderFlag)
}

type CelestiaDA struct {
	Cfg       *DAConfig
	Client    *openrpc.Client
	Namespace *share.Namespace
	Prover    *CelestiaProver
}

type CelestiaProver struct {
	Trpc        *http.HTTP
	EthClient   *ethclient.Client
	BlobstreamX *blobstreamx.BlobstreamX
}

func CelestiaDAConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.Bool(prefix+".enable", false, "Enable Celestia DA")
	f.Float64(prefix+".gas-price", 0.1, "Gas for Celestia transactions")
	f.String(prefix+".rpc", "", "Rpc endpoint for celestia-node")
	f.String(prefix+".namespace-id", "", "Celestia Namespace to post data to")
	f.String(prefix+".auth-token", "", "Auth token for Celestia Node")
	f.String(prefix+".validator-config"+".tendermint-rpc", "", "Tendermint RPC endpoint, only used for validation")
	f.String(prefix+".validator-config"+".eth-ws", "", "L1 Websocket connection, only used for validation")
	f.String(prefix+".validator-config"+".blobstream", "", "Blobstream address, only used for validation")
}

func NewCelestiaDA(cfg *DAConfig, ethClient *ethclient.Client) (*CelestiaDA, error) {
	if cfg == nil {
		return nil, errors.New("celestia cfg cannot be blank")
	}
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

	if cfg.ValidatorConfig != nil {
		trpc, err := http.New(cfg.ValidatorConfig.TendermintRPC, "/websocket")
		if err != nil {
			log.Error("Unable to establish connection with celestia-core tendermint rpc")
			return nil, err
		}
		err = trpc.Start()
		if err != nil {
			return nil, err
		}

		var ethRpc *ethclient.Client
		if ethClient != nil {
			ethRpc = ethClient
		} else if len(cfg.ValidatorConfig.EthClient) > 0 {
			ethRpc, err = ethclient.Dial(cfg.ValidatorConfig.EthClient)
			if err != nil {
				return nil, err
			}
		}

		blobstreamx, err := blobstreamx.NewBlobstreamX(common.HexToAddress(cfg.ValidatorConfig.BlobstreamAddr), ethClient)
		if err != nil {
			return nil, err
		}

		return &CelestiaDA{
			Cfg:       cfg,
			Client:    daClient,
			Namespace: &namespace,
			Prover: &CelestiaProver{
				Trpc:        trpc,
				EthClient:   ethRpc,
				BlobstreamX: blobstreamx,
			},
		}, nil

	}

	return &CelestiaDA{
		Cfg:       cfg,
		Client:    daClient,
		Namespace: &namespace,
	}, nil
}

// TODO (Diego): add retry logic and gas fee bumps
func (c *CelestiaDA) Store(ctx context.Context, message []byte) ([]byte, error) {

	dataBlob, err := blob.NewBlobV0(*c.Namespace, message)
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

	proofs, err := c.Client.Blob.GetProof(ctx, height, *c.Namespace, commitment)
	if err != nil {
		log.Warn("Error retrieving proof", "err", err)
		return nil, err
	}

	included, err := c.Client.Blob.Included(ctx, height, *c.Namespace, proofs, commitment)
	if err != nil || !included {
		log.Warn("Error checking for inclusion", "err", err, "proof", proofs)
		return nil, err
	}
	log.Info("Succesfully posted blob", "height", height, "commitment", hex.EncodeToString(commitment))

	// we fetch the blob so that we can get the correct start index in the square
	blob, err := c.Client.Blob.Get(ctx, height, *c.Namespace, commitment)
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
	SquareSize  uint64 // Refers to original data square size
	StartRow    uint64
	EndRow      uint64
}

func (c *CelestiaDA) Read(ctx context.Context, blobPointer *BlobPointer) ([]byte, *SquareData, error) {
	// Wait until our client is synced
	err := c.Client.Header.SyncWait(ctx)
	if err != nil {
		return nil, nil, err
	}

	header, err := c.Client.Header.GetByHeight(ctx, blobPointer.BlockHeight)
	if err != nil {
		return nil, nil, err
	}

	headerDataHash := [32]byte{}
	copy(headerDataHash[:], header.DataHash)
	if headerDataHash != blobPointer.DataRoot {
		log.Error("Data Root mismatch", " header.DataHash", header.DataHash, "blobPointer.DataRoot", hex.EncodeToString(blobPointer.DataRoot[:]))
		return []byte{}, nil, nil
	}

	proofs, err := c.Client.Blob.GetProof(ctx, blobPointer.BlockHeight, *c.Namespace, blobPointer.TxCommitment[:])
	if err != nil {
		log.Error("Error retrieving proof", "err", err)
		return []byte{}, nil, nil
	}

	sharesLength := uint64(0)
	for _, proof := range *proofs {
		sharesLength += uint64(proof.End()) - uint64(proof.Start())
	}

	if sharesLength != blobPointer.SharesLength {
		log.Error("Share length mismatch", "sharesLength", sharesLength, "blobPointer.SharesLength", blobPointer.SharesLength)
		return []byte{}, nil, nil
	}

	blob, err := c.Client.Blob.Get(ctx, blobPointer.BlockHeight, *c.Namespace, blobPointer.TxCommitment[:])
	if err != nil {
		// return an empty batch of data because we could not find the blob from the sequencer message
		log.Error("Failed to get blob", "height", blobPointer.BlockHeight, "commitment", hex.EncodeToString(blobPointer.TxCommitment[:]))
		return []byte{}, nil, nil
	}

	eds, err := c.Client.Share.GetEDS(ctx, header)
	if err != nil {
		log.Error("Failed to get EDS", "height", blobPointer.BlockHeight)
		return []byte{}, nil, nil
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

func (c *CelestiaDA) GetProof(ctx context.Context, msg []byte) ([]byte, error) {
	if c.Prover == nil {
		return nil, fmt.Errorf("no celestia prover config found")
	}

	buf := bytes.NewBuffer(msg)
	msgLength := uint32(len(msg) + 1)
	blobPointer := BlobPointer{}
	blobBytes := buf.Bytes()
	err := blobPointer.UnmarshalBinary(blobBytes)
	if err != nil {
		log.Error("Couldn't unmarshal Celestia blob pointer", "err", err)
		return nil, nil
	}

	// Get data root from a celestia node
	header, err := c.Client.Header.GetByHeight(ctx, blobPointer.BlockHeight)
	if err != nil {
		log.Warn("Header retrieval error", "err", err)
		return nil, err
	}

	latestBlockNumber, err := c.Prover.EthClient.BlockNumber(context.Background())
	if err != nil {
		return nil, err
	}

	// check the latest celestia block on the Blobstream contract
	latestCelestiaBlock, err := c.Prover.BlobstreamX.LatestBlock(&bind.CallOpts{
		Pending:     false,
		BlockNumber: big.NewInt(int64(latestBlockNumber)),
		Context:     ctx,
	})
	if err != nil {
		return nil, err
	}

	var backwards bool
	if blobPointer.BlockHeight < latestCelestiaBlock {
		backwards = true
	} else {
		backwards = false
	}

	var event *blobstreamx.BlobstreamXDataCommitmentStored

	event, err = c.filter(ctx, latestBlockNumber, blobPointer.BlockHeight, backwards)
	if err != nil {
		return nil, err
	}

	// get the block data root inclusion proof to the data root tuple root
	dataRootProof, err := c.Prover.Trpc.DataRootInclusionProof(ctx, blobPointer.BlockHeight, event.StartBlock, event.EndBlock)
	if err != nil {
		return nil, err
	}

	// verify that the data root was committed to by the BlobstreamX contract
	sideNodes := make([][32]byte, len(dataRootProof.Proof.Aunts))
	for i, aunt := range dataRootProof.Proof.Aunts {
		sideNodes[i] = *(*[32]byte)(aunt)
	}

	tuple := blobstreamx.DataRootTuple{
		Height:   big.NewInt(int64(blobPointer.BlockHeight)),
		DataRoot: [32]byte(header.DataHash),
	}

	proof := blobstreamx.BinaryMerkleProof{
		SideNodes: sideNodes,
		Key:       big.NewInt(dataRootProof.Proof.Index),
		NumLeaves: big.NewInt(dataRootProof.Proof.Total),
	}

	valid, err := c.Prover.BlobstreamX.VerifyAttestation(
		&bind.CallOpts{},
		event.ProofNonce,
		tuple,
		proof,
	)
	if err != nil {
		return nil, err
	}

	log.Info("Verified Celestia Attestation", "height", blobPointer.BlockHeight, "valid", valid)

	if valid {
		sharesProof, err := c.Prover.Trpc.ProveShares(ctx, blobPointer.BlockHeight, blobPointer.Start, blobPointer.Start+blobPointer.SharesLength-1)
		if err != nil {
			log.Error("Unable to get ShareProof", "err", err)
			return nil, err
		}

		namespaceNode := toNamespaceNode(sharesProof.RowProof.RowRoots[0])
		rowProof := toRowProofs((sharesProof.RowProof.Proofs[0]))
		attestationProof := toAttestationProof(event.ProofNonce.Uint64(), blobPointer.BlockHeight, blobPointer.DataRoot, dataRootProof)

		celestiaVerifierAbi, err := celestiagen.CelestiaBatchVerifierMetaData.GetAbi()
		if err != nil {
			log.Error("Could not get ABI for Celestia Batch Verifier", "err", err)
			return nil, err
		}

		verifyProofABI := celestiaVerifierAbi.Methods["verifyProof"]

		// need to encode function signature to this
		proofData, err := verifyProofABI.Inputs.Pack(
			common.HexToAddress(c.Cfg.ValidatorConfig.BlobstreamAddr), namespaceNode, rowProof, attestationProof,
		)
		if err != nil {
			log.Error("Could not pack structs into ABI", "err", err)
			return nil, err
		}

		// apend size of batch + proofData
		sizeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBytes, uint32((len(proofData)))+msgLength)
		proofData = append(proofData, sizeBytes...)

		return proofData, nil
	}

	return nil, err
}

func (c *CelestiaDA) filter(ctx context.Context, latestBlock uint64, celestiaHeight uint64, backwards bool) (*blobstreamx.BlobstreamXDataCommitmentStored, error) {
	// Geth has a default of 5000 block limit for filters
	start := uint64(0)
	if latestBlock < 5000 {
		start = 0
	}
	end := latestBlock

	for attempt := 0; attempt < int(c.Cfg.ValidatorConfig.MaxIterations); attempt++ {
		eventsIterator, err := c.Prover.BlobstreamX.FilterDataCommitmentStored(
			&bind.FilterOpts{
				Context: ctx,
				Start:   start,
				End:     &end,
			},
			nil,
			nil,
			nil,
		)
		if err != nil {
			log.Error("Error creating event iterator", "err", err)
			return nil, err
		}

		var event *blobstreamx.BlobstreamXDataCommitmentStored
		for eventsIterator.Next() {
			e := eventsIterator.Event
			if e.StartBlock <= celestiaHeight && celestiaHeight < e.EndBlock {
				event = &blobstreamx.BlobstreamXDataCommitmentStored{
					ProofNonce:     e.ProofNonce,
					StartBlock:     e.StartBlock,
					EndBlock:       e.EndBlock,
					DataCommitment: e.DataCommitment,
				}
				break
			}
		}
		if err := eventsIterator.Error(); err != nil {
			return nil, err
		}
		err = eventsIterator.Close()
		if err != nil {
			return nil, err
		}
		if event != nil {
			log.Info("Found Data Root submission event", "proof_nonce", event.ProofNonce, "start", event.StartBlock, "end", event.EndBlock)
			return event, nil
		}

		if backwards {
			start -= 5000
			if end < 5000 {
				end = start + 10
			} else {
				end -= 5000
			}
		} else {
			time.Sleep(time.Second * 3600)
			latestBlockNumber, err := c.Prover.EthClient.BlockNumber(context.Background())
			if err != nil {
				return nil, err
			}

			start = end
			end = latestBlockNumber
		}
	}

	return nil, fmt.Errorf("unable to find Data Commitment Stored event in Blobstream")
}

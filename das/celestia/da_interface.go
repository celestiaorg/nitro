package celestia

import (
	"context"
)

type DataAvailabilityWriter interface {
	Store(context.Context, []byte) (*BlobPointer, bool, error)
	WaitForRelay(context.Context, uint64) error
	Verify(ctx context.Context, blobPointer *BlobPointer, beginBlock uint64, endBlock uint64) (bool, error)
	Serialize(blobPointer *BlobPointer) ([]byte, error)
}

type DataAvailabilityReader interface {
	Read(context.Context, *BlobPointer) ([]byte, *SquareData, error)
}

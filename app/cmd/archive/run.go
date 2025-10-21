package archive

import (
	"context"
	"errors"

	"github.com/simulot/immich-go/adapters"
	"github.com/simulot/immich-go/adapters/folder"
	"github.com/simulot/immich-go/app"
	"github.com/simulot/immich-go/internal/fileevent"
)

// run processes assets from a source reader into a destination writer
func run(ctx context.Context, jnl *fileevent.Recorder, _ *app.Application, source adapters.Reader, dest adapters.AssetWriter) error {
	localDest, ok := dest.(*folder.LocalAssetWriter)
	var existingFiles map[string]bool

	if ok {
		jnl.Log().Info("Scanning existing files in destination")
		var err error
		existingFiles, err = localDest.ScanExistingFiles(ctx)
		if err != nil {
			return err
		}
		jnl.Log().Info("Found existing files", "count", len(existingFiles))
	}

	gChan := source.Browse(ctx)
	errCount := 0
	skippedCount := 0
	writtenCount := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case g, ok := <-gChan:
			if !ok {
				jnl.Log().Info("Summary", "written", writtenCount, "skipped", skippedCount)
				return nil
			}

			for _, a := range g.Assets {
				if localDest != nil && existingFiles != nil {
					destPath := localDest.MakePathOfAsset(a)
					if existingFiles[destPath] {
						// Asset already exists locally, skip writing and suppress server log
						jnl.Record(ctx, fileevent.UploadServerDuplicate, a)
						skippedCount++
						continue
					}
				}

				// Write the asset
				err := dest.WriteAsset(ctx, a)
				if errors.Is(err, folder.ErrFileExists) {
					jnl.Record(ctx, fileevent.UploadServerDuplicate, a)
					skippedCount++
					continue
				} else if err != nil {
					jnl.Log().Error(err.Error())
					errCount++
					if errCount > 5 {
						return errors.New("too many errors, aborting")
					}
					continue
				}

				_ = a.Close()
				jnl.Record(ctx, fileevent.Written, a)
				writtenCount++
			}
		}
	}
}

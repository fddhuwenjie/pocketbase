package apis

import (
	"context"
	"net/http"
	"time"

	"github.com/pocketbase/pocketbase/core"
)

// backupCollectionRestore handles the synchronous restore of a single
// existing collection from an existing instance backup.
//
// Unlike the full backup restore, the operation completes before the response
// is returned and the application is NOT restarted.
func backupCollectionRestore(e *core.RequestEvent) error {
	// reject early if a backup create/restore operation is already running
	if e.App.Store().Has(core.StoreKeyActiveBackup) {
		return e.BadRequestError("Try again later - another backup/restore process has already been started.", nil)
	}

	key := e.Request.PathValue("key")
	collection := e.Request.PathValue("collection")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := e.App.RestoreBackupCollection(ctx, key, collection); err != nil {
		return e.BadRequestError("Failed to restore the requested collection backup.", err)
	}

	return e.NoContent(http.StatusNoContent)
}

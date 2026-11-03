package core

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/archive"
	"github.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketbase/pocketbase/tools/list"
	"github.com/pocketbase/pocketbase/tools/security"
)

// collectionRestoreBatchSize limits the number of rows inserted with a single
// INSERT statement to avoid exceeding the SQLite parameter limits.
const collectionRestoreBatchSize = 200

// RestoreBackupCollection synchronously restores the records and the local
// storage files of a single existing collection from the specified backup
// without restarting the application.
//
// The backupName argument is an existing backup key (see CreateBackup) and
// collectionNameOrId is the name or the id of a currently existing regular
// (non-view) collection.
//
// On success:
//
//   - The target collection table is replaced with the backup one, preserving
//     the original record ids and field values.
//   - Records created after the backup in the target collection are removed.
//   - The collection local storage files (including thumbs) are replaced with
//     the backup ones.
//   - All other collections and their records/files remain untouched.
//
// The operation is all-or-nothing and returns an error without modifying the
// current state (apart from best-effort S3 sync) when:
//
//   - Another backup create/restore operation is in progress.
//   - The backup or the target collection doesn't exist (or the collection is
//     a view or is missing from the backup).
//   - The target collection type/field/table definitions have changed.
//   - A local file referenced by a backup record is missing from the backup.
//   - Removing post-backup records would leave dangling relations in other
//     collections.
func (app *BaseApp) RestoreBackupCollection(ctx context.Context, backupName, collectionNameOrId string) error {
	if app.Store().Has(StoreKeyActiveBackup) {
		return errors.New("try again later - another backup/restore operation has already been started")
	}

	// prevent concurrent backup create/restore operations for the whole duration
	app.Store().Set(StoreKeyActiveBackup, backupName)
	defer app.Store().Remove(StoreKeyActiveBackup)

	// resolve the current target collection
	collection, err := app.FindCollectionByNameOrId(collectionNameOrId)
	if err != nil {
		return fmt.Errorf("target collection %q doesn't exist: %w", collectionNameOrId, err)
	}
	if collection.IsView() {
		return fmt.Errorf("collection %q is a view and cannot be restored because it doesn't store its own records", collection.Name)
	}

	// prepare the temp dir (inside pb_data to avoid "cross-device link" errors)
	localTempDir := filepath.Join(app.DataDir(), LocalTempDirName)
	if err := os.MkdirAll(localTempDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create a temp dir: %w", err)
	}

	extractedDataDir := filepath.Join(localTempDir, "pb_col_restore_"+security.PseudorandomString(8))
	if err := os.MkdirAll(extractedDataDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create a temp dir: %w", err)
	}
	defer os.RemoveAll(extractedDataDir)

	if err := extractBackupArchive(ctx, app, backupName, extractedDataDir); err != nil {
		return err
	}

	// open the backup database
	backupDBPath := filepath.Join(extractedDataDir, dataDBFilename)
	if _, err := os.Stat(backupDBPath); err != nil {
		return fmt.Errorf("the backup %q is invalid - missing data.db file: %w", backupName, err)
	}

	backupDB, err := DefaultDBConnect(backupDBPath)
	if err != nil {
		return fmt.Errorf("failed to open the backup database: %w", err)
	}
	backupDB.DB().SetMaxOpenConns(1)
	defer backupDB.Close()

	// the collection is matched by id since both the relation fields and
	// the storage files are referencing the collection id
	backupCollection := &Collection{}
	err = backupDB.Select("*").
		From(collection.TableName()).
		AndWhere(dbx.HashExp{"id": collection.Id}).
		Limit(1).
		One(backupCollection)
	if err != nil {
		return fmt.Errorf("the backup %q doesn't contain collection %q: %w", backupName, collection.Name, err)
	}

	if err := ensureCollectionDefinitionsMatch(collection, backupCollection); err != nil {
		return err
	}

	// ensure that the physical records tables have identical columns
	currentColumns, err := loadTableColumnNames(app.NonconcurrentDB(), collection.Name)
	if err != nil {
		return fmt.Errorf("failed to inspect the current records table: %w", err)
	}
	backupColumns, err := loadTableColumnNames(backupDB, backupCollection.Name)
	if err != nil {
		return fmt.Errorf("failed to inspect the backup records table: %w", err)
	}
	if len(backupColumns) == 0 {
		return fmt.Errorf("the backup %q doesn't contain the records table for collection %q", backupName, collection.Name)
	}
	if !slices.Equal(currentColumns, backupColumns) {
		return fmt.Errorf("cannot restore collection %q: the records table schema has changed since the backup was created", collection.Name)
	}

	// load all backup rows (raw to preserve the exact stored values)
	backupRows, err := readBackupCollectionRows(ctx, backupDB, backupCollection.Name, backupColumns)
	if err != nil {
		return fmt.Errorf("failed to read the backup records for collection %q: %w", collection.Name, err)
	}

	// verify that all local files referenced by the backup records exist in it
	backupFileKeys, err := listBackupCollectionStorageFiles(extractedDataDir, collection.Id)
	if err != nil {
		return err
	}
	if err := ensureBackupRecordFilesExist(collection, backupRows, backupFileKeys); err != nil {
		return err
	}

	// preload all currently defined inbound relations from other collections
	references, err := app.FindCollectionReferences(collection, collection.Id)
	if err != nil {
		return fmt.Errorf("failed to load the collection references: %w", err)
	}

	backupRecordIds := make(map[string]struct{}, len(backupRows))
	for _, row := range backupRows {
		backupRecordIds[asString(row[FieldNameId])] = struct{}{}
	}

	// replace the local storage files before the db transaction so that
	// the change can be reverted if the later validation or db action fails
	filesChange, err := applyBackupCollectionFiles(ctx, app, extractedDataDir, collection.Id)
	if err != nil {
		return err
	}

	txErr := app.RunInTransaction(func(txApp App) error {
		// ids that currently exist but are missing from the backup,
		// aka. the records that will disappear with the restore
		idsToDelete, err := findCurrentRecordIdsMissingFromBackup(txApp, collection.Name, backupRecordIds)
		if err != nil {
			return err
		}

		if len(idsToDelete) > 0 {
			if err := ensureNoDanglingCollectionReferences(txApp, collection, references, idsToDelete); err != nil {
				return err
			}
		}

		// replace the records without going through the model hooks
		// (no cascade deletes, no realtime broadcasts, no autodate changes)
		if _, err := txApp.NonconcurrentDB().
			NewQuery("DELETE FROM " + quoteSQLIdentifier(collection.Name)).
			Execute(); err != nil {
			return fmt.Errorf("failed to clear the current collection records: %w", err)
		}

		if err := insertBackupCollectionRows(txApp, collection.Name, backupColumns, backupRows); err != nil {
			return err
		}

		return nil
	})
	if txErr != nil {
		if revertErr := filesChange.Revert(); revertErr != nil {
			app.Logger().Warn(
				"Failed to revert the collection storage files after an unsuccessful collection restore",
				slog.String("collection", collection.Name),
				slog.String("error", revertErr.Error()),
			)
		}

		return txErr
	}

	if err := filesChange.Cleanup(); err != nil {
		app.Logger().Warn(
			"Failed to cleanup the old collection storage files after a successful restore",
			slog.String("collection", collection.Name),
			slog.String("error", err.Error()),
		)
	}

	app.Logger().Info(
		"Collection restored from backup",
		slog.String("backup", backupName),
		slog.String("collection", collection.Name),
		slog.Int("records", len(backupRows)),
	)

	return nil
}

// extractBackupArchive downloads (if S3 backups are configured) and extracts
// the backup with the specified name into destDir.
func extractBackupArchive(ctx context.Context, app App, name, destDir string) error {
	fsys, err := app.NewBackupsFilesystem()
	if err != nil {
		return err
	}
	defer fsys.Close()

	fsys.SetContext(ctx)

	if ok, _ := fsys.Exists(name); !ok {
		return fmt.Errorf("missing or invalid backup file %q", name)
	}

	if app.Settings().Backups.S3.Enabled {
		br, err := fsys.GetReader(name)
		if err != nil {
			return err
		}
		defer br.Close()

		tempZip, err := os.CreateTemp(filepath.Dir(destDir), "pb_col_restore_zip_*")
		if err != nil {
			return err
		}
		defer os.Remove(tempZip.Name())
		defer tempZip.Close()

		if _, err := io.Copy(tempZip, br); err != nil {
			return err
		}
		if err := tempZip.Close(); err != nil {
			return err
		}

		return archive.Extract(tempZip.Name(), destDir)
	}

	// local backups - construct the path manually to avoid creating a zip copy
	zipPath := filepath.Join(app.DataDir(), LocalBackupsDirName, filepath.Base(name))

	return archive.Extract(zipPath, destDir)
}

// ensureCollectionDefinitionsMatch verifies that the target collection type
// and field definitions are identical between the current state and the backup.
func ensureCollectionDefinitionsMatch(current, backup *Collection) error {
	if backup.Type != current.Type {
		return fmt.Errorf("cannot restore collection %q: the collection type has changed since the backup was created", current.Name)
	}

	currentFields, err := json.Marshal(current.Fields, json.Deterministic(true))
	if err != nil {
		return err
	}

	backupFields, err := json.Marshal(backup.Fields, json.Deterministic(true))
	if err != nil {
		return err
	}

	if !bytes.Equal(currentFields, backupFields) {
		return fmt.Errorf("cannot restore collection %q: the field definitions have changed since the backup was created", current.Name)
	}

	return nil
}

// quoteSQLIdentifier returns the safely quoted form of the provided
// SQLite identifier (table or column name).
func quoteSQLIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// pragmaTableColumn is a single row from PRAGMA table_info used by
// loadTableColumnNames.
type pragmaTableColumn struct {
	CID  int    `db:"cid"`
	Name string `db:"name"`
}

// loadTableColumnNames returns the ordered column names for the specified table.
func loadTableColumnNames(builder dbx.Builder, table string) ([]string, error) {
	columnsInfo := []pragmaTableColumn{}

	err := builder.
		NewQuery("PRAGMA table_info(" + quoteSQLIdentifier(table) + ")").
		All(&columnsInfo)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(columnsInfo))
	for _, c := range columnsInfo {
		columns = append(columns, c.Name)
	}

	return columns, nil
}

// readBackupCollectionRows reads all rows from the specified backup table
// preserving the raw driver values so that they can be reinserted as-is.
func readBackupCollectionRows(ctx context.Context, builder dbx.Builder, table string, columns []string) ([]map[string]any, error) {
	quotedColumns := make([]string, len(columns))
	for i, c := range columns {
		quotedColumns[i] = quoteSQLIdentifier(c)
	}

	query := "SELECT " + strings.Join(quotedColumns, ", ") + " FROM " + quoteSQLIdentifier(table)

	rows, err := builder.NewQuery(query).WithContext(ctx).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []map[string]any{}

	for rows.Next() {
		values := make([]any, len(columns))
		holders := make([]any, len(columns))
		for i := range values {
			holders[i] = &values[i]
		}

		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(columns))
		for i, c := range columns {
			row[c] = values[i]
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// listBackupCollectionStorageFiles lists (as storage keys relative to the
// storage root) all files available in the extracted backup for the specified
// collection (including thumbs and any other regular file).
func listBackupCollectionStorageFiles(extractedDataDir, collectionId string) (map[string]struct{}, error) {
	storageRoot := filepath.Join(extractedDataDir, LocalStorageDirName)
	collectionDir := filepath.Join(storageRoot, collectionId)

	result := map[string]struct{}{}

	err := filepath.WalkDir(collectionDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && filepath.Clean(p) == filepath.Clean(collectionDir) {
				return filepath.SkipAll
			}
			return err
		}

		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(storageRoot, p)
		if err != nil {
			return err
		}

		result[filepath.ToSlash(rel)] = struct{}{}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list the backup storage files: %w", err)
	}

	return result, nil
}

// ensureBackupRecordFilesExist verifies that every file referenced by a file
// field value in the backup records is available in the backup storage.
func ensureBackupRecordFilesExist(collection *Collection, rows []map[string]any, availableFiles map[string]struct{}) error {
	fileFields := []*FileField{}
	for _, f := range collection.Fields {
		if ff, ok := f.(*FileField); ok {
			fileFields = append(fileFields, ff)
		}
	}

	if len(fileFields) == 0 {
		return nil
	}

	for _, row := range rows {
		recordId := asString(row[FieldNameId])

		for _, field := range fileFields {
			raw, ok := row[field.Name]
			if !ok {
				continue
			}

			for _, filename := range extractStoredFileNames(raw, field.IsMultiple()) {
				key := collection.Id + "/" + recordId + "/" + filename
				if _, ok := availableFiles[key]; !ok {
					return fmt.Errorf(
						"cannot restore collection %q: the backup record %q references missing local file %q",
						collection.Name,
						recordId,
						filename,
					)
				}
			}
		}
	}

	return nil
}

// extractStoredFileNames extracts the plain file name(s) stored in a file
// field column (single TEXT value or JSON array for multi-select fields).
func extractStoredFileNames(raw any, multiple bool) []string {
	data, ok := raw.([]byte)
	if !ok {
		if s, ok := raw.(string); ok {
			data = []byte(s)
		}
	}

	if len(data) == 0 {
		return nil
	}

	if !multiple {
		name := strings.TrimSpace(string(data))
		if name == "" {
			return nil
		}
		return []string{name}
	}

	names := []string{}
	if err := json.Unmarshal(data, &names); err != nil {
		return nil
	}

	return names
}

// asString converts the raw driver value to its string representation.
func asString(raw any) string {
	switch v := raw.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// findCurrentRecordIdsMissingFromBackup returns the ids of the records that
// currently exist in the target table but are missing from the backup,
// aka. the records that will be removed by the restore.
func findCurrentRecordIdsMissingFromBackup(app App, table string, backupRecordIds map[string]struct{}) ([]string, error) {
	rows, err := app.NonconcurrentDB().
		NewQuery("SELECT id FROM " + quoteSQLIdentifier(table)).
		Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to load the current record ids: %w", err)
	}
	defer rows.Close()

	result := []string{}

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		if _, ok := backupRecordIds[id]; !ok {
			result = append(result, id)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// ensureNoDanglingCollectionReferences verifies that no record in any other
// collection references a record from the target collection that is about to
// be deleted through a relation field.
func ensureNoDanglingCollectionReferences(app App, target *Collection, references map[*Collection][]Field, idsToDelete []string) error {
	for refCollection, fields := range references {
		if refCollection.IsView() {
			continue // views don't store their own records
		}

		for _, rawField := range fields {
			field, ok := rawField.(*RelationField)
			if !ok {
				continue
			}

			for _, chunk := range list.ToChunks(idsToDelete, 400) {
				params := dbx.Params{}
				placeholders := make([]string, len(chunk))
				for i, id := range chunk {
					key := fmt.Sprintf("delId%d", i)
					params[key] = id
					placeholders[i] = "{:" + key + "}"
				}

				var query string
				if field.IsMultiple() {
					query = fmt.Sprintf(
						"SELECT 1 FROM %s r, json_each(r.%s) je WHERE je.value IN (%s) LIMIT 1",
						quoteSQLIdentifier(refCollection.Name),
						quoteSQLIdentifier(field.Name),
						strings.Join(placeholders, ", "),
					)
				} else {
					query = fmt.Sprintf(
						"SELECT 1 FROM %s r WHERE r.%s IN (%s) LIMIT 1",
						quoteSQLIdentifier(refCollection.Name),
						quoteSQLIdentifier(field.Name),
						strings.Join(placeholders, ", "),
					)
				}

				var exists int
				err := app.NonconcurrentDB().
					NewQuery(query).
					Bind(params).
					Row(&exists)
				if err == nil {
					return fmt.Errorf(
						"cannot restore collection %q: restoring it would leave dangling relations in collection %q (field %q)",
						target.Name,
						refCollection.Name,
						field.Name,
					)
				}
				if !errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("failed to verify the relation references in %q.%q: %w", refCollection.Name, field.Name, err)
				}
			}
		}
	}

	return nil
}

// insertBackupCollectionRows reinserts the loaded backup rows in batches,
// preserving the exact stored column values.
func insertBackupCollectionRows(app App, table string, columns []string, rows []map[string]any) error {
	quotedColumns := make([]string, len(columns))
	for i, c := range columns {
		quotedColumns[i] = quoteSQLIdentifier(c)
	}

	for start := 0; start < len(rows); start += collectionRestoreBatchSize {
		end := start + collectionRestoreBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		var sb strings.Builder

		sb.WriteString("INSERT INTO ")
		sb.WriteString(quoteSQLIdentifier(table))
		sb.WriteString(" (")
		sb.WriteString(strings.Join(quotedColumns, ", "))
		sb.WriteString(") VALUES ")

		params := dbx.Params{}

		for r := start; r < end; r++ {
			if r > start {
				sb.WriteString(", ")
			}

			sb.WriteByte('(')

			for c, column := range columns {
				if c > 0 {
					sb.WriteString(", ")
				}

				key := fmt.Sprintf("v_%d_%d", r, c)
				sb.WriteString("{:" + key + "}")
				params[key] = rows[r][column]
			}

			sb.WriteByte(')')
		}

		if _, err := app.NonconcurrentDB().NewQuery(sb.String()).Bind(params).Execute(); err != nil {
			return fmt.Errorf("failed to insert the backup records: %w", err)
		}
	}

	return nil
}

// collectionFilesChange represents the applied storage change and allows
// reverting it (on restore failure) or cleaning up the old files (on success).
type collectionFilesChange struct {
	revert  func() error
	cleanup func() error
}

func (c collectionFilesChange) Revert() error {
	if c.revert != nil {
		return c.revert()
	}
	return nil
}

func (c collectionFilesChange) Cleanup() error {
	if c.cleanup != nil {
		return c.cleanup()
	}
	return nil
}

// applyBackupCollectionFiles replaces the current collection storage with the
// backup one.
//
// For local storage it uses directory renames within pb_data, which allows
// cheap and near-atomic revert in case of a later failure.
//
// When S3 storage is configured the files are synced through the storage
// filesystem (backups created with S3 storage don't contain local files, so
// in that case the file existence validation would already reject restores of
// records referencing files).
func applyBackupCollectionFiles(ctx context.Context, app App, extractedDataDir, collectionId string) (collectionFilesChange, error) {
	noop := collectionFilesChange{}

	if app.Settings().S3.Enabled {
		return noop, syncS3CollectionFiles(ctx, app, extractedDataDir, collectionId)
	}

	storageRoot := filepath.Join(app.DataDir(), LocalStorageDirName)
	currentDir := filepath.Join(storageRoot, collectionId)
	backupDir := filepath.Join(extractedDataDir, LocalStorageDirName, collectionId)
	oldDir := filepath.Join(
		filepath.Dir(extractedDataDir),
		"old_col_storage_"+security.PseudorandomString(8),
	)

	hadCurrent := false
	hadBackup := false

	// move the current collection storage dir aside
	if _, err := os.Stat(currentDir); err == nil {
		hadCurrent = true
		if err := os.Rename(currentDir, oldDir); err != nil {
			return noop, fmt.Errorf("failed to move aside the current collection files: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return noop, err
	}

	revert := func() error {
		// remove the restored dir (if any) and return the backup one back
		// into the extracted location so the deferred cleanup can remove it
		if hadBackup {
			if err := os.Rename(currentDir, backupDir); err != nil {
				return err
			}
		} else {
			if err := os.RemoveAll(currentDir); err != nil {
				return err
			}
		}

		if hadCurrent {
			return os.Rename(oldDir, currentDir)
		}

		return nil
	}

	// move the backup collection storage dir in place
	if _, err := os.Stat(backupDir); err == nil {
		hadBackup = true

		if err := os.MkdirAll(storageRoot, os.ModePerm); err != nil {
			_ = revert()
			return noop, err
		}

		if err := os.Rename(backupDir, currentDir); err != nil {
			_ = revert()
			return noop, fmt.Errorf("failed to apply the backup collection files: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		_ = revert()
		return noop, err
	}

	change := collectionFilesChange{
		revert: revert,
		cleanup: func() error {
			if !hadCurrent {
				return nil
			}
			return os.RemoveAll(oldDir)
		},
	}

	return change, nil
}

// syncS3CollectionFiles uploads the backup collection files to the S3 storage
// and removes the currently stored ones that are missing from the backup.
func syncS3CollectionFiles(ctx context.Context, app App, extractedDataDir, collectionId string) error {
	fsys, err := app.NewFilesystem()
	if err != nil {
		return err
	}
	defer fsys.Close()
	fsys.SetContext(ctx)

	storageRoot := filepath.Join(extractedDataDir, LocalStorageDirName)
	collectionDir := filepath.Join(storageRoot, collectionId)

	backupKeys := map[string]struct{}{}

	// upload the backup files
	if _, statErr := os.Stat(collectionDir); statErr == nil {
		walkErr := filepath.WalkDir(collectionDir, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			rel, err := filepath.Rel(storageRoot, p)
			if err != nil {
				return err
			}

			key := filepath.ToSlash(rel)
			backupKeys[key] = struct{}{}

			file, err := filesystem.NewFileFromPath(p)
			if err != nil {
				return err
			}

			return fsys.UploadFile(file, key)
		})
		if walkErr != nil {
			return fmt.Errorf("failed to upload the backup collection files: %w", walkErr)
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}

	// remove current files that are not present in the backup
	prefix := collectionId + "/"

	current, err := fsys.List(prefix)
	if err != nil {
		return err
	}

	for _, obj := range current {
		if strings.HasSuffix(obj.Key, "/") {
			continue // virtual dir
		}

		if _, ok := backupKeys[obj.Key]; ok {
			continue
		}

		if err := fsys.Delete(obj.Key); err != nil && !errors.Is(err, filesystem.ErrNotFound) {
			return fmt.Errorf("failed to remove extra collection file %q: %w", obj.Key, err)
		}
	}

	return nil
}

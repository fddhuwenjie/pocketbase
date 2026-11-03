package apis_test

import (
	"archive/zip"
	"context"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/archive"
	"github.com/pocketbase/pocketbase/tools/filesystem"
)

const (
	colRestoreSuperuserToken = "eyJhbGciOiJIUzI1NiJ9.eyJpZCI6InN5d2JoZWNuaDQ2cmhtMCIsInR5cGUiOiJhdXRoIiwiY29sbGVjdGlvbklkIjoicGJjXzMxNDI2MzU4MjMiLCJleHAiOjI1MjQ2MDQ0NjEsInJlZnJlc2hhYmxlIjp0cnVlfQ.UXgO3j-0BumcugrFjbd7j0M4MQvbrLggLlcu_YNGjoY"
	colRestoreUserToken      = "eyJhbGciOiJIUzI1NiJ9.eyJpZCI6IjRxMXhsY2xtZmxva3UzMyIsInR5cGUiOiJhdXRoIiwiY29sbGVjdGlvbklkIjoiX3BiX3VzZXJzX2F1dGhfIiwiZXhwIjoyNTI0NjA0NDYxLCJyZWZyZXNoYWJsZSI6dHJ1ZX0.ZT3F0Z3iM-xbGgSG3LEKiEzHrPHr8t8IuHLZGGNuxLo"

	colRestoreDemo3Id   = "wzlqyes4orhoygb"
	colRestoreDemo4Id   = "4d1blo5cuycfaca"
	colRestoreNewDemo3  = "newrec000000001" // 15 chars
	colRestoreNewDemo3b = "newrec000000002"
	colRestoreNewDemo3c = "newrec000000003"
	colRestoreNewDemo4  = "newrel000000001" // 15 chars
	colRestoreNewDemo4b = "newrel000000002"
	colRestoreNewDemo4c = "newrel000000003"
)

func TestBackupCollectionRestore(t *testing.T) {
	t.Parallel()

	scenarios := []tests.ApiScenario{
		{
			Name:            "unauthorized",
			Method:          http.MethodPost,
			URL:             "/api/backups/test.zip/collections/demo3/restore",
			ExpectedStatus:  401,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "authorized as regular user",
			Method: http.MethodPost,
			URL:    "/api/backups/test.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreUserToken,
			},
			ExpectedStatus:  403,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "missing backup",
			Method: http.MethodPost,
			URL:    "/api/backups/missing.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "missing collection",
			Method: http.MethodPost,
			URL:    "/api/backups/test.zip/collections/unknown/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "test.zip")
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "view collection is not restorable",
			Method: http.MethodPost,
			URL:    "/api/backups/test.zip/collections/view1/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "test.zip")
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "active backup/restore process",
			Method: http.MethodPost,
			URL:    "/api/backups/test.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "test.zip")
				app.Store().Set(core.StoreKeyActiveBackup, "")
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "changed field definitions",
			Method: http.MethodPost,
			URL:    "/api/backups/schema.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "schema.zip")

				// add a new field after the backup was created
				collection, err := app.FindCollectionByNameOrId("demo3")
				if err != nil {
					t.Fatal(err)
				}
				collection.Fields.Add(&core.TextField{Name: "added_after_backup"})
				if err := app.Save(collection); err != nil {
					t.Fatal(err)
				}
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				// the current db must remain untouched
				if total := countTableRecords(t, app, "demo3"); total != 4 {
					t.Fatalf("Expected 4 demo3 records, got %d", total)
				}

				collection, err := app.FindCollectionByNameOrId("demo3")
				if err != nil {
					t.Fatal(err)
				}
				if f := collection.Fields.GetByName("added_after_backup"); f == nil {
					t.Fatal("Expected the newly added field to still exist after the failed restore")
				}
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "dangling relation in another collection",
			Method: http.MethodPost,
			URL:    "/api/backups/dangling.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "dangling.zip")

				// post-backup demo3 record...
				mustExec(t, app,
					"INSERT INTO `demo3` (`id`) VALUES ({:id})",
					dbx.Params{"id": colRestoreNewDemo3},
				)

				// ...referenced by a post-backup demo4 record
				mustExec(t, app,
					"INSERT INTO `demo4` (`id`, `rel_one_no_cascade_required`) VALUES ({:id}, {:rel})",
					dbx.Params{"id": colRestoreNewDemo4, "rel": colRestoreNewDemo3},
				)

				// ...and by a post-backup demo4 record using a multi relation
				mustExec(t, app,
					"INSERT INTO `demo4` (`id`, `rel_many_no_cascade_required`) VALUES ({:id}, {:rel})",
					dbx.Params{"id": colRestoreNewDemo4c, "rel": `["` + colRestoreNewDemo3 + `"]`},
				)
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				// nothing should have changed
				if total := countTableRecords(t, app, "demo3"); total != 5 {
					t.Fatalf("Expected 5 demo3 records after failed restore, got %d", total)
				}
				if total := countTableRecords(t, app, "demo4"); total != 4 {
					t.Fatalf("Expected 4 demo4 records after failed restore, got %d", total)
				}

				// remove the blockers and verify that the valid backup can be retried
				mustExec(t, app,
					"DELETE FROM `demo4` WHERE `id` IN ({:a}, {:b})",
					dbx.Params{"a": colRestoreNewDemo4, "b": colRestoreNewDemo4c},
				)

				if err := app.RestoreBackupCollection(context.Background(), "dangling.zip", "demo3"); err != nil {
					t.Fatalf("Expected successful retry, got error: %v", err)
				}

				if total := countTableRecords(t, app, "demo3"); total != 4 {
					t.Fatalf("Expected 4 demo3 records after retry, got %d", total)
				}
				if total := countTableRecords(t, app, "demo4"); total != 2 {
					t.Fatalf("Expected 2 demo4 records after retry, got %d", total)
				}
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "backup record references missing local file",
			Method: http.MethodPost,
			URL:    "/api/backups/broken.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "valid.zip")

				// build a broken backup copy in which one of the files
				// referenced by a backup demo3 record is missing
				repackBackupWithoutFile(
					t,
					app,
					"valid.zip",
					"broken.zip",
					colRestoreDemo3Id+"/7nwo8tuiatetxdm/test_JnXeKEwgwr.txt",
				)
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				if total := countTableRecords(t, app, "demo3"); total != 4 {
					t.Fatalf("Expected 4 demo3 records, got %d", total)
				}

				fsys := mustNewStorage(t, app)
				defer fsys.Close()

				if ok, _ := fsys.Exists(colRestoreDemo3Id + "/7nwo8tuiatetxdm/test_JnXeKEwgwr.txt"); !ok {
					t.Fatal("Expected the current storage file to remain untouched")
				}
			},
			ExpectedStatus:  400,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "successful restore by collection name",
			Method: http.MethodPost,
			URL:    "/api/backups/success.zip/collections/demo3/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "success.zip")

				// modify an existing record
				mustExec(t, app,
					"UPDATE `demo3` SET `title`={:title} WHERE `id`={:id}",
					dbx.Params{"title": "mutated title", "id": "lcl9d87w22ml6jy"},
				)

				// add a post-backup record
				mustExec(t, app,
					"INSERT INTO `demo3` (`id`, `title`) VALUES ({:id}, {:title})",
					dbx.Params{"id": colRestoreNewDemo3b, "title": "post backup record"},
				)

				fsys := mustNewStorage(t, app)
				defer fsys.Close()

				// delete an existing file
				if err := fsys.Delete(colRestoreDemo3Id + "/7nwo8tuiatetxdm/test_JnXeKEwgwr.txt"); err != nil {
					t.Fatal(err)
				}

				// add post-backup files
				if err := fsys.Upload([]byte("extra"), colRestoreDemo3Id+"/7nwo8tuiatetxdm/extra_after_backup.txt"); err != nil {
					t.Fatal(err)
				}
				if err := fsys.Upload([]byte("extra"), colRestoreDemo3Id+"/"+colRestoreNewDemo3b+"/f.txt"); err != nil {
					t.Fatal(err)
				}

				// a post-backup record in another collection referencing a surviving record
				mustExec(t, app,
					"INSERT INTO `demo4` (`id`, `rel_one_no_cascade_required`) VALUES ({:id}, {:rel})",
					dbx.Params{"id": colRestoreNewDemo4b, "rel": "lcl9d87w22ml6jy"},
				)
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				// the modified record is reverted to its backup state
				title := queryString(t, app,
					"SELECT `title` FROM `demo3` WHERE `id`={:id}",
					dbx.Params{"id": "lcl9d87w22ml6jy"},
				)
				if title != "test2" {
					t.Fatalf("Expected restored title %q, got %q", "test2", title)
				}

				// the post-backup record is gone
				if total := countTableRecords(t, app, "demo3"); total != 4 {
					t.Fatalf("Expected 4 demo3 records, got %d", total)
				}

				// the record files column is back to its backup state
				filesValue := queryString(t, app,
					"SELECT `files` FROM `demo3` WHERE `id`={:id}",
					dbx.Params{"id": "7nwo8tuiatetxdm"},
				)
				if filesValue != `["test_JnXeKEwgwr.txt"]` {
					t.Fatalf("Expected restored files value, got %q", filesValue)
				}

				fsys := mustNewStorage(t, app)
				defer fsys.Close()

				expectations := map[string]bool{
					// deleted file is restored
					colRestoreDemo3Id + "/7nwo8tuiatetxdm/test_JnXeKEwgwr.txt": true,
					// untouched files of other records remain
					colRestoreDemo3Id + "/lcl9d87w22ml6jy/test_FLurQTgrY8.txt": true,
					colRestoreDemo3Id + "/lcl9d87w22ml6jy/300_UhLKX91HVb.png":  true,
					colRestoreDemo3Id + "/mk5fmymtx4wsprk/300_JdfBOieXAW.png":  true,
					// file thumbs are also restored
					colRestoreDemo3Id + "/lcl9d87w22ml6jy/thumbs_300_UhLKX91HVb.png/100x100_300_UhLKX91HVb.png": true,
					// post-backup files are removed
					colRestoreDemo3Id + "/7nwo8tuiatetxdm/extra_after_backup.txt": false,
					colRestoreDemo3Id + "/" + colRestoreNewDemo3b + "/f.txt":      false,
				}
				for key, expected := range expectations {
					exists, _ := fsys.Exists(key)
					if exists != expected {
						t.Fatalf("Expected file %q existence to be %v, got %v", key, expected, exists)
					}
				}

				// other collections remain untouched, including the post-backup record
				if total := countTableRecords(t, app, "demo4"); total != 3 {
					t.Fatalf("Expected 3 demo4 records, got %d", total)
				}
				rel := queryString(t, app,
					"SELECT `rel_one_no_cascade_required` FROM `demo4` WHERE `id`={:id}",
					dbx.Params{"id": colRestoreNewDemo4b},
				)
				if rel != "lcl9d87w22ml6jy" {
					t.Fatalf("Expected the post-backup demo4 relation to be preserved, got %q", rel)
				}

				// the backup itself is still available
				backupFsys, err := app.NewBackupsFilesystem()
				if err != nil {
					t.Fatal(err)
				}
				defer backupFsys.Close()
				if ok, _ := backupFsys.Exists("success.zip"); !ok {
					t.Fatal("Expected the backup file to still exist")
				}
			},
			ExpectedStatus: 204,
			ExpectedEvents: map[string]int{"*": 0},
		},
		{
			Name:   "successful restore by collection id",
			Method: http.MethodPost,
			URL:    "/api/backups/byid.zip/collections/" + colRestoreDemo3Id + "/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "byid.zip")

				mustExec(t, app,
					"UPDATE `demo3` SET `title`={:title} WHERE `id`={:id}",
					dbx.Params{"title": "mutated", "id": "1tmknxy2868d869"},
				)

				mustExec(t, app,
					"INSERT INTO `demo3` (`id`, `title`) VALUES ({:id}, {:title})",
					dbx.Params{"id": colRestoreNewDemo3c, "title": "post backup"},
				)
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				if total := countTableRecords(t, app, "demo3"); total != 4 {
					t.Fatalf("Expected 4 demo3 records, got %d", total)
				}

				title := queryString(t, app,
					"SELECT `title` FROM `demo3` WHERE `id`={:id}",
					dbx.Params{"id": "1tmknxy2868d869"},
				)
				if title != "test1" {
					t.Fatalf("Expected restored title %q, got %q", "test1", title)
				}
			},
			ExpectedStatus: 204,
			ExpectedEvents: map[string]int{"*": 0},
		},
		{
			Name:   "successful restore of an auth collection",
			Method: http.MethodPost,
			URL:    "/api/backups/users.zip/collections/users/restore",
			Headers: map[string]string{
				"Authorization": colRestoreSuperuserToken,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				mustCreateBackup(t, app, "users.zip")

				// modify an existing user record
				mustExec(t, app,
					"UPDATE `users` SET `name`={:name} WHERE `id`={:id}",
					dbx.Params{"name": "mutated name", "id": "4q1xlclmfloku33"},
				)
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				name := queryString(t, app,
					"SELECT `name` FROM `users` WHERE `id`={:id}",
					dbx.Params{"id": "4q1xlclmfloku33"},
				)
				if name != "test1" {
					t.Fatalf("Expected restored user name %q, got %q", "test1", name)
				}

				// the avatar file referenced by the backup user record is available
				fsys := mustNewStorage(t, app)
				defer fsys.Close()
				if ok, _ := fsys.Exists("_pb_users_auth_/4q1xlclmfloku33/300_1SEi6Q6U72.png"); !ok {
					t.Fatal("Expected the user avatar file to be available after the restore")
				}
			},
			ExpectedStatus: 204,
			ExpectedEvents: map[string]int{"*": 0},
		},
	}

	for _, scenario := range scenarios {
		scenario.Test(t)
	}
}

// -------------------------------------------------------------------

func mustCreateBackup(t testing.TB, app *tests.TestApp, name string) {
	t.Helper()

	if err := app.CreateBackup(context.Background(), name); err != nil {
		t.Fatalf("Failed to create backup %q: %v", name, err)
	}
}

func mustNewStorage(t testing.TB, app *tests.TestApp) *filesystem.System {
	t.Helper()

	fsys, err := app.NewFilesystem()
	if err != nil {
		t.Fatal(err)
	}

	return fsys
}

func mustExec(t testing.TB, app *tests.TestApp, query string, params ...dbx.Params) {
	t.Helper()

	q := app.NonconcurrentDB().NewQuery(query)
	if len(params) > 0 {
		q.Bind(params[0])
	}

	if _, err := q.Execute(); err != nil {
		t.Fatalf("Failed to execute %q: %v", query, err)
	}
}

func countTableRecords(t testing.TB, app *tests.TestApp, table string) int {
	t.Helper()

	var total int
	if err := app.NonconcurrentDB().Select("count(*)").From(table).Row(&total); err != nil {
		t.Fatalf("Failed to count %s records: %v", table, err)
	}

	return total
}

func queryString(t testing.TB, app *tests.TestApp, query string, params dbx.Params) string {
	t.Helper()

	var result string
	if err := app.NonconcurrentDB().NewQuery(query).Bind(params).Row(&result); err != nil {
		t.Fatalf("Failed to execute %q: %v", query, err)
	}

	return result
}

// repackBackupWithoutFile creates a new backup (dstName) from an existing one
// (srcName) while removing a single storage file (relative to the storage root).
func repackBackupWithoutFile(t testing.TB, app *tests.TestApp, srcName, dstName, storageRelKey string) {
	t.Helper()

	backupsDir := filepath.Join(app.DataDir(), core.LocalBackupsDirName)

	tmpDir := t.TempDir()

	if err := archive.Extract(filepath.Join(backupsDir, srcName), tmpDir); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(tmpDir, "storage", filepath.FromSlash(storageRelKey))); err != nil {
		t.Fatal(err)
	}

	out, err := os.Create(filepath.Join(backupsDir, dstName))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	zw := zip.NewWriter(out)

	err = filepath.WalkDir(tmpDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}

		rel, err := filepath.Rel(tmpDir, p)
		if err != nil {
			return err
		}

		w, err := zw.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}

		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(w, f)

		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

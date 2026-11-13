"""Bounded imports and atomic publication of cumulative dataset downloads."""

import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import dataset
import sync_dataset as sync


def artifact(identity, name="results-ubuntu-latest", run=None, created=None, **extra):
    return dict(id=identity, name=name, workflow_run={"id": run or identity},
        created_at=created or f"2026-10-26T00:{identity:02}:00Z", expired=False, **extra)


class SyncDatasetTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.api = Mock(repository="owner/repository")

    def connection(self):
        path = self.root / "benchmarks.sqlite"
        connection = dataset.connect_database(path)
        self.addCleanup(connection.close)
        return connection, path

    def test_selection_keeps_last_checkpoint_per_attempt_and_baseline_fallbacks(self):
        candidates = [artifact(1, "checkpoint-ubuntu-latest-1-1", 100),
            artifact(2, "checkpoint-ubuntu-latest-1-2", 100),
            artifact(3, "checkpoint-ubuntu-latest-2-1", 100),
            artifact(4, "results-ubuntu-latest", 100),
            artifact(5, "retained-ubuntu-latest", 100),
            artifact(6, "compared-results-ubuntu-latest", 100),
            artifact(7, "retained-windows-latest", 100),
            artifact(8, "references-0-1", 200),
            artifact(9, "environment-ubuntu-latest", 100),
            artifact(10, "results-macos-latest", 100),
            artifact(11, "results-ubuntu-latest", 300)]
        candidates[-2]["expired"] = True
        candidates[-1]["workflow_run"] = {}
        self.assertEqual([value["id"] for value in sync.select_artifacts(candidates)], [8, 7, 6, 4, 3, 2])

    def test_download_checks_checksum_before_replacing_existing_file(self):
        api = sync.DatasetAPI("owner/repository", "secret")
        api.opener = Mock()
        path = self.root / "incoming.zip"
        path.write_bytes(b"previous")
        api.opener.open.return_value = io.BytesIO(b"download")
        with self.assertRaisesRegex(ValueError, "checksum"):
            api.download("/actions/artifacts/123/zip", path, expected="0" * 64)
        self.assertEqual(path.read_bytes(), b"previous")
        self.assertFalse(path.with_suffix(".zip.part").exists())
        api.opener.open.return_value = io.BytesIO(b"download")
        expected = hashlib.sha256(b"download").hexdigest()
        self.assertEqual(api.download("/actions/artifacts/123/zip", path, expected=expected), expected)
        self.assertEqual(path.read_bytes(), b"download")
        request = api.opener.open.call_args.args[0]
        self.assertEqual(request.get_header("Accept"), "application/vnd.github+json")

    def test_download_limit_removes_partial_output(self):
        api = sync.DatasetAPI("owner/repository", "secret")
        api.opener = Mock()
        api.opener.open.return_value = io.BytesIO(b"too large")
        path = self.root / "incoming.zip"
        with self.assertRaisesRegex(ValueError, "size limit"):
            api.download("/actions/artifacts/123/zip", path, maximum=2)
        self.assertFalse(path.exists())
        self.assertFalse(path.with_suffix(".zip.part").exists())

    def test_signed_storage_redirect_does_not_forward_authentication(self):
        api = sync.DatasetAPI("owner/repository", "secret")
        api.opener = Mock()
        redirect = HTTPError("https://api.github.com/", 302, "redirect",
                             {"Location": "https://release-assets.githubusercontent.com/download?signed=yes"}, None)
        api.opener.open.side_effect = [redirect, io.BytesIO(b"data")]
        api.download("/releases/assets/123", self.root / "data.gz")
        original = api.opener.open.call_args_list[0].args[0]
        redirected = api.opener.open.call_args_list[1].args[0]
        self.assertEqual(original.get_header("Authorization"), "Bearer secret")
        self.assertEqual(original.get_header("Accept"), "application/octet-stream")
        self.assertIsNone(redirected.get_header("Authorization"))

    def test_download_rejects_untrusted_redirect_and_path(self):
        api = sync.DatasetAPI("owner/repository", "secret")
        api.opener = Mock()
        api.opener.open.side_effect = HTTPError("https://api.github.com/", 302, "redirect",
                                               {"Location": "https://example.com/data"}, None)
        with self.assertRaisesRegex(ValueError, "storage host"):
            api.download("/releases/assets/123", self.root / "data.gz")
        with self.assertRaisesRegex(ValueError, "download path"):
            api.download("https://example.com/data", self.root / "data.gz")

    def test_streamed_dataset_download_retries_storage_503(self):
        api = sync.DatasetAPI("owner/repository", "secret")
        api.opener = Mock()
        signed = "https://release-assets.githubusercontent.com/data?signed=private"
        redirect = lambda: HTTPError("https://api.github.com/", 302, "redirect", {"Location": signed}, None)
        api.opener.open.side_effect = [redirect(), HTTPError(signed, 503, "busy", {}, None),
                                      redirect(), io.BytesIO(b"verified")]
        path = self.root / "dataset.gz"
        with patch.object(sync.time, "sleep"):
            api.download("/releases/assets/123", path, hashlib.sha256(b"verified").hexdigest())
        self.assertEqual(path.read_bytes(), b"verified")
        self.assertIsNone(api.opener.open.call_args_list[1].args[0].get_header("Authorization"))
        self.assertIsNone(api.opener.open.call_args_list[3].args[0].get_header("Authorization"))

    def test_multipart_database_roundtrip_preserves_complete_sqlite_content(self):
        connection, database_path = self.connection()
        connection.execute("CREATE TABLE recovery_probe (payload BLOB)")
        payload = b"".join(hashlib.sha256(str(i).encode()).digest() for i in range(256))
        connection.execute("INSERT INTO recovery_probe VALUES (?)", (payload,))
        connection.commit()
        compressed = database_path.with_suffix(".sqlite.gz")
        compressed.write_bytes(gzip.compress(database_path.read_bytes()))
        uploaded = {}
        def upload(release_id, path, name):
            self.assertTrue(sync.ASSET_NAME.fullmatch(name))
            self.assertLessEqual(path.stat().st_size, 1024)
            identity = len(uploaded) + 1
            uploaded[identity] = path.read_bytes()
            return dict(id=identity, browser_download_url=f"https://example.com/{identity}")
        self.api.upload.side_effect = upload
        with patch.object(sync, "MAX_DOWNLOAD", 1024), patch.object(sync, "ASSET_PART_BYTES", 1024):
            asset = sync.upload_asset(self.api, 9, compressed, "123-abcdef")
            self.assertGreater(len(asset["parts"]), 1)
            self.assertFalse(list(self.root.glob("*.part[0-9][0-9][0-9]")))
            def download(path, destination, expected):
                content = uploaded[int(path.rsplit("/", 1)[-1])]
                self.assertEqual(hashlib.sha256(content).hexdigest(), expected)
                Path(destination).write_bytes(content)
            self.api.download.side_effect = download
            restored = self.root / "restored"
            restored.mkdir()
            recovered, _ = sync.restore_database(self.api, {"assets": {"database": asset}}, restored)
            try:
                self.assertEqual(recovered.execute("SELECT payload FROM recovery_probe").fetchone()[0], payload)
            finally:
                recovered.close()

    def test_multipart_checksum_failure_preserves_previous_destination(self):
        destination = self.root / "previous.gz"
        destination.write_bytes(b"previous")
        asset = dict(bytes=3, sha256=hashlib.sha256(b"abc").hexdigest(),
                     parts=[dict(id=1, bytes=3, sha256=hashlib.sha256(b"bad").hexdigest())])
        self.api.download.side_effect = lambda path, output, expected: Path(output).write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "Assembled dataset checksum"):
            sync.restore_asset(self.api, asset, destination)
        self.assertEqual(destination.read_bytes(), b"previous")
        self.assertFalse(list(self.root.glob("*.assembling")))
        self.assertFalse(list(self.root.glob("*.download")))

    def test_failed_multipart_upload_keeps_catalog_and_previous_release_assets(self):
        connection, database_path = self.connection()
        connection.execute("CREATE TABLE recovery_probe (payload BLOB)")
        connection.execute("INSERT INTO recovery_probe VALUES (?)", (bytes(range(256)) * 32,))
        connection.commit()
        self.api.upload.side_effect = [dict(id=1, browser_download_url="https://example.com/1"), RuntimeError("upload failed")]
        with patch.object(sync, "MAX_DOWNLOAD", 256), patch.object(sync, "ASSET_PART_BYTES", 256), \
                patch.object(sync, "ensure_release", return_value={"id": 10}), patch.object(sync, "write_catalog") as write:
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                sync.publish_dataset(self.api, connection, database_path, {"assets": {}}, "previous", {}, [], 0)
        write.assert_not_called()
        self.api.request.assert_not_called()
        self.assertFalse(list(self.root.glob("*.part[0-9][0-9][0-9]")))

    def test_cleanup_retains_every_part_of_previous_database(self):
        connection, path = self.connection()
        old_assets = {"database": {"parts": [dict(id=1), dict(id=2)]}}
        self.api.upload.side_effect = [dict(id=i, browser_download_url=f"https://example.com/{i}") for i in (11, 12, 13)]
        self.api.request.side_effect = lambda method, url, *body: [
            dict(id=i, name=f"dataset-100-abcdef-benchmarks.sqlite.gz.part00{i}") for i in (1, 2, 3)
        ] if method == "GET" else {}
        with patch.object(sync, "ensure_release", return_value={"id": 10, "html_url": "https://example.com/release"}), \
                patch.object(sync, "write_catalog"):
            sync.publish_dataset(self.api, connection, path, {"assets": old_assets}, "old", {}, [], 0)
        self.assertEqual([call.args[1] for call in self.api.request.call_args_list if call.args[0] == "DELETE"],
                         ["/releases/assets/3"])

    def test_restore_uses_catalog_checksum_and_checks_sqlite_integrity(self):
        source = self.root / "source.sqlite"
        connection = dataset.connect_database(source)
        connection.close()
        content = gzip.compress(source.read_bytes())
        checksum = hashlib.sha256(content).hexdigest()
        self.api.download.side_effect = lambda path, destination, expected: Path(destination).write_bytes(content)
        restored, path = sync.restore_database(self.api, {"assets": {"database": {"id": 123, "sha256": checksum}}}, self.root)
        self.addCleanup(restored.close)
        self.assertEqual(path.name, "benchmarks.sqlite")
        self.assertEqual(restored.execute("PRAGMA quick_check").fetchone()[0], "ok")
        self.assertEqual(self.api.download.call_args.args[2], checksum)
        self.assertFalse((self.root / "previous.sqlite.gz").exists())

    def test_failed_asset_upload_does_not_change_published_pointer_or_cleanup(self):
        connection, path = self.connection()
        self.api.upload.side_effect = [dict(id=1, browser_download_url="https://example.com/1"), RuntimeError("upload failed")]
        with patch.object(sync, "ensure_release", return_value={"id": 10}), patch.object(sync, "write_catalog") as write:
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                sync.publish_dataset(self.api, connection, path, {"assets": {}}, "old-sha", {}, [], 0)
        write.assert_not_called()
        self.api.request.assert_not_called()

    def test_publication_uploads_all_assets_before_pointer_and_keeps_prior_generation(self):
        connection, path = self.connection()
        self.api.upload.side_effect = [dict(id=identity, browser_download_url=f"https://example.com/{identity}") for identity in (11, 12, 13)]
        old_assets = {key: {"id": identity} for key, identity in (("database", 1), ("measurements", 2), ("comparisons", 3))}
        self.api.request.side_effect = lambda method, url, *body: [
            dict(id=1, name="dataset-100-abcdef-benchmarks.sqlite.gz"),
            dict(id=4, name="dataset-90-abcdef-benchmarks.sqlite.gz"),
            dict(id=5, name="unrelated-file.txt"),
            dict(id=11, name="dataset-200-abcdef-benchmarks.sqlite.gz")
        ] if method == "GET" else {}
        events = []
        def publish_pointer(api, value, previous):
            self.assertEqual(api.upload.call_count, 3)
            self.assertEqual(previous, "old-sha")
            self.assertFalse(any(call.args[0] == "DELETE" for call in api.request.call_args_list))
            events.append(value)
        with patch.object(sync, "ensure_release", return_value={"id": 10, "html_url": "https://example.com/release"}), patch.object(sync, "write_catalog", side_effect=publish_pointer):
            value = sync.publish_dataset(self.api, connection, path, {"assets": old_assets}, "old-sha", {}, [], 0)
        self.assertEqual(value["previous_assets"], old_assets)
        self.assertEqual(len(events), 1)
        deletes = [call.args[1] for call in self.api.request.call_args_list if call.args[0] == "DELETE"]
        self.assertEqual(deletes, ["/releases/assets/4"])
        for metadata in value["assets"].values():
            self.assertEqual(len(metadata["sha256"]), 64)

    def test_cleanup_reads_every_asset_page_before_deleting(self):
        connection, path = self.connection()
        self.api.upload.side_effect = [dict(id=identity, browser_download_url=f"https://example.com/{identity}") for identity in (11, 12, 13)]
        pages = [[dict(id=identity, name=f"dataset-{identity}-abcdef-benchmarks.sqlite.gz") for identity in range(100, 200)],
                 [dict(id=200, name="dataset-200-abcdef-benchmarks.sqlite.gz")]]
        def response(method, url, *body):
            if method == "GET":
                self.assertFalse(any(call.args[0] == "DELETE" for call in self.api.request.call_args_list))
                return pages.pop(0)
            return {}
        self.api.request.side_effect = response
        with patch.object(sync, "ensure_release", return_value={"id": 10, "html_url": "https://example.com/release"}), patch.object(sync, "write_catalog"):
            sync.publish_dataset(self.api, connection, path, None, None, {}, [], 0)
        self.assertEqual(sum(call.args[0] == "DELETE" for call in self.api.request.call_args_list), 101)

    def test_pointer_conflict_does_not_overwrite_another_generation(self):
        self.api.request.side_effect = sync.APIError(409, "conflict")
        with patch.object(sync, "read_catalog", return_value=({}, "another-sha")):
            with self.assertRaisesRegex(ValueError, "changed concurrently"):
                sync.write_catalog(self.api, {"schema_version": 1}, "original-sha")
        self.assertEqual(self.api.request.call_count, 1)

    def test_unrelated_branch_conflict_retries_same_catalog_sha(self):
        self.api.request.side_effect = [sync.APIError(409, "conflict"), {}]
        with patch.object(sync, "read_catalog", return_value=({}, "original-sha")):
            sync.write_catalog(self.api, {"schema_version": 1}, "original-sha")
        self.assertEqual(self.api.request.call_count, 2)

    def test_no_new_artifacts_avoids_database_restore_and_publication(self):
        catalog = {"processed": {"1": {"name": "results-ubuntu-latest"}}}
        with patch.object(sync, "read_catalog", return_value=(catalog, "sha")), patch.object(sync, "list_artifacts", return_value=[artifact(1)]), patch.object(sync, "restore_database") as restore, patch.object(sync, "publish_dataset") as publish:
            result = sync.synchronize(self.api, self.root)
        self.assertFalse(result["changed"])
        restore.assert_not_called()
        publish.assert_not_called()

    def test_failed_newer_artifact_does_not_starve_untried_older_artifact(self):
        catalog = {"processed": {}, "import_errors": [{"artifact_id": 2, "name": "results-ubuntu-latest", "error": "bad"}]}
        connection = Mock()
        self.api.download.return_value = "a" * 64
        with patch.object(sync, "read_catalog", return_value=(catalog, "sha")), patch.object(sync, "list_artifacts", return_value=[artifact(2), artifact(1)]), patch.object(sync, "restore_database", return_value=(connection, self.root / "data.sqlite")), patch.object(sync, "import_artifact", return_value={"imported": True}), patch.object(sync, "publish_dataset", return_value={"summary": {}, "release_url": "url"}) as publish:
            result = sync.synchronize(self.api, self.root, maximum=1)
        self.assertEqual(self.api.download.call_args.args[0], "/actions/artifacts/1/zip")
        self.assertEqual(result["pending_artifacts"], 1)
        self.assertEqual(result["pending_untried"], 0)
        self.assertIn("1", publish.call_args.args[5])
        self.assertEqual(result["import_errors"][0]["artifact_id"], 2)
        self.assertEqual(result["batch_errors"], [])
        connection.close.assert_called_once()

    def test_all_failed_imports_persist_errors_without_reuploading_existing_database(self):
        catalog = {"processed": {}, "assets": {"database": {"id": 99}}}
        connection = Mock()
        self.api.download.side_effect = RuntimeError("bad download")
        with patch.object(sync, "read_catalog", return_value=(catalog, "sha")), patch.object(sync, "list_artifacts", return_value=[artifact(1)]), patch.object(sync, "restore_database", return_value=(connection, self.root / "data.sqlite")), patch.object(sync, "publish_dataset") as publish, patch.object(sync, "write_catalog") as write:
            result = sync.synchronize(self.api, self.root)
        self.assertFalse(result["changed"])
        self.assertEqual(result["pending_untried"], 0)
        self.assertEqual(len(result["batch_errors"]), 1)
        publish.assert_not_called()
        saved = write.call_args.args[1]
        self.assertEqual(saved["assets"], catalog["assets"])
        self.assertEqual(saved["import_errors"][0]["artifact_id"], 1)
        self.assertNotIn("1", saved["processed"])

    def test_initial_all_failed_batch_can_publish_empty_catalog_and_advance_backfill(self):
        connection = Mock()
        self.api.download.side_effect = RuntimeError("bad download")
        with patch.object(sync, "read_catalog", return_value=(None, None)), patch.object(sync, "list_artifacts", return_value=[artifact(2), artifact(1)]), patch.object(sync, "restore_database", return_value=(connection, self.root / "data.sqlite")), patch.object(sync, "publish_dataset", return_value={"summary": {}, "release_url": "url"}) as publish:
            result = sync.synchronize(self.api, self.root, maximum=1)
        self.assertTrue(result["changed"])
        self.assertEqual(result["pending_untried"], 1)
        self.assertEqual(publish.call_args.args[5], {})




class LargeHistoricalArtifactTests(unittest.TestCase):
    def test_large_spectrum_requires_checksum_identity_disk_space_and_exact_bound(self):
        size = sync.MAX_DOWNLOAD + 100
        entry = artifact(123, 'spectrum-'+'a'*24+'-1', size_in_bytes=size)
        api = Mock()
        with tempfile.TemporaryDirectory() as temporary, patch.object(sync.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)):
            destination = Path(temporary)/'incoming.zip'
            sync.download_artifact(api, entry, destination, 'f'*64)
            api.download.assert_called_once_with('/actions/artifacts/123/zip', destination, 'f'*64, maximum=size)
            for changed, digest in ((dict(entry, name='results-ubuntu-latest'), 'f'*64),
                                    (dict(entry, size_in_bytes=sync.MAX_SPECTRUM_DOWNLOAD+1), 'f'*64),
                                    (entry, None)):
                with self.assertRaises(ValueError):
                    sync.download_artifact(api, changed, destination, digest)
        with patch.object(sync.shutil, 'disk_usage', return_value=Mock(free=size)):
            with self.assertRaisesRegex(ValueError, 'disk space'):
                sync.download_artifact(api, entry, Path('incoming.zip'), 'f'*64)

    def test_one_large_archive_per_batch_preserves_remaining_work(self):
        size=sync.MAX_DOWNLOAD+100
        candidates=[artifact(i,'spectrum-'+str(i)*24+'-1',size_in_bytes=size,digest='sha256:'+'f'*64) for i in (1,2)]
        api=Mock(repository='owner/repository')
        api.download.return_value='f'*64
        with tempfile.TemporaryDirectory() as temporary, \
             patch.object(sync, 'read_catalog', return_value=({'processed':{}}, 'sha')), \
             patch.object(sync, 'list_artifacts', return_value=candidates), \
             patch.object(sync, 'restore_database', return_value=(Mock(), Path(temporary)/'data.sqlite')), \
             patch.object(sync.shutil, 'disk_usage', return_value=Mock(free=10*1024**3)), \
             patch.object(sync, 'import_artifact', return_value={'imported':True}), \
             patch.object(sync, 'publish_dataset', return_value={'summary':{},'release_url':'url'}) as publish:
            result=sync.synchronize(api,Path(temporary),maximum=8)
        self.assertEqual(api.download.call_count,1)
        self.assertEqual(result['pending_artifacts'],1)
        self.assertEqual(result['pending_untried'],1)
        self.assertEqual(len(publish.call_args.args[5]),1)

if __name__ == "__main__":
    unittest.main()

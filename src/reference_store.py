"""Persist qualified numerical references on a Git branch and join timing artifacts."""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import sys
import zipfile

from benchmark import ROOT, atomic_json, export, load_references, read_json, utc_now
from publish_progress import APIError, GitHub, PLATFORMS, comparison_rows, select_artifact
from references import build_requests, cached_reference, export_references, valid_cached

BRANCH = "reference-data"
MAX_RECORD_BYTES = 2 * 1024 * 1024


def branch_tree(api, branch=BRANCH):
    if branch not in (BRANCH, "local-reference-data"):
        raise ValueError("Unsupported numerical reference branch")
    try:
        head = api.request("GET", f"/git/ref/heads/{branch}")["object"]["sha"]
    except APIError as error:
        if error.status == 404:
            return None, None, {}
        raise
    commit = api.request("GET", f"/git/commits/{head}")
    tree_sha = commit["tree"]["sha"]
    tree = api.request("GET", f"/git/trees/{tree_sha}?recursive=1")
    if tree.get("truncated"):
        raise ValueError("Reference tree exceeds GitHub's recursive listing limit")
    return head, tree_sha, {entry["path"]: entry["sha"] for entry in tree["tree"]
                            if entry["type"] == "blob"}


def blob_record(api, sha):
    blob = api.request("GET", f"/git/blobs/{sha}")
    if blob.get("encoding") != "base64" or blob.get("size", 0) > MAX_RECORD_BYTES:
        raise ValueError("Unsupported reference blob")
    raw = base64.b64decode("".join(blob["content"].split()), validate=True)
    if len(raw) > MAX_RECORD_BYTES:
        raise ValueError("Oversized reference blob")
    actual = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
    if actual != sha:
        raise ValueError("Reference blob content does not match Git SHA")
    return json.loads(raw)


def restore_cache(api, jobs, cache_dir):
    """Fetch only requested keys. Invalid evidence is a miss, network errors are failures."""
    head, _, tree = branch_tree(api)
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    wanted = {job["reference_id"]: job for job in jobs
              if cached_reference(cache_dir / f"{job['reference_id']}.json", job) is None
              and f"references/{job['reference_id']}.json" in tree}
    # One archive download avoids thousands of API requests for a full dense grid.
    if len(wanted) > 8:
        with zipfile.ZipFile(io.BytesIO(api.repository_archive(head))) as archive:
            for entry in archive.infolist():
                parts = PurePosixPath(entry.filename).parts
                if len(parts) != 3 or parts[1] != "references" or not parts[2].endswith(".json"):
                    continue
                identity = parts[2][:-5]
                if identity not in wanted or entry.file_size > MAX_RECORD_BYTES:
                    continue
                raw = archive.read(entry)
                sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
                if tree.get(f"references/{identity}.json") != sha:
                    continue
                try:
                    record = json.loads(raw)
                    if valid_cached(record, wanted[identity]):
                        atomic_json(cache_dir / f"{identity}.json", record)
                except ValueError:
                    pass
        restored = sum(cached_reference(cache_dir / f"{job['reference_id']}.json", job) is not None
                       for job in jobs)
        return dict(planned=len(jobs), cached=restored, missing=len(jobs) - restored)

    def restore(job):
        path = cache_dir / f"{job['reference_id']}.json"
        if cached_reference(path, job) is not None:
            return True
        sha = tree.get(f"references/{job['reference_id']}.json")
        if sha is None:
            return False
        try:
            record = blob_record(api, sha)
        except (ValueError, KeyError, TypeError):
            return False
        if not valid_cached(record, job):
            return False
        atomic_json(path, record)
        return True

    with ThreadPoolExecutor(max_workers=4) as pool:
        restored = sum(pool.map(restore, jobs))
    return dict(planned=len(jobs), cached=restored, missing=len(jobs) - restored)


def publish_cache(api, jobs, cache_dir, *, branch=BRANCH):
    """Commit new successful keys atomically, retaining concurrent writers' results."""
    records = {}
    by_id = {job["reference_id"]: job for job in jobs}
    for job in jobs:
        record = cached_reference(Path(cache_dir) / f"{job['reference_id']}.json", job)
        if record is not None:
            records[f"references/{job['reference_id']}.json"] = record
    if not records:
        return 0
    for _ in range(5):
        head, tree_sha, tree = branch_tree(api, branch)
        changes = []
        for path, record in records.items():
            serialized = json.dumps(record, allow_nan=False, separators=(",", ":"))
            raw = serialized.encode()
            sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
            if path in tree:
                if tree[path] == sha:
                    continue
                try:
                    previous = blob_record(api, tree[path])
                    if valid_cached(previous, by_id[record["reference_id"]]):
                        continue
                except (ValueError, KeyError, TypeError):
                    pass
            changes.append(dict(path=path, mode="100644", type="blob",
                                content=serialized))
        if not changes:
            return 0
        body = dict(tree=changes)
        if tree_sha:
            body["base_tree"] = tree_sha
        new_tree = api.request("POST", "/git/trees", body)
        commit = api.request("POST", "/git/commits", dict(
            message=f"Save {len(changes)} qualified numerical references",
            tree=new_tree["sha"], parents=[head] if head else []))
        try:
            if head:
                api.request("PATCH", f"/git/refs/heads/{branch}",
                            dict(sha=commit["sha"], force=False))
            else:
                api.request("POST", "/git/refs", dict(ref=f"refs/heads/{branch}", sha=commit["sha"]))
            return len(changes)
        except APIError as error:
            if error.status not in (409, 422):
                raise
    raise RuntimeError("Reference branch changed concurrently too often. Retry publication")


def join_directory(directory, references_path):
    """Refresh derived comparisons without running or modifying any timing measurements."""
    directory = Path(directory)
    rows = read_json(directory / "results.json")
    references = load_references(references_path)
    status = read_json(directory / "status.json")
    # Remove old derived fields before rejoining so missing references remain missing.
    cases = [{key: value for key, value in row.items()
              if not key.startswith("reference_") and key != "delta_ts_db"} for row in rows]
    records = {row["case_id"]: row for row in cases if row["status"] != "pending"}
    export(directory, cases, records, references, status["state"])
    atomic_json(directory / "references.json", read_json(references_path))
    return sum(row["status"] == "ok" and row["physical_id"] in references
               for row in comparison_rows(directory, rows, references=references))


def download_timing(api, run_id, platform, directory):
    artifacts = []
    for page in range(1, 101):
        part = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(part)
        if len(part) < 100:
            break
    artifact = select_artifact(artifacts, platform)
    if artifact is None:
        return False
    from campaign import setup_only_checkpoint
    content = api.archive(artifact["id"])
    if setup_only_checkpoint(content, api.repository, run_id) is not None:
        print(f"No timing measurements for {platform}: setup did not produce a checkpoint", flush=True)
        return False
    from refresh import baseline_settings
    if baseline_settings(content, api.repository, run_id, platform) is not None:
        print(f"No new timing measurements for {platform}: refresh setup did not produce a checkpoint", flush=True)
        return False
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    values = {}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for entry in archive.infolist():
            name = PurePosixPath(entry.filename).name
            if name not in ("manifest.json", "results.json", "status.json", "retained-results.json"):
                continue
            if name in values or entry.file_size > 128 * 1024 * 1024:
                raise ValueError("Ambiguous or oversized timing artifact")
            values[name] = json.loads(archive.read(entry))
    if not {"manifest.json", "results.json", "status.json"} <= values.keys():
        raise ValueError("Incomplete timing artifact")
    if "retained-results.json" in values:
        from publish_progress import retained_comparison_rows
        retained_comparison_rows(values["retained-results.json"], values["results.json"])
    for name, value in values.items():
        atomic_json(directory / name, value)
    if "retained-results.json" not in values:
        (directory / "retained-results.json").unlink(missing_ok=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("restore", "publish", "join", "attach"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--config", default=ROOT / "benchmark.json")
    parser.add_argument("--sources-lock", default=ROOT / "sources.lock.json")
    parser.add_argument("--profile", default="pilot")
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    jobs = build_requests(read_json(args.config), read_json(args.sources_lock), args.profile)
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("Invalid reference shard")
    jobs = [job for job in jobs if int(job["physical_id"], 16) % args.shard_count == args.shard_index]
    api = None if args.action == "join" else GitHub(args.repository, os.environ.get("GITHUB_TOKEN"), 300)
    if args.action == "attach":
        if not args.run_id or not args.platform or not args.directory:
            parser.error("attach requires --run-id, --platform and --directory")
        if not download_timing(api, args.run_id, args.platform, args.directory):
            print(f"No timing artifact for {args.platform}")
            return
        manifest = read_json(args.directory / "manifest.json")
        jobs = manifest.get("reference_requests") or build_requests(
            manifest["configuration"], manifest["sources"], manifest["profile"])
    if args.action in ("restore", "attach"):
        status = restore_cache(api, jobs, args.cache_dir)
        print(json.dumps(status), flush=True)
        if args.action == "restore" and os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
                stream.write(f"missing={status['missing']}\n")
    if args.action == "publish":
        print(f"Persisted {publish_cache(api, jobs, args.cache_dir)} new references", flush=True)
    export_references(jobs, args.cache_dir, args.output)
    curve_run = args.run_id if args.action == "attach" else os.getenv("REFERENCE_TIMING_RUN_ID", os.getenv("GITHUB_RUN_ID"))
    if api is not None and curve_run:
        from reference_curves import curves_from_cache, publish_curves
        try:
            published = publish_curves(api, int(curve_run), curves_from_cache(jobs, args.cache_dir))
            print(f"Published {published} additional TS reference points", flush=True)
        except (OSError, RuntimeError, ValueError) as error:
            # Reference publication must not prevent cached values from being used.
            print(f"TS reference curve publication deferred ({type(error).__name__})", file=sys.stderr)
    if args.action in ("join", "attach"):
        if not args.directory:
            parser.error("join requires --directory")
        compared = join_directory(args.directory, args.output)
        print(f"Joined {compared} successful timing observations", flush=True)
    if args.action == "attach":
        from publish_progress import compact_payload, publish
        run = api.request("GET", f"/actions/runs/{args.run_id}")
        payload = compact_payload(args.repository, args.platform, args.run_id, run["run_attempt"],
            comparison_rows(args.directory, read_json(args.directory / "results.json")),
            read_json(args.directory / "status.json"), "completed" if run.get("status") == "completed" else "running")
        comparison_run = os.getenv("GITHUB_RUN_ID")
        if comparison_run:
            payload["artifacts_url"] = f"https://github.com/{args.repository}/actions/runs/{comparison_run}#artifacts"
        publish(api, payload)


"""Pending-only queue additions for the existing reference_store runner."""

class ReferenceGitTransport:
    """Use the existing checkout for reference data, outside the REST request quota."""
    def __init__(self, root):
        import threading
        from parallel_campaign import GitDataStore
        class ReferenceDataStore(GitDataStore):
            def git(self, *arguments, **kwargs):
                import subprocess
                try:
                    return super().git(*arguments, **kwargs)
                except subprocess.TimeoutExpired as error:
                    raise APIError(503, 'Reference Git transport timed out') from error
        self.store = ReferenceDataStore(root)
        self.lock = threading.RLock()
        self.heads = {}
        self.fetched = {}
        self.store.git('config', 'remote.origin.promisor', 'true')
        self.store.git('config', 'remote.origin.partialclonefilter', 'blob:none')

    def handles(self, method, path, body=None):
        branches = ('benchmark-data', 'reference-data', 'local-reference-data')
        if path.startswith('/contents/'):
            branch = (body or {}).get('branch') if method == 'PUT' else path.partition('?ref=')[2]
            return branch in branches
        if path.startswith(('/git/blobs', '/git/trees', '/git/commits')):
            return True
        return any(path in ('/git/ref/heads/' + b, '/git/refs/heads/' + b) for b in branches)

    def fetch(self, branch, force=False):
        import time
        with self.lock:
            if not force and branch in self.heads and time.monotonic() - self.fetched.get(branch, 0) < 15:
                return self.heads[branch]
            ref = 'refs/remotes/origin/' + branch
            self.store.git('fetch', '--quiet', '--no-tags', '--depth=1', '--filter=blob:none',
                'origin', f'+refs/heads/{branch}:{ref}')
            head = self.store.git('rev-parse', ref).stdout.decode().strip()
            self.heads[branch], self.fetched[branch] = head, time.monotonic()
            return head

    def prefetch(self, shas):
        # Fetch requested checkpoint blobs together without downloading the reference archive.
        if not shas:
            return
        env = dict(os.environ, GIT_NO_LAZY_FETCH='1')
        raw = self.store.git('cat-file', '--batch-check=%(objectname) %(objecttype)',
            data=('\n'.join(shas)+'\n').encode(), env=env).stdout.decode().splitlines()
        missing = [line.split()[0] for line in raw if line.endswith(' missing')]
        for start in range(0, len(missing), 128):
            self.store.git('-c','fetch.negotiationAlgorithm=noop','fetch','--quiet','--no-tags',
                '--no-write-fetch-head','--recurse-submodules=no','--filter=blob:none','--stdin','origin',
                data=('\n'.join(missing[start:start+128])+'\n').encode())

    def tree(self, tree, recursive=False):
        args = ['ls-tree', '-z'] + (['-r'] if recursive else []) + [tree]
        rows = []
        for item in self.store.git(*args).stdout.split(b'\0'):
            if item:
                meta, name = item.split(b'\t', 1)
                mode, kind, sha = meta.decode().split()
                rows.append(dict(path=name.decode(), mode=mode, type=kind, sha=sha))
        return rows

    def create_tree(self, body):
        import tempfile
        import re
        with tempfile.TemporaryDirectory(prefix='reference-index-') as directory:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(directory) / 'index'))
            self.store.git('read-tree', body.get('base_tree', '--empty'), env=env)
            rows = []
            for item in body['tree']:
                path = item['path']
                if not re.fullmatch(r'[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*', path) or any(p in ('.','..') for p in path.split('/')):
                    raise ValueError('Invalid reference data path')
                sha = item.get('sha')
                if sha is None:
                    sha = self.store.git('hash-object', '-w', '--stdin', data=item['content'].encode()).stdout.decode().strip()
                rows.append(f'100644 blob {sha}\t{path}\n')
            self.store.git('update-index', '--index-info', data=''.join(rows).encode(), env=env)
            return self.store.git('write-tree', env=env).stdout.decode().strip()

    def commit(self, body):
        command = ['-c','user.name=acousticTSbenchmark','-c','user.email=benchmark@users.noreply.github.com',
                   'commit-tree',body['tree']]
        for parent in body.get('parents', []):
            command.extend(['-p',parent])
        return self.store.git(*command, data=(body['message']+'\n').encode()).stdout.decode().strip()

    def push(self, branch, sha):
        import time
        result = self.store.git('push', '--quiet', 'origin', f'{sha}:refs/heads/{branch}', check=False)
        if result.returncode:
            self.fetched[branch] = 0
            conflict = b'rejected' in result.stderr or b'failed to update ref' in result.stderr
            raise APIError(409 if conflict else 503, 'Reference Git publication conflict' if conflict else 'Reference Git transport unavailable')
        self.heads[branch], self.fetched[branch] = sha, time.monotonic()

    def put(self, path, body):
        import time
        import random
        branch = body['branch']
        raw = base64.b64decode(body['content'])
        proposed = self.store.git('hash-object', '-w', '--stdin', data=raw).stdout.decode().strip()
        for attempt in range(24):
            head = self.fetch(branch, force=True)
            entries = {item['path']:item['sha'] for item in self.tree(head, recursive='/' in path)}
            actual = entries.get(path)
            if actual == proposed:
                return dict(content=dict(sha=proposed))
            if actual != body.get('sha'):
                raise APIError(409, 'Reference document changed concurrently')
            tree = self.create_tree(dict(base_tree=head, tree=[dict(path=path, sha=proposed)]))
            commit = self.commit(dict(tree=tree, parents=[head], message=body['message']))
            try:
                self.push(branch, commit)
                return dict(content=dict(sha=proposed))
            except APIError as error:
                if error.status != 409 or attempt == 23:
                    raise
                time.sleep(random.uniform(.3, min(4., 1 + attempt / 4)))
        raise RuntimeError('Reference Git checkpoint remains pending')

    def request(self, method, path, body=None):
        if path.startswith('/contents/'):
            name = path[len('/contents/'):].split('?')[0]
            if method == 'PUT':
                return self.put(name, body)
            head = self.fetch(path.partition('?ref=')[2])
            entry = self.store.git('ls-tree', '-z', head, '--', name).stdout
            if not entry:
                raise APIError(404, 'Missing reference document')
            return self.store.blob(entry.split(b'\t', 1)[0].split()[2].decode())
        if method == 'GET' and path.startswith('/git/ref/heads/'):
            return dict(object=dict(sha=self.fetch(path.rsplit('/',1)[1], force=True)))
        if method == 'PATCH' and path.startswith('/git/refs/heads/'):
            if body.get('force'):
                raise ValueError('Forced reference publication is prohibited')
            self.push(path.rsplit('/',1)[1], body['sha'])
            return dict(object=dict(sha=body['sha']))
        if method == 'GET' and path.startswith('/git/blobs/'):
            return self.store.blob(path.rsplit('/',1)[1])
        if method == 'POST' and path == '/git/blobs':
            return dict(sha=self.store.git('hash-object','-w','--stdin',data=base64.b64decode(body['content'])).stdout.decode().strip())
        if method == 'GET' and path.startswith('/git/commits/'):
            raw = self.store.git('cat-file','commit',path.rsplit('/',1)[1]).stdout
            return dict(tree=dict(sha=raw.splitlines()[0].split()[1].decode()))
        if method == 'GET' and path.startswith('/git/trees/'):
            sha = path.rsplit('/',1)[1].split('?')[0]
            return dict(tree=self.tree(sha, recursive='recursive=1' in path),truncated=False)
        if method == 'POST' and path == '/git/trees':
            return dict(sha=self.create_tree(body))
        if method == 'POST' and path == '/git/commits':
            return dict(sha=self.commit(body))
        raise ValueError('Unsupported reference Git operation')


def reference_api(repository, token, root):
    import time
    from parallel_campaign import LiveGitHub
    class ReferenceAPI(LiveGitHub):
        def __init__(self):
            super().__init__(repository, token, 300)
            self.wait_on_rate_limit = False
            self.reference_git = ReferenceGitTransport(root)
            self.job_cache = {}
        def request(self, method, path, body=None):
            if self.reference_git.handles(method, path, body):
                return self.reference_git.request(method, path, body)
            cached = self.job_cache.get(path)
            if method == 'GET' and '/jobs?' in path and cached and time.monotonic()-cached[0] < 180:
                return cached[1]
            for attempt in range(5):
                try:
                    result = super().request(method, path, body)
                    if method == 'GET' and '/jobs?' in path:
                        self.job_cache[path] = (time.monotonic(), result)
                    return result
                except APIError as error:
                    if error.status not in (500, 502, 503, 504) or attempt == 4:
                        raise
                    time.sleep(min(20, 2**attempt))
    return ReferenceAPI()

def queue_path(queue_id, kind, task=None):
    import re
    if not re.fullmatch(r'[a-zA-Z0-9-]{1,64}', queue_id):
        raise ValueError('Invalid reference queue ID')
    if kind not in ('plan', 'state', 'progress', 'task'):
        raise ValueError('Invalid reference queue document')
    if kind == 'task' and not re.fullmatch(r'[a-f0-9]{24}', task or ''):
        raise ValueError('Invalid reference task ID')
    return f'reference-queue-{queue_id}-{kind}' + (f'-{task}' if task else '') + '.json'


def queue_read(api, path):
    from parallel_campaign import get_document
    return get_document(api, path)


def queue_write(api, path, value, expected):
    """Compare-and-swap one compressed document without listing the whole branch."""
    import random
    import time
    from parallel_campaign import pack, DocumentConflict
    raw = json.dumps(pack(value), separators=(',', ':')).encode()
    body = dict(message=f'Checkpoint {path}', branch='benchmark-data',
                content=base64.b64encode(raw).decode())
    if expected is not None:
        body['sha'] = expected
    for attempt in range(16):
        try:
            return api.request('PUT', '/contents/' + path, body)['content']['sha']
        except APIError as error:
            if error.status not in (409, 422, 500, 502, 503, 504):
                raise
            previous, sha = queue_read(api, path)
            if previous == value:
                return sha
            if sha != expected:
                raise DocumentConflict('Reference queue changed concurrently') from error
            time.sleep(min(8, .25 * 2 ** min(attempt, 5)) + random.random())
    raise RuntimeError('Reference checkpoint remains pending after publication retries')


def queue_change(api, queue_id, change):
    import random
    import time
    from parallel_campaign import DocumentConflict
    path = queue_path(queue_id, 'state')
    for attempt in range(64):
        state, sha = queue_read(api, path)
        if state is None:
            raise ValueError('Reference queue is missing')
        original = json.dumps(state, sort_keys=True)
        result = change(state)
        if json.dumps(state, sort_keys=True) == original:
            return result
        try:
            queue_write(api, path, state, sha)
            return result
        except DocumentConflict:
            time.sleep(random.uniform(.2, min(5, 1 + attempt / 4)))
    raise RuntimeError('Reference queue remains busy')


def queue_claim(api, queue_id, owner, previous=None):
    def change(state):
        run, slot = owner.split(':')
        owns_slot = (state.get('slots', {}).get(slot, {}).get('run') == run
                     if state.get('scheduler_version') == 2 else state['active_run'] == run)
        if state['status'] != 'running' or not owns_slot:
            return None
        if previous is not None:
            if state['claims'].get(previous) != owner:
                raise ValueError('Reference claim ownership changed')
            del state['claims'][previous]
            if previous not in state['finished']:
                state['finished'].append(previous)
        current = next((task for task, worker in state['claims'].items() if worker == owner), None)
        if current:
            return current
        if not state['pending']:
            return None
        task = state['pending'].pop(0)
        state['claims'][task] = owner
        return task
    return queue_change(api, queue_id, change)


def queue_plan(api, queue_id):
    from benchmark import fingerprint, physical_identity
    plan, _ = queue_read(api, queue_path(queue_id, 'plan'))
    if not plan or plan['queue_id'] != queue_id or plan['threads'] != 4:
        raise ValueError('Missing or invalid reference transfer manifest')
    identities = set()
    task_ids = set()
    for task in plan['tasks']:
        if (task['id'] in task_ids or not 1 <= len(task['jobs']) <= 8 or
                (type(task['case_timeout_seconds']) is not int or
                 not 600 <= task['case_timeout_seconds'] <= 5400)):
            raise ValueError('Invalid reference chunk')
        task_ids.add(task['id'])
        for job in task['jobs']:
            if (job['reference_id'] != fingerprint(job['signature']) or
                    job['physical_id'] != physical_identity(job) or
                    job['physical_id'] != physical_identity(job['signature']['physical']) or
                    job['source_sha'] != plan['source_sha'] or
                    job['signature']['source_sha'] != job['source_sha'] or
                    job['signature']['policy'] != job['policy'] or
                    job['signature']['method'] != job['method'] or
                    job['signature']['controls'] != job['controls'] or
                    job['controls']['mode_threads'] != plan['threads'] or
                    job['reference_id'] in identities):
                raise ValueError('Invalid or duplicate transferred reference request')
            identities.add(job['reference_id'])
    return plan


def queue_initialize_legacy(api, args):
    plan = queue_plan(api, args.queue_id)
    run = str(os.environ['GITHUB_RUN_ID'])
    current, _ = queue_read(api, queue_path(args.queue_id, 'state'))
    if current.get('active_run') and current['active_run'] != run:
        old = api.request('GET', '/actions/runs/' + current['active_run'])
        if old['status'] != 'completed':
            raise ValueError('A previous reference allocation is still active')
        queue_recover(api, plan, current['active_run'])
    def start(state):
        previous = state.get('active_run')
        if previous and previous != run:
            old = api.request('GET', '/actions/runs/' + previous)
            if old['status'] != 'completed':
                raise ValueError('A previous reference allocation is still active')
        if previous != run:
            # Interrupted tasks retain per-frequency evidence in their checkpoints.
            state['pending'] = list(state['claims']) + state['pending']
            state['claims'] = {}
            state['finished_before_run'] = len(state['finished'])
        state.update(status='running', active_run=run)
        return len(state['pending'])
    pending = queue_change(api, args.queue_id, start)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('workers=' + json.dumps(list(range(min(16, pending)))) + '\n')
    print(f'REFERENCE_QUEUE pending_chunks={pending} threads={plan["threads"]}', flush=True)


def queue_reconcile(api, queue_id, plan):
    """Release only slots whose numerical jobs have finished, regardless of sibling jobs."""
    import re
    state, _ = queue_read(api, queue_path(queue_id, 'state'))
    for run, allocation in state.get('allocations', {}).items():
        if allocation.get('status') == 'completed':
            continue
        jobs = api.request('GET', f'/actions/runs/{run}/jobs?per_page=100')['jobs']
        workers = {int(match.group(1)):job for job in jobs
                   if (match := re.fullmatch(r'references worker (\d+)', job['name']))}
        completed = {slot:workers[slot] for slot in allocation['slots']
                     if slot in workers and workers[slot]['status'] == 'completed'}
        # Failed jobs may have an uploaded result that missed the online checkpoint.
        failed = {slot for slot, job in completed.items() if job['conclusion'] != 'success'
                  and state['slots'].get(str(slot), {}).get('run') == run}
        if failed:
            queue_recover(api, plan, run, workers=failed)
        if completed:
            def release(current):
                for slot in completed:
                    owned = current['slots'].get(str(slot), {})
                    if owned.get('run') != run:
                        continue
                    owner = f'{run}:{slot}'
                    for task in [task for task, claimant in current['claims'].items() if claimant == owner]:
                        del current['claims'][task]
                        if task not in current['pending'] and task not in current['finished']:
                            current['pending'].insert(0, task)
                    del current['slots'][str(slot)]
                if set(completed) == set(allocation['slots']):
                    current['allocations'][run]['status'] = 'completed'
            queue_change(api, queue_id, release)
        if (run == state.get('legacy_run') and workers and
                all(job['status'] == 'completed' for job in workers.values())):
            legacy = api.request('GET', '/actions/runs/' + run)
            if legacy['status'] != 'completed':
                # Stop only the obsolete publisher/finalizer after every numerical job ends.
                api.request('POST', f'/actions/runs/{run}/cancel')
                print('REFERENCE_LEGACY_CONTROLLER_STOP_REQUESTED ' + run, flush=True)


def queue_initialize(api, args):
    plan = queue_plan(api, args.queue_id)
    run = str(os.environ['GITHUB_RUN_ID'])
    role = os.getenv('QUEUE_ROLE', 'pool')
    dispatch = os.getenv('ALLOCATION_ID', '')
    def upgrade(state):
        if state.get('scheduler_version') == 2:
            return
        old = state.get('active_run')
        state.update(scheduler_version=2, slots={}, allocations={}, dispatches={},
                     legacy_run=old, status='running')
        if old:
            state['allocations'][old] = dict(slots=list(range(16)), status='active')
            state['slots'] = {str(slot):dict(run=old) for slot in range(16)}
    queue_change(api, args.queue_id, upgrade)
    if role != 'workers':
        try:
            queue_reconcile(api, args.queue_id, plan)
        except (APIError, OSError, RuntimeError) as error:
            print(f'REFERENCE_INITIAL_RECONCILIATION_DEFERRED error={type(error).__name__}', flush=True)
        pointer = dict(schema_version=1, queue_id=args.queue_id)
        previous, sha = queue_read(api, 'reference-queue-active.json')
        if previous != pointer:
            queue_write(api, 'reference-queue-active.json', pointer, sha)
    def allocate(state):
        state['active_run'] = run
        if state['status'] not in ('running', 'continuation_pending', 'ready'):
            return []
        state['status'] = 'running'
        if role == 'controller':
            return []
        if run in state['allocations']:
            return [slot for slot in state['allocations'][run]['slots']
                    if state['slots'].get(str(slot), {}).get('run') == run]
        if role == 'workers':
            requested = state['dispatches'].get(dispatch)
            if requested is None:
                raise ValueError('Missing reference allocation reservation')
            if requested.get('run') and requested['run'] != run:
                return []
            slots = requested['slots']
            if any(state['slots'].get(str(slot), {}).get('dispatch') != dispatch and
                   state['slots'].get(str(slot), {}).get('run') != run for slot in slots):
                raise ValueError('Reference worker slot reservation changed')
            requested.update(run=run, status='started')
        else:
            slots = [slot for slot in range(16) if str(slot) not in state['slots']][:len(state['pending'])]
        for slot in slots:
            state['slots'][str(slot)] = dict(run=run)
        state['allocations'][run] = dict(slots=slots, status='active')
        return slots
    slots = queue_change(api, args.queue_id, allocate)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('workers=' + json.dumps(slots) + '\n')
    print(f'REFERENCE_ALLOCATION run={run} role={role} slots={slots}', flush=True)


def queue_refill(api, args, plan):
    import time
    import uuid
    queue_reconcile(api, args.queue_id, plan)
    def reserve(state):
        if state['status'] != 'running':
            return []
        free = [slot for slot in range(16) if str(slot) not in state['slots']][:len(state['pending'])]
        if free:
            identity = uuid.uuid4().hex
            state['dispatches'][identity] = dict(slots=free, status='requested', last_attempt=0)
            for slot in free:
                state['slots'][str(slot)] = dict(dispatch=identity)
        eligible = []
        for identity, request in state['dispatches'].items():
            if request['status'] == 'requested' and time.time() - request['last_attempt'] >= 180:
                request['last_attempt'] = time.time()
                eligible.append(identity)
        return eligible
    for identity in queue_change(api, args.queue_id, reserve):
        state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
        api.request('POST', '/actions/workflows/references.yml/dispatches', dict(
            ref=state.get('controller_ref', os.environ['GITHUB_REF_NAME']), inputs=dict(queue_id=args.queue_id,
                budget_minutes=str(int(args.budget_seconds / 60)), profile='full',
                queue_role='workers', allocation_id=identity)))
        print('REFERENCE_REPLACEMENT_DISPATCHED ' + identity, flush=True)


def queue_begin_controller(api, args):
    """Record ownership only after the serialized publisher job actually starts."""
    run = str(os.environ['GITHUB_RUN_ID'])
    queue_change(api, args.queue_id, lambda state: state.update(
        controller_run=run, controller_started_at=utc_now()))
    print('REFERENCE_CONTROLLER_STARTED ' + run, flush=True)


def queue_next_controller(api, args):
    run = str(os.environ['GITHUB_RUN_ID'])
    def reserve(state):
        if state['status'] != 'running' or not (state['pending'] or state['claims']):
            return False
        if state.get('controller_run') != run:
            return False
        successors = state.setdefault('controller_successors', {})
        if run in successors or state.get('controller_successor_for') == run:
            return False
        state['controller_successor_for'] = run
        successors[run] = dict(requested_at=utc_now())
        return True
    if queue_change(api, args.queue_id, reserve):
        try:
            state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
            api.request('POST', '/actions/workflows/references.yml/dispatches', dict(
                ref=state.get('controller_ref', os.environ['GITHUB_REF_NAME']), inputs=dict(queue_id=args.queue_id,
                    budget_minutes=str(int(args.budget_seconds / 60)), profile='full', queue_role='controller')))
        except BaseException:
            def release(state):
                state.get('controller_successors', {}).pop(run, None)
                if state.get('controller_successor_for') == run:
                    state.pop('controller_successor_for', None)
            queue_change(api, args.queue_id, release)
            raise
        print('REFERENCE_CONTROLLER_CONTINUATION_DISPATCHED', flush=True)


def queue_settings(api, args):
    plan = queue_plan(api, args.queue_id)
    atomic_json(args.output, plan['sources_lock'])
    from references import source_digest
    for name, expected in plan['adapter_hashes'].items():
        if source_digest(ROOT / 'src' / name) != expected:
            raise ValueError(f'Transferred reference adapter changed: {name}')


def queue_packet(api, queue_id, task):
    packet, sha = queue_read(api, queue_path(queue_id, 'task', task['id']))
    packet = packet or dict(task_id=task['id'], records={}, skipped=[], active=None)
    allowed = {job['reference_id']: job for job in task['jobs']}
    if (packet['task_id'] != task['id'] or not set(packet['records']) <= set(allowed) or
            not set(packet['skipped']) <= set(allowed)):
        raise ValueError('Reference checkpoint belongs to another task')
    for identity, record in packet['records'].items():
        if record.get('qualified') and not valid_cached(record, allowed[identity]):
            raise ValueError('Invalid qualified reference checkpoint')
    return packet, sha


def queue_shared(api):
    from parallel_campaign import _document_bytes
    from reference_curves import _validated
    raw, _ = _document_bytes(api, 'qualified-reference-curves.json')
    return _validated(json.loads(raw)['reference_curves']) if raw else {}


def queue_timing_line(active, worker, now=None):
    import time
    now = time.time() if now is None else now
    elapsed = max(0., now - active['started_epoch'])
    clock = f'{int(elapsed)//3600:02d}:{int(elapsed)//60%60:02d}:{int(elapsed)%60:02d}'
    return (f'REFERENCE_RUNNING worker={worker} a={active["semimajor_m"]} m '
            f'ar={active["aspect_ratio"]} boundary={active["boundary"]} '
            f'angle={active["incidence_deg"]} deg f={active["frequency_hz"]/1000:.2f} kHz '
            f'phase={active["phase"]} elapsed_seconds={elapsed:.2f} elapsed={clock}')


def queue_heartbeat(packet, worker):
    import threading
    from contextlib import contextmanager
    @contextmanager
    def run():
        stop = threading.Event()
        def report():
            while not stop.wait(30):
                active = packet.get('active')
                if active:
                    print(queue_timing_line(active, worker), flush=True)
        thread = threading.Thread(target=report, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=1)
    return run()


def queue_worker(api, args):
    import time
    from references import _run_batch, reference_record, julia_provenance
    plan = queue_plan(api, args.queue_id)
    runtime = read_json(args.runtime)
    if (runtime['source_shas']['acousticscattering'] != plan['source_sha'] or
            runtime['availability']['acousticscattering']['status'] != 'ready'):
        raise ValueError('Prepared reference runtime differs from the transferred queue')
    tasks = {task['id']: task for task in plan['tasks']}
    owner = f'{os.environ["GITHUB_RUN_ID"]}:{args.worker}'
    deadline = time.time() + args.budget_seconds
    provenance = dict(source_sha=plan['source_sha'], threads=4, blas_threads=1,
        environment_fingerprint=runtime.get('environment_fingerprint'),
        environment_compatibility=julia_provenance(runtime),
        github_run_id=os.environ['GITHUB_RUN_ID'], reference_queue=args.queue_id)
    args.directory.mkdir(parents=True, exist_ok=True)
    shared = queue_shared(api)
    task_id = queue_claim(api, args.queue_id, owner)
    previous = None
    while task_id:
        task = tasks[task_id]
        timeout = task['case_timeout_seconds']
        if deadline - time.time() < timeout + 60:
            break
        packet, sha = queue_packet(api, args.queue_id, task)
        packet['owner'] = owner
        by_id = {job['reference_id']: job for job in task['jobs']}
        packet['skipped'] = sorted(set(packet['skipped']) | {
            job['reference_id'] for job in task['jobs'] if job['physical_id'] in shared})
        last_progress = 0
        last_checkpoint = 0

        def checkpoint(force=False):
            nonlocal sha, last_checkpoint
            from parallel_campaign import DocumentConflict
            packet['updated_at'] = utc_now()
            atomic_json(args.directory / (task_id + '.json'), packet)
            if not force and time.monotonic() - last_checkpoint < 60:
                return True
            limit = time.monotonic() + (300 if force else 0)
            while True:
                try:
                    sha = queue_write(api, queue_path(args.queue_id, 'task', task_id), packet, sha)
                    last_checkpoint = time.monotonic()
                    return True
                except DocumentConflict:
                    remote, current_sha = queue_packet(api, args.queue_id, task)
                    if remote.get('owner') not in (None, owner) or any(
                            packet['records'].get(key) != value for key, value in remote['records'].items()):
                        raise ValueError('Reference checkpoint changed ownership or evidence')
                    sha = current_sha
                except (APIError, OSError, RuntimeError) as error:
                    print(f'REFERENCE_CHECKPOINT_DEFERRED worker={args.worker} saved_locally={len(packet["records"])} error={type(error).__name__}', flush=True)
                if time.monotonic() >= limit:
                    return False
                time.sleep(5)

        def accept(job, evidence):
            record = reference_record(job, evidence, provenance)
            packet['records'][job['reference_id']] = record
            packet['active'] = None
            checkpoint()
            print(f'REFERENCE_SAVED worker={args.worker} a={job["semimajor_m"]} '
                  f'ar={job["aspect_ratio"]} boundary={job["boundary"]} '
                  f'angle={job["incidence_deg"]} f={job["frequency_hz"]} '
                  f'status={record["status"]} qualified={record["qualified"]} '
                  f'reference_seconds={evidence.get("elapsed_seconds")} '
                  f'warmup_seconds={evidence.get("warmup_seconds")}', flush=True)

        def progress(active):
            nonlocal last_progress
            job = by_id[active['reference_id']]
            packet['active'] = dict(active, frequency_hz=job['frequency_hz'],
                semimajor_m=job['semimajor_m'], aspect_ratio=job['aspect_ratio'],
                boundary=job['boundary'], incidence_deg=job['incidence_deg'])
            print(queue_timing_line(packet['active'], args.worker), flush=True)
            if time.monotonic() - last_progress >= 30:
                checkpoint()
                last_progress = time.monotonic()

        checkpoint()
        while True:
            todo = [job for job in task['jobs'] if job['reference_id'] not in packet['records']
                    and job['reference_id'] not in packet['skipped']]
            if not todo or deadline - time.time() < timeout + 60:
                break
            with queue_heartbeat(packet, args.worker):
                completed = _run_batch(todo, runtime, args.directory, 4, deadline - 60, timeout,
                    accept, progress, attempts_dir=args.directory / 'attempts')
            if not completed:
                break
        packet['active'] = None
        if not checkpoint(force=True):
            print('REFERENCE_SAVED_FOR_ARTIFACT_RECOVERY ' + task_id, flush=True)
            break
        todo = [j for j in task['jobs'] if j['reference_id'] not in packet['records']
                and j['reference_id'] not in packet['skipped']]
        if todo:
            break
        previous = task_id
        task_id = queue_claim(api, args.queue_id, owner, previous=previous)
    print(f'REFERENCE_WORKER_STOP worker={args.worker} pending_chunk={task_id}', flush=True)


def queue_merge_curves(api, curves):
    from reference_curves import publish_shared_curves
    for attempt in range(4):
        selected = queue_shared(api)
        missing = [curve for curve in curves if curve['physical_id'] not in selected]
        if not missing:
            return 0
        try:
            return publish_shared_curves(api, missing)
        except ValueError as error:
            if str(error) != 'Conflicting qualified TS references' or attempt == 3:
                raise
    raise RuntimeError('Reference selection changed during publication')


def queue_publish(api, plan, directory, seen):
    from publish_progress import _progress_tree
    from reference_curves import curves_from_records, publish_shared_curves
    from concurrent.futures import ThreadPoolExecutor
    _, _, tree = _progress_tree(api)
    changed = [(task, tree.get(queue_path(plan['queue_id'], 'task', task['id']), {}).get('sha'))
               for task in plan['tasks']]
    changed = [(task, sha) for task, sha in changed if sha and seen.get(task['id']) != sha]
    if hasattr(api, 'reference_git'):
        api.reference_git.prefetch([sha for task, sha in changed])
    def read(pair):
        task, sha = pair
        packet, actual = queue_packet(api, plan['queue_id'], task)
        return task, packet, actual
    with ThreadPoolExecutor(max_workers=4) as pool:
        packets = list(pool.map(read, changed))
    jobs, records = [], []
    for task, packet, sha in packets:
        for job in task['jobs']:
            record = packet['records'].get(job['reference_id'])
            if record and record['qualified']:
                jobs.append(job)
                records.append(record)
                atomic_json(directory / (job['reference_id'] + '.json'), record)
    if jobs:
        print(f'REFERENCE_PUBLISHING qualified_records={len(records)} changed_chunks={len(packets)}', flush=True)
        publish_cache(api, jobs, directory)
        print('REFERENCE_EVIDENCE_SAVED publishing shared curves', flush=True)
        queue_merge_curves(api, curves_from_records(jobs, {r['reference_id']:r for r in records}))
    for task, packet, sha in packets:
        seen[task['id']] = sha
    print(f'REFERENCE_PUBLISHED qualified_records={len(records)} changed_chunks={len(packets)}', flush=True)
    return packets


def queue_monitor(api, args, once=False):
    import time
    plan = queue_plan(api, args.queue_id)
    if not once:
        queue_begin_controller(api, args)
    args.directory.mkdir(parents=True, exist_ok=True)
    receipt_path = args.directory / 'publication-receipts.json'
    receipt = read_json(receipt_path) if receipt_path.exists() else {}
    seen, stats = receipt.get('seen', {}), receipt.get('stats', {})
    deadline = time.monotonic() + args.budget_seconds
    while True:
        if not once:
            try:
                queue_refill(api, args, plan)
            except (APIError, OSError, RuntimeError) as error:
                print(f'REFERENCE_REFILL_RETRY error={type(error).__name__}', flush=True)
            state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
            legacy = state.get('legacy_run')
            if legacy:
                try:
                    old = api.request('GET', '/actions/runs/' + legacy)
                except APIError as error:
                    print(f'REFERENCE_LEGACY_STATUS_DEFERRED error={error.status}', flush=True)
                else:
                    if old['status'] == 'completed':
                        queue_change(api, args.queue_id, lambda current: current.pop('legacy_run', None))
                    else:
                        print('REFERENCE_REFILL_ACTIVE previous publisher finishing', flush=True)
                        if time.monotonic() >= deadline:
                            break
                        time.sleep(30)
                        continue
        try:
            changed = queue_publish(api, plan, args.directory, seen)
            for task, packet, sha in changed:
                stats[task['id']] = dict(qualified=sum(r['qualified'] for r in packet['records'].values()),
                    failed=sum(not r['qualified'] for r in packet['records'].values()),
                    skipped=len(set(packet['skipped']) - set(packet['records'])),
                    active=packet['active'], owner=packet.get('owner'))
            atomic_json(receipt_path, dict(seen=seen, stats=stats))
        except (APIError, OSError, RuntimeError) as error:
            if once:
                raise
            print(f'REFERENCE_PUBLICATION_RETRY error={type(error).__name__} detail={str(error)[:180]}', flush=True)
            # Preserve the last complete public counters while publication is unavailable.
            if time.monotonic() >= deadline:
                break
            time.sleep(15)
            continue
        state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
        totals = {key:sum(s[key] for s in stats.values()) for key in ('qualified','failed','skipped')}
        totals['planned'] = sum(len(t['jobs']) for t in plan['tasks'])
        totals['remaining'] = totals['planned'] - sum(totals[key] for key in ('qualified','failed','skipped'))
        totals.update(updated_at=utc_now(), queue_id=args.queue_id,
            active=[dict(value['active'], worker=value['owner'],
                         elapsed_seconds=max(0.,time.time()-value['active']['started_epoch'])) for task_id, value in stats.items()
                    if value['active'] and task_id in state['claims']])
        progress_path = queue_path(args.queue_id, 'progress')
        _, progress_sha = queue_read(api, progress_path)
        queue_write(api, progress_path, totals, progress_sha)
        print('REFERENCE_POINTS ' + json.dumps(totals, separators=(',', ':')), flush=True)
        for active in totals['active']:
            print(queue_timing_line(active, active['worker']), flush=True)
        print(f'REFERENCE_QUEUE_PROGRESS finished_chunks={len(state["finished"])} '
              f'total_chunks={len(plan["tasks"])} active_workers={len(state["claims"])} '
              f'pending_chunks={len(state["pending"])}', flush=True)
        if once or (not state['pending'] and not state['claims']) or time.monotonic() >= deadline:
            break
        time.sleep(45)
    if not once:
        state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
        if state['pending'] or state['claims']:
            queue_next_controller(api, args)
        else:
            queue_change(api, args.queue_id, lambda current: current.update(
                status='completed_with_failures' if totals['failed'] else 'completed'))


def queue_recover(api, plan, run, workers=None):
    """Recover saved worker packets before returning interrupted claims to the queue."""
    tasks = {task['id']:task for task in plan['tasks']}
    artifacts = api.request('GET', f'/actions/runs/{run}/artifacts?per_page=100')['artifacts']
    for artifact in artifacts:
        if artifact['expired'] or not artifact['name'].startswith('reference-queue-worker-'):
            continue
        if workers is not None and int(artifact['name'].rsplit('-', 1)[1]) not in workers:
            continue
        with zipfile.ZipFile(io.BytesIO(api.archive(artifact['id']))) as archive:
            for entry in archive.infolist():
                task_id = PurePosixPath(entry.filename).stem
                if task_id not in tasks or entry.file_size > MAX_RECORD_BYTES * 16:
                    continue
                saved = json.loads(archive.read(entry))
                task = tasks[task_id]
                current, sha = queue_packet(api, plan['queue_id'], task)
                original = json.dumps(current, sort_keys=True)
                if saved.get('task_id') != task_id:
                    raise ValueError('Invalid worker reference artifact')
                allowed = {j['reference_id']:j for j in task['jobs']}
                for identity, record in saved['records'].items():
                    if identity not in allowed or (record.get('qualified') and not valid_cached(record, allowed[identity])):
                        raise ValueError('Invalid saved reference evidence')
                    if identity in current['records'] and current['records'][identity] != record:
                        raise ValueError('Conflicting saved reference evidence')
                    current['records'][identity] = record
                if current.get('owner', '').startswith(run + ':'):
                    current['active'] = None
                if json.dumps(current, sort_keys=True) != original:
                    queue_write(api, queue_path(plan['queue_id'], 'task', task_id), current, sha)


def queue_finish(api, args):
    if os.getenv('QUEUE_ROLE', 'pool') == 'workers':
        print('REFERENCE_WORKER_ALLOCATION_FINISHED controller handles recovery', flush=True)
        return
    run = str(os.environ['GITHUB_RUN_ID'])
    jobs = api.request('GET', f'/actions/runs/{run}/jobs?per_page=100')['jobs']
    publisher = next((job for job in jobs if job['name'] == 'queue-progress'), None)
    # Cancellation of a pending job does not make the whole workflow cancelled.
    # A superseded publisher must never create another pending publisher.
    if (publisher is None or publisher['status'] != 'completed' or
            publisher.get('conclusion') not in ('success', 'failure', 'timed_out')):
        print('REFERENCE_CONTROLLER_FINALIZER_SKIPPED publisher_not_completed_or_cancelled', flush=True)
        return
    state, _ = queue_read(api, queue_path(args.queue_id, 'state'))
    if state.get('controller_run') != run:
        print('REFERENCE_CONTROLLER_FINALIZER_SKIPPED publisher_never_started_or_superseded', flush=True)
        return
    plan = queue_plan(api, args.queue_id)
    queue_reconcile(api, args.queue_id, plan)
    if os.getenv('QUEUE_ROLE', 'pool') != 'workers':
        queue_next_controller(api, args)


def queue_main():
    parser = argparse.ArgumentParser(description='Continue only transferred missing reference work')
    parser.add_argument('action', choices=['queue-init', 'queue-settings', 'queue-worker', 'queue-monitor', 'queue-finish'])
    parser.add_argument('--queue-id', required=True)
    parser.add_argument('--repository', default=os.getenv('GITHUB_REPOSITORY'))
    parser.add_argument('--worker', type=int, default=0)
    parser.add_argument('--runtime', type=Path)
    parser.add_argument('--directory', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--budget-seconds', type=float, default=14400)
    args = parser.parse_args()
    if not 600 <= args.budget_seconds <= 18000:
        parser.error('Reference queue budget must be between 600 and 18000 seconds')
    api = reference_api(args.repository, os.environ.get('GITHUB_TOKEN'), ROOT)
    actions = {'queue-init':queue_initialize, 'queue-settings':queue_settings,
               'queue-worker':queue_worker, 'queue-monitor':queue_monitor, 'queue-finish':queue_finish}
    actions[args.action](api, args)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("queue-"):
        queue_main()
    else:
        main()

"""Run independent, serial spectra and merge immutable timing observations."""
from __future__ import annotations

import argparse
import base64
import copy
from datetime import datetime
import gzip
import hashlib
import io
import json
import math
import os
import platform as host_platform
from pathlib import Path
import random
import re
import subprocess
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict

import benchmark as core
from campaign import checkpoint_settings, read_checkpoint, select_resume_artifacts
from publish_progress import (APIError, PublicationConflict, BRANCH, GitHub, compact_payload, existing_snapshot,
                              publish, retained_comparison_rows, snapshot_filename, _progress_tree)
from refresh import _environments, _runtime_selection

WORKFLOW = 'parallel-spectra.yml'
PLATFORM = 'ubuntu-latest'
PLATFORMS = {'ubuntu-latest': ('ubuntu-24.04', 'Linux'),
             'windows-latest': ('windows-latest', 'Windows'),
             'macos-latest': ('macos-latest', 'Darwin')}
MAX_BYTES = 512 * 1024 * 1024
MAX_EXPANDED_PLAN_BYTES = 1024 * 1024 * 1024
PLAN_RUNTIME_ENCODING = 'gzip-base64-plan-runtime-pool-v1'
DOCUMENT_PART_BYTES = 4 * 1024 * 1024
FREQUENCY_TIMEOUT_SECONDS = 300
TIMEOUT_STREAK_LENGTH = 5
TIMEOUT_STREAK_RULE = 'five_consecutive_prediction_timeouts'
PACKET_CACHE_BYTES = 64 * 1024 * 1024
TASK_FIELDS = ('software', 'semimajor_m', 'aspect_ratio', 'boundary', 'incidence_deg', 'repetition')
REFERENCE_FIELDS = ('reference_id', 'reference_source_sha', 'reference_ts_db',
                    'reference_qualification', 'reference_fingerprint', 'reference_uncertainty_db')
CONTROLLER_FILES = {'src/setup_julia.jl', 'src/prepare.py', 'tests/test_prepare.py', 'src/parallel_campaign.py', 'src/publish_progress.py', 'src/dataset.py', 'src/report.py',
                    'tests/test_parallel_campaign.py', 'tests/test_publish_progress.py', 'tests/test_dataset.py'}


class LiveGitHub(GitHub):
    """Give every network operation a bounded deadline during long-running jobs."""
    def request(self, method, path, body=None):
        data_store = getattr(self, 'data_store', None)
        if data_store is not None and data_store.handles(method, path):
            return data_store.request(method, path, body)
        for attempt in range(4):
            self.deadline = time.monotonic() + 90
            try:
                return super().request(method, path, body)
            except APIError as error:
                if (not getattr(error, 'rate_limited', False) or attempt == 3 or
                        not getattr(self, 'wait_on_rate_limit', True)):
                    raise
                delay = max(60 * 2**attempt, getattr(error, 'retry_after', 0),
                    getattr(error, 'rate_reset', 0)-time.time()+2)
                print(f'API_RATE_LIMIT status={error.status} remaining={getattr(error, "rate_remaining", None)} '
                      f'reset_epoch={getattr(error, "rate_reset", None)} retry_seconds={delay:.0f}', flush=True)
                until = time.monotonic()+delay
                while time.monotonic() < until:
                    time.sleep(min(30, until-time.monotonic()))

    def archive(self, artifact_id):
        self.deadline = time.monotonic() + 180
        return super().archive(artifact_id)


class GitDataStore:
    """Read and publish data through the existing checkout without REST polling."""
    def __init__(self, root):
        self.root = Path(root)
        self.lock = threading.RLock()
        self.fetched = 0.
        self.head = None

    def git(self, *arguments, data=None, env=None, check=True):
        try:
            result = subprocess.run(['git', *arguments], cwd=self.root, input=data,
                capture_output=True, env=env, timeout=180)
        except subprocess.TimeoutExpired:
            self.fetched = float('-inf')
            raise APIError(503, f'Git data transport exceeded its deadline during {arguments[0]}') from None
        if check and result.returncode:
            raise APIError(503, 'Git data transport failed')
        return result

    def fetch(self, force=False):
        with self.lock:
            if self.head is not None and not force and time.monotonic()-self.fetched < 15:
                return self.head
            ref = f'refs/remotes/origin/{BRANCH}'
            self.git('config','remote.origin.promisor','true')
            self.git('config','remote.origin.partialclonefilter','blob:none')
            self.git('fetch', '--quiet', '--no-tags', '--depth=1', '--filter=blob:none', 'origin',
                f'+refs/heads/{BRANCH}:{ref}')
            self.head = self.git('rev-parse', ref).stdout.decode().strip()
            self.fetched = time.monotonic()
            return self.head

    def handles(self, method, path):
        return (path.startswith('/contents/') and path.endswith('?ref='+BRANCH) or
            path.startswith(('/git/blobs', '/git/trees', '/git/commits')) or
            path == '/git/ref/heads/'+BRANCH or path == '/git/refs/heads/'+BRANCH or
            path.startswith('/git/ref/tags/spectra-') or method == 'POST' and path == '/git/refs')

    def blob(self, sha):
        raw = self.git('cat-file', 'blob', sha).stdout
        return dict(sha=sha, size=len(raw), encoding='base64', content=base64.b64encode(raw).decode())

    def request(self, method, path, body=None):
        if method == 'GET' and path.startswith('/git/ref/tags/spectra-'):
            ref='refs/tags/'+path.rsplit('/',1)[1]
            raw=self.git('ls-remote','--refs','origin',ref).stdout
            if not raw:
                raise APIError(404,'Missing campaign tag')
            return dict(object=dict(sha=raw.split()[0].decode()))
        if method == 'POST' and path == '/git/refs':
            ref=body['ref']
            if not re.fullmatch(r'refs/tags/spectra-[A-Za-z0-9-]+',ref):
                raise ValueError('Only immutable campaign tags can be created')
            result=self.git('push','--quiet','origin',f"{body['sha']}:{ref}",check=False)
            if result.returncode:
                raise APIError(409,'Campaign tag publication failed')
            return dict(object=dict(sha=body['sha']))
        if method == 'GET' and path.startswith('/contents/'):
            name = path[len('/contents/'):].split('?')[0]
            head = self.fetch()
            entry = self.git('ls-tree', '-z', head, '--', name).stdout
            if not entry:
                raise APIError(404, 'Missing Git data file')
            sha = entry.split(b'\t', 1)[0].split()[2].decode()
            return self.blob(sha)
        if method == 'GET' and path == '/git/ref/heads/'+BRANCH:
            return dict(object=dict(sha=self.fetch(force=True)))
        if method == 'GET' and path.startswith('/git/blobs/'):
            return self.blob(path.rsplit('/',1)[1])
        if method == 'GET' and path.startswith('/git/commits/'):
            raw = self.git('cat-file','commit',path.rsplit('/',1)[1]).stdout
            return dict(tree=dict(sha=raw.splitlines()[0].split()[1].decode()))
        if method == 'GET' and path.startswith('/git/trees/'):
            raw = self.git('ls-tree','-z',path.rsplit('/',1)[1]).stdout
            entries = []
            for item in raw.split(b'\0'):
                if not item:
                    continue
                metadata, name = item.split(b'\t',1)
                mode, kind, sha = metadata.decode().split()
                entries.append(dict(path=name.decode(),mode=mode,type=kind,sha=sha))
            return dict(tree=entries,truncated=False)
        if method == 'POST' and path == '/git/blobs':
            raw = base64.b64decode(body['content'],validate=True)
            return dict(sha=self.git('hash-object','-w','--stdin',data=raw).stdout.decode().strip())
        if method == 'POST' and path == '/git/trees':
            with tempfile.TemporaryDirectory(prefix='spectra-index-') as temporary:
                env = dict(os.environ,GIT_INDEX_FILE=str(Path(temporary)/'index'))
                self.git('read-tree',body.get('base_tree','--empty'),env=env)
                lines = []
                for entry in body['tree']:
                    name = entry['path']
                    if not re.fullmatch(r'[A-Za-z0-9_.-]+',name):
                        raise ValueError('Invalid data checkpoint path')
                    lines.append(f"100644 blob {entry['sha']}\t{name}\n")
                self.git('update-index','--index-info',data=''.join(lines).encode(),env=env)
                # Parent blobs already exist remotely. Avoid hydrating the entire partial checkout.
                return dict(sha=self.git('write-tree','--missing-ok',env=env).stdout.decode().strip())
        if method == 'POST' and path == '/git/commits':
            arguments=['-c','user.name=acousticTSbenchmark','-c','user.email=benchmark@users.noreply.github.com',
                'commit-tree',body['tree']]
            for parent in body.get('parents',[]):
                arguments.extend(['-p',parent])
            return dict(sha=self.git(*arguments,data=(body['message']+'\n').encode()).stdout.decode().strip())
        if method == 'PATCH' and path == '/git/refs/heads/'+BRANCH:
            if body.get('force'):
                raise ValueError('Forced data publication is prohibited')
            result = self.git('push','--quiet','origin',f"{body['sha']}:refs/heads/{BRANCH}",check=False)
            if result.returncode:
                self.fetched = 0.
                if b'rejected' in result.stderr or b'failed to update ref' in result.stderr:
                    raise APIError(409,'Concurrent Git data publication')
                raise APIError(503,'Git data publication failed')
            self.head, self.fetched = body['sha'], time.monotonic()
            return dict(object=dict(sha=body['sha']))
        raise ValueError('Unsupported Git data operation')


def _is_plan_document(value):
    return (isinstance(value, dict) and 'campaign_id' in value and 'source_ref' in value
            and isinstance(value.get('datasets'), dict) and isinstance(value.get('tasks'), list))


def _plan_runtime_payload(value):
    # Pool exact runtime values while retaining every environment and observation.
    plan = dict(value, datasets={})
    runtimes, runtime_indexes, environment_runtimes = [], {}, {}
    for size, dataset in value['datasets'].items():
        if not isinstance(dataset, dict):
            raise ValueError('Invalid plan dataset')
        saved = dict(dataset)
        if 'environments' in dataset:
            if not isinstance(dataset['environments'], list):
                raise ValueError('Invalid plan environments')
            environments, indexes = [], []
            for environment in dataset['environments']:
                if not isinstance(environment, dict):
                    raise ValueError('Invalid plan environment')
                compact = dict(environment)
                index = -1
                if 'runtime' in compact:
                    runtime = compact.pop('runtime')
                    key = json.dumps(runtime, allow_nan=False, separators=(',', ':'))
                    if key not in runtime_indexes:
                        runtime_indexes[key] = len(runtimes)
                        runtimes.append(runtime)
                    index = runtime_indexes[key]
                environments.append(compact)
                indexes.append(index)
            saved['environments'] = environments
            environment_runtimes[size] = indexes
        plan['datasets'][size] = saved
    return dict(schema_version=1, plan=plan, runtimes=runtimes,
                environment_runtimes=environment_runtimes)


def _check_plan_runtime_payload(payload):
    if (not isinstance(payload, dict)
            or set(payload) != {'schema_version', 'plan', 'runtimes', 'environment_runtimes'}
            or type(payload['schema_version']) is not int or payload['schema_version'] != 1
            or not _is_plan_document(payload['plan'])
            or not isinstance(payload['runtimes'], list)
            or not isinstance(payload['environment_runtimes'], dict)):
        raise ValueError('Invalid pooled plan envelope')
    plan, runtimes, indexes = payload['plan'], payload['runtimes'], payload['environment_runtimes']
    lengths, unique = [], set()
    for runtime in runtimes:
        key = json.dumps(runtime, allow_nan=False, separators=(',', ':'))
        if key in unique:
            raise ValueError('Duplicate pooled plan runtime')
        unique.add(key)
        lengths.append(len(key))
    # Count exact expanded JSON bytes before allocating independent runtime copies.
    encoder = json.JSONEncoder(allow_nan=False, separators=(',', ':'))
    expanded = sum(map(len, encoder.iterencode(plan)))
    expected, used = set(), set()
    for size, dataset in plan['datasets'].items():
        if not isinstance(dataset, dict):
            raise ValueError('Invalid pooled plan dataset')
        if 'environments' not in dataset:
            continue
        expected.add(size)
        environments, references = dataset['environments'], indexes.get(size)
        if (not isinstance(environments, list) or not isinstance(references, list)
                or len(environments) != len(references)):
            raise ValueError('Pooled plan environment indexes differ from the dataset')
        for environment, reference in zip(environments, references):
            if (not isinstance(environment, dict) or 'runtime' in environment
                    or type(reference) is not int or not -1 <= reference < len(runtimes)):
                raise ValueError('Invalid pooled plan runtime reference')
            if reference >= 0:
                used.add(reference)
                expanded += lengths[reference] + len('"runtime":') + bool(environment)
                if expanded > MAX_EXPANDED_PLAN_BYTES:
                    raise ValueError('Expanded spectrum plan exceeds its size limit')
    if set(indexes) != expected or used != set(range(len(runtimes))):
        raise ValueError('Pooled plan contains unmatched runtime data')
    if expanded > MAX_EXPANDED_PLAN_BYTES:
        raise ValueError('Expanded spectrum plan exceeds its size limit')


def pack(value):
    pooled = _is_plan_document(value) and any(
        isinstance(dataset, dict) and 'environments' in dataset for dataset in value['datasets'].values())
    payload = _plan_runtime_payload(value) if pooled else value
    if pooled:
        _check_plan_runtime_payload(payload)
    raw = json.dumps(payload, allow_nan=False, separators=(',', ':')).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('Spectrum state exceeds its size limit')
    return dict(encoding=PLAN_RUNTIME_ENCODING if pooled else 'gzip-base64',
                sha256=hashlib.sha256(raw).hexdigest(),
                data=base64.b64encode(gzip.compress(raw, mtime=0)).decode())


def unpack(value):
    encoding = value.get('encoding')
    if encoding not in ('gzip-base64', PLAN_RUNTIME_ENCODING):
        raise ValueError('Unrecognized spectrum state encoding')
    with gzip.GzipFile(fileobj=io.BytesIO(base64.b64decode(value['data'], validate=True))) as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != value['sha256']:
        raise ValueError('Spectrum state content does not match its digest')
    payload = json.loads(raw)
    if encoding == 'gzip-base64':
        return payload
    _check_plan_runtime_payload(payload)
    plan, runtimes = payload['plan'], payload['runtimes']
    for size, references in payload['environment_runtimes'].items():
        for environment, reference in zip(plan['datasets'][size]['environments'], references):
            if reference >= 0:
                environment['runtime'] = copy.deepcopy(runtimes[reference])
    return plan


def identifier(value):
    value = str(value)
    if not re.fullmatch(r'[1-9][0-9]{0,19}', value):
        raise ValueError('Campaign identity must be a positive GitHub run ID')
    return value


def filename(campaign, kind, task=None):
    name = f'spectra-{identifier(campaign)}-{kind}'
    if kind not in ('plan', 'state', 'task') or (kind == 'task') != (task is not None):
        raise ValueError('Invalid spectrum state kind')
    if task is not None:
        if not re.fullmatch(r'[a-f0-9]{24}', task):
            raise ValueError('Invalid spectrum task ID')
        name += '-' + task
    return name + '.json'


def _document_bytes(api, path):
    try:
        entry = api.request('GET', f'/contents/{path}?ref={BRANCH}')
    except APIError as error:
        if error.status == 404:
            return None, None
        raise
    blob = entry
    if blob.get('encoding') != 'base64' or not blob.get('content'):
        blob = api.request('GET', '/git/blobs/' + entry['sha'])
    raw = base64.b64decode(''.join(blob['content'].split()), validate=True)
    if hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() != entry['sha']:
        raise ValueError('Downloaded spectrum state has a different Git digest')
    return raw, entry['sha']


def get_document(api, path):
    raw, sha = _document_bytes(api, path)
    if raw is None:
        return None, None
    return _unpack_document(api, raw), sha


def _unpack_document(api, raw):
    saved = json.loads(raw)
    if saved.get('encoding') == 'spectrum-parts-v1':
        length = saved.get('size')
        parts = saved.get('parts')
        if (type(length) is not int or not 0 < length <= MAX_BYTES * 2 or
                not isinstance(parts, list) or not 1 <= len(parts) <= 256):
            raise ValueError('Invalid spectrum document parts')
        chunks, total = [], 0
        for part in parts:
            if not isinstance(part, str) or not re.fullmatch(r'[a-f0-9]{64}', part):
                raise ValueError('Invalid spectrum document part identity')
            chunk, _ = _document_bytes(api, f'spectra-part-{part}.part')
            if chunk is None or hashlib.sha256(chunk).hexdigest() != part:
                raise ValueError('Missing or corrupt spectrum document part')
            total += len(chunk)
            if total > length:
                raise ValueError('Spectrum document parts exceed the recorded size')
            chunks.append(chunk)
        raw = b''.join(chunks)
        if len(raw) != length or hashlib.sha256(raw).hexdigest() != saved.get('sha256'):
            raise ValueError('Spectrum document parts do not match the recorded digest')
        saved = json.loads(raw)
    return unpack(saved)


def _document_blob(api, raw):
    expected = hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()
    result = api.request('POST', '/git/blobs', dict(
        content=base64.b64encode(raw).decode(), encoding='base64'))
    if result['sha'] != expected:
        raise ValueError('Saved spectrum document blob has a different digest')
    return expected


class DocumentConflict(ValueError):
    """A competing writer changed the expected checkpoint revision."""


def invalidate_data_cache(api):
    store = getattr(api, 'data_store', None)
    if isinstance(store, GitDataStore):
        store.fetched = float('-inf')


def put_document(api, path, value, previous_sha=None, *, immutable=False):
    previous, actual_sha = get_document(api, path)
    if immutable and previous is not None:
        if previous != value:
            raise ValueError('Refusing to replace an existing spectrum plan')
        return actual_sha
    if previous_sha is not None and actual_sha != previous_sha:
        if previous == value:
            return actual_sha
        invalidate_data_cache(api)
        raise DocumentConflict('Another writer changed this task or campaign state')
    previous_sha = actual_sha
    raw = (json.dumps(pack(value), separators=(',', ':')) + '\n').encode()
    entries = []
    if len(raw) > DOCUMENT_PART_BYTES:
        parts = []
        for offset in range(0, len(raw), DOCUMENT_PART_BYTES):
            chunk = raw[offset:offset + DOCUMENT_PART_BYTES]
            digest = hashlib.sha256(chunk).hexdigest()
            parts.append(digest)
            entries.append(dict(path=f'spectra-part-{digest}.part', mode='100644',
                type='blob', sha=_document_blob(api, chunk)))
        raw = json.dumps(dict(encoding='spectrum-parts-v1', parts=parts, size=len(raw),
            sha256=hashlib.sha256(raw).hexdigest()), separators=(',', ':')).encode()
    proposed_sha = _document_blob(api, raw)
    entries.append(dict(path=path, mode='100644', type='blob', sha=proposed_sha))
    # Upload blobs independently, then attach every part and the manifest atomically.
    for attempt in range(12):
        head, tree_sha, existing = _progress_tree(api)
        current_sha = existing.get(path, {}).get('sha')
        if current_sha == proposed_sha:
            return proposed_sha
        if current_sha != previous_sha:
            invalidate_data_cache(api)
            raise DocumentConflict('Another writer changed this task or campaign state')
        body = dict(tree=entries)
        if tree_sha:
            body['base_tree'] = tree_sha
        tree = api.request('POST', '/git/trees', body)
        commit = api.request('POST', '/git/commits', dict(message=f'Checkpoint {path}',
            tree=tree['sha'], parents=[head] if head else []))
        try:
            if head:
                api.request('PATCH', '/git/refs/heads/' + BRANCH,
                    dict(sha=commit['sha'], force=False))
            else:
                api.request('POST', '/git/refs',
                    dict(ref='refs/heads/' + BRANCH, sha=commit['sha']))
            return proposed_sha
        except APIError as error:
            if error.status not in (409, 422) or attempt == 11:
                raise
            latest, _, _ = _progress_tree(api)
            if latest == head and error.status != 409:
                raise
            delay = random.uniform(.5, min(8., 2. ** attempt))
            print(f'CHECKPOINT_RETRY conflict={attempt + 1}/12 delay_seconds={delay:.2f}', flush=True)
            time.sleep(delay)
    raise RuntimeError('Unable to checkpoint spectrum state')


def put_documents(api, updates):
    """Commit a bounded checkpoint batch with per-document conflict checks."""
    if len(updates) == 1:
        path, value, previous_sha = updates[0]
        return {path: put_document(api, path, value, previous_sha)}
    if not updates:
        return {}
    if len(updates) > 32 or len({path for path, _, _ in updates}) != len(updates):
        raise ValueError('Invalid checkpoint batch size or duplicate path')
    entries, expected, proposed = [], {}, {}
    for path, value, previous_sha in updates:
        expected[path] = previous_sha
        raw = (json.dumps(pack(value), separators=(',', ':')) + '\n').encode()
        if len(raw) > DOCUMENT_PART_BYTES:
            parts = []
            for offset in range(0, len(raw), DOCUMENT_PART_BYTES):
                chunk = raw[offset:offset + DOCUMENT_PART_BYTES]
                digest = hashlib.sha256(chunk).hexdigest()
                parts.append(digest)
                entries.append(dict(path=f'spectra-part-{digest}.part', mode='100644',
                    type='blob', sha=_document_blob(api, chunk)))
            raw = json.dumps(dict(encoding='spectrum-parts-v1', parts=parts, size=len(raw),
                sha256=hashlib.sha256(raw).hexdigest()), separators=(',', ':')).encode()
        proposed[path] = _document_blob(api, raw)
        entries.append(dict(path=path, mode='100644', type='blob', sha=proposed[path]))
    entries = list({entry['path']: entry for entry in entries}.values())
    for attempt in range(12):
        head, tree_sha, existing = _progress_tree(api)
        current = {path: existing.get(path, {}).get('sha') for path in proposed}
        if current == proposed:
            return proposed
        if any(current[path] not in (expected[path], proposed[path]) for path in proposed):
            invalidate_data_cache(api)
            raise DocumentConflict('Another writer changed a checkpoint in the batch')
        body = dict(tree=entries)
        if tree_sha:
            body['base_tree'] = tree_sha
        tree = api.request('POST', '/git/trees', body)
        commit = api.request('POST', '/git/commits', dict(
            message=f'Consolidate {len(updates)} spectrum checkpoints',
            tree=tree['sha'], parents=[head] if head else []))
        try:
            if head:
                api.request('PATCH', '/git/refs/heads/' + BRANCH, dict(sha=commit['sha'], force=False))
            else:
                api.request('POST', '/git/refs', dict(ref='refs/heads/' + BRANCH, sha=commit['sha']))
            return proposed
        except APIError as error:
            if error.status not in (409, 422) or attempt == 11:
                raise
            latest, _, _ = _progress_tree(api)
            if latest == head and error.status != 409:
                raise
            time.sleep(random.uniform(.5, min(8., 2. ** attempt)))
    raise RuntimeError('Unable to checkpoint spectrum batch')


def queue_filename(campaign_id, run_id):
    return f'spectra-{identifier(campaign_id)}-queue-{identifier(run_id)}.json'


def change_queue(api, campaign_id, run_id, worker_id, completed_task=None, outcome=None):
    """Claim or finish a whole spectrum with an atomic ownership update."""
    path = queue_filename(campaign_id, run_id)
    for attempt in range(32):
        queue, sha = get_document(api, path)
        if (queue is None or queue.get('campaign_id') != campaign_id or
                queue.get('run_id') != int(run_id) or worker_id not in queue['owners']):
            raise ValueError('Worker does not belong to the saved spectrum queue')
        if completed_task is None:
            owned = queue['owners'][worker_id]
            if owned is not None:
                return owned
            if not queue['pending']:
                return None
            selected = queue['pending'].pop(0)
            if selected in queue['claims']:
                raise ValueError('Spectrum queue contains an already claimed task')
            queue['claims'][selected] = worker_id
            queue['owners'][worker_id] = selected
        else:
            if queue['claims'].get(completed_task) != worker_id:
                raise ValueError('A worker cannot complete another worker spectrum')
            if queue['finished'].get(completed_task) == outcome:
                return completed_task
            if queue['owners'][worker_id] != completed_task:
                raise ValueError('Spectrum completion differs from the current claim')
            queue['finished'][completed_task] = outcome
            if outcome == 'complete':
                queue['owners'][worker_id] = None
            selected = completed_task
        queue['updated_at'] = core.utc_now()
        try:
            put_document(api, path, queue, sha)
            return selected
        except DocumentConflict:
            pass
        except (APIError, OSError, TimeoutError) as error:
            if isinstance(error, APIError) and error.status not in (409, 422, 500, 502, 503, 504):
                raise
        if attempt == 31:
            raise PublicationConflict('Spectrum claim remained busy. Saved measurements are unchanged')
        time.sleep(random.uniform(.1, min(2., .2 * (attempt + 1))))


def execution_policy():
    """Report the active prediction budget and whole-sweep scheduling policy."""
    return dict(case_timeout_seconds=FREQUENCY_TIMEOUT_SECONDS,
                scheduling='whole_sweep', frequency_deferral=False,
                consecutive_timeouts=TIMEOUT_STREAK_LENGTH)


def timeout_streak(task, records):
    """Find adjacent measured timeouts at the active budget within one repetition."""
    streak = []
    for case in sorted(task['cases'], key=lambda case: case['frequency_hz']):
        row = records.get(case['case_id'], {})
        if measured_timeout(row, FREQUENCY_TIMEOUT_SECONDS):
            streak.append(case['case_id'])
            if len(streak) == TIMEOUT_STREAK_LENGTH:
                return dict(after_frequency_hz=case['frequency_hz'], rule=TIMEOUT_STREAK_RULE,
                            trigger_case_ids=streak)
        else:
            streak = []
    return None


def measured_timeout(row, minimum_seconds=None):
    """Distinguish measured prediction timeouts from inferred or shorter-budget rows."""
    if row.get('status') != 'timeout' or row.get('prediction_attempted') is False:
        return False
    seconds = row.get('case_timeout_seconds')
    return minimum_seconds is None or (isinstance(seconds, (int, float))
        and not isinstance(seconds, bool) and math.isfinite(seconds) and seconds >= minimum_seconds)


def timeout_basis_frequency(task, records, basis, *, check_records=True):
    """Validate the recorded trigger independently of later completed frequencies."""
    identities = [case['case_id'] for case in sorted(task['cases'], key=lambda case: case['frequency_hz'])]
    trigger = basis.get('trigger_case_ids', []) if isinstance(basis, dict) else []
    rule = basis.get('rule') if isinstance(basis, dict) else None
    length = {'ten_consecutive_prediction_timeouts': 10,
              'five_consecutive_prediction_timeouts': 5}.get(rule)
    if (not isinstance(basis, dict) or set(basis) != {'rule', 'trigger_case_ids'} or
            length is None or not isinstance(trigger, list) or
            len(trigger) != length or trigger[0] not in identities):
        raise ValueError('Invalid timeout-tail trigger')
    start = identities.index(trigger[0])
    if identities[start:start + length] != trigger or check_records and any(
            not measured_timeout(records.get(identity, {}),
                300 if rule == 'five_consecutive_prediction_timeouts' else None) for identity in trigger):
        raise ValueError('Timeout-tail trigger requires adjacent measured timeouts at the stated budget')
    return next(case['frequency_hz'] for case in task['cases'] if case['case_id'] == trigger[-1])


def scheduling_queue(plan, candidates, packets, run_id, parallel):
    """Assign every incomplete spectrum to one worker without frequency deferral."""
    queue = dict(schema_version=2, campaign_id=plan['campaign_id'], run_id=run_id,
                 frequency_deferral=False, scheduling='whole_sweep',
                 pending=[], deferred=[], units={}, owners={}, claims={}, finished={}, timeout_streaks={},
                 actual_timeouts={})
    resumed_sweeps = []
    for task in candidates:
        packet = packets.get(task['id'], {})
        records = packet.get('records', {})
        queue['actual_timeouts'][task['id']] = sorted(identity for identity, row in records.items()
            if measured_timeout(row, FREQUENCY_TIMEOUT_SECONDS))
        cutoff = timeout_streak(task, records)
        if cutoff:
            queue['timeout_streaks'][task['id']] = cutoff
        remaining = [case['case_id'] for case in task['cases'] if case['case_id'] not in records]
        if not remaining:
            continue
        queue['units'][task['id']] = dict(kind='sweep', task_id=task['id'])
        (resumed_sweeps if records else queue['pending']).append(task['id'])
    queue['pending'].extend(resumed_sweeps)
    count = min(parallel, len(queue['pending']) + len(queue['deferred']))
    # Existing task identities remain usable as worker-slot identities.
    workers = [task['id'] for task in candidates[:count]]
    while len(workers) < count:
        workers.append(core.fingerprint([plan['campaign_id'], run_id, 'worker', len(workers)])[:24])
    queue['owners'] = {worker: None for worker in workers}
    return queue, workers


def claim_queued_work(queue, plan, tasks, worker_id):
    selected = queue['owners'][worker_id]
    if selected is not None:
        return dict(queue['units'][selected], id=selected), False
    waiting = queue['pending'] or queue['deferred']
    if not waiting:
        return (dict(kind='wait') if any(queue['owners'].values()) else None), False
    selected = waiting.pop(0)
    unit = queue['units'][selected]
    cutoff = queue.get('timeout_streaks', {}).get(unit['task_id'])
    if unit['kind'] == 'frequency' and cutoff:
        cases = {case['case_id']: case for case in tasks[unit['task_id']]['cases']}
        if cases[unit['case_id']]['frequency_hz'] > cutoff['after_frequency_hz']:
            tail = [selected] + [identity for identity in queue['deferred']
                if queue['units'][identity]['task_id'] == unit['task_id']
                and cases[identity]['frequency_hz'] > cutoff['after_frequency_hz']]
            excluded = set(tail)
            queue['deferred'] = [identity for identity in queue['deferred'] if identity not in excluded]
            selected = core.fingerprint([plan['campaign_id'], queue['run_id'], 'timeout_tail', tail])
            queue['units'][selected] = dict(kind='timeout_tail', task_id=unit['task_id'],
                case_ids=tail, timeout_basis={key: cutoff[key] for key in ('rule', 'trigger_case_ids')})
    if selected in queue['claims']:
        raise ValueError('Frequency queue contains an already claimed assignment')
    queue['claims'][selected] = worker_id
    queue['owners'][worker_id] = selected
    return dict(queue['units'][selected], id=selected), True


def change_work_queue(api, plan, run_id, worker_id, completed_unit=None, packet=None, *, claim_next=False):
    """Atomically claim one sweep or one frequency, with fresh sweeps first."""
    path = queue_filename(plan['campaign_id'], run_id)
    tasks = {task['id']: task for task in plan['tasks']}
    for attempt in range(32):
        queue, sha = get_document(api, path)
        if (not queue or queue.get('schema_version') != 2 or
                queue.get('campaign_id') != plan['campaign_id'] or
                queue.get('run_id') != int(run_id) or worker_id not in queue['owners']):
            raise ValueError('Worker does not belong to the frequency queue')
        changed = False
        if completed_unit is not None:
            selected = completed_unit
            if queue['claims'].get(selected) != worker_id:
                raise ValueError('Frequency assignment belongs to another worker')
            if selected in queue['finished']:
                if not claim_next:
                    return dict(queue['units'][selected], id=selected)
            else:
                if queue['owners'][worker_id] != selected or packet is None:
                    raise ValueError('Frequency completion differs from the active assignment')
                unit = queue['units'][selected]
                task = tasks[unit['task_id']]
                validate_packet(plan, task, packet)
                outcome = packet['state']
                if unit['kind'] in ('frequency', 'timeout_tail') and outcome not in ('budget_paused', 'setup_failed'):
                    required = [unit['case_id']] if unit['kind'] == 'frequency' else unit['case_ids']
                    if any(identity not in packet['records'] for identity in required):
                        raise ValueError('Frequency assignment has no saved observation')
                actual = set(queue.setdefault('actual_timeouts', {}).get(task['id'], []))
                actual.update(identity for identity, row in packet['records'].items()
                    if measured_timeout(row, FREQUENCY_TIMEOUT_SECONDS))
                queue['actual_timeouts'][task['id']] = sorted(actual)
                cutoff = timeout_streak(task, {identity: dict(status='timeout',
                    case_timeout_seconds=FREQUENCY_TIMEOUT_SECONDS) for identity in actual})
                if cutoff:
                    queue.setdefault('timeout_streaks', {})[task['id']] = cutoff
                if queue.get('frequency_deferral', True) and unit['kind'] == 'sweep' and packet.get('slow_sweep'):
                    for case in task['cases']:
                        case_id = case['case_id']
                        if case_id in packet['records']:
                            continue
                        if case_id in queue['units']:
                            raise ValueError('Deferred frequency already has an assignment')
                        queue['units'][case_id] = dict(kind='frequency', task_id=task['id'], case_id=case_id)
                        queue['deferred'].append(case_id)
                queue['finished'][selected] = outcome
                queue['owners'][worker_id] = None
                changed = True
            result = dict(queue['units'][selected], id=selected)
        if completed_unit is None or claim_next:
            result, claimed = claim_queued_work(queue, plan, tasks, worker_id)
            changed = changed or claimed
        if not changed:
            return result
        queue['updated_at'] = core.utc_now()
        try:
            put_document(api, path, queue, sha)
            return result
        except DocumentConflict:
            pass
        except (APIError, OSError, TimeoutError) as error:
            if isinstance(error, APIError) and error.status not in (409, 422, 500, 502, 503, 504):
                raise
        if attempt == 31:
            raise PublicationConflict('Frequency assignment remained busy. Saved observations are retained')
        time.sleep(random.uniform(.1, min(2., .2 * (attempt + 1))))


def merge_task_packet(api, plan, task, packet):
    """Merge disjoint worker records with compare-and-swap protection."""
    validate_packet(plan, task, packet)
    path = filename(plan['campaign_id'], 'task', task['id'])
    for attempt in range(32):
        current, sha = get_document(api, path)
        merged = copy.deepcopy(packet)
        merged.pop('_blob_sha', None)
        if current:
            validate_packet(plan, task, current)
            for identity, row in current['records'].items():
                if identity in merged['records'] and merged['records'][identity] != row:
                    raise ValueError('Concurrent worker observations conflict')
                merged['records'][identity] = row
            for identity, environment in current['environments'].items():
                if identity in merged['environments'] and merged['environments'][identity] != environment:
                    raise ValueError('Concurrent worker environments conflict')
                merged['environments'][identity] = environment
            if current.get('slow_sweep'):
                merged['slow_sweep'] = True
        if len(merged['records']) == len(task['cases']):
            merged['state'] = 'complete'
        validate_packet(plan, task, merged)
        try:
            put_document(api, path, merged, sha)
            return merged
        except DocumentConflict:
            if attempt == 31:
                raise PublicationConflict('Concurrent frequency results could not be merged')
            time.sleep(random.uniform(.1, min(2., .2 * (attempt + 1))))


def worker_packet_filename(campaign_id, run_id, worker_id, task_id):
    if not all(re.fullmatch(r'[a-f0-9]{24}', value) for value in (worker_id, task_id)):
        raise ValueError('Invalid worker packet identity')
    return f'spectra-{identifier(campaign_id)}-worker-{identifier(run_id)}-{worker_id}-{task_id}.json'


def combine_packets(plan, task, packets, *, check_tail_evidence=True):
    """Combine immutable observations while rejecting conflicting duplicates."""
    merged = None
    for packet in packets:
        validate_packet(plan, task, packet, check_tail_evidence=False)
        if merged is None:
            merged = copy.deepcopy(packet)
            for key in ('_blob_sha', '_worker_packet_shas', 'publication_owner'):
                merged.pop(key, None)
            continue
        for field in ('records', 'environments'):
            for identity, value in packet[field].items():
                if identity in merged[field] and merged[field][identity] != value:
                    raise ValueError(f'Concurrent worker {field} conflict')
                merged[field][identity] = copy.deepcopy(value)
        if packet.get('slow_sweep'):
            merged['slow_sweep'] = True
    if merged is not None:
        merged['state'] = 'complete' if len(merged['records']) == len(task['cases']) else 'running'
        validate_packet(plan, task, merged, check_tail_evidence=check_tail_evidence)
    return merged


def publish_worker_packet(api, plan, task, packet, work):
    """Publish to a single-owner file without competing for the sweep record."""
    validate_packet(plan, task, packet)
    path = worker_packet_filename(plan['campaign_id'], work['run_id'], work['worker_id'], task['id'])
    records = {identity: row for identity, row in packet['records'].items()
               if row.get('measurement_worker_id') == work['worker_id']
               and row['measurement_origin_run_id'] == work['run_id']}
    if not records:
        return None
    invocations = {row['invocation_id'] for row in records.values()}
    fragment = dict(campaign_id=plan['campaign_id'], task_id=task['id'], source_ref=plan['source_ref'],
                    records=records, environments={key:packet['environments'][key] for key in invocations},
                    state='fragment_complete')
    if packet.get('slow_sweep'):
        fragment['slow_sweep'] = True
    current, sha = get_document(api, path)
    merged = combine_packets(plan, task, [value for value in (current, fragment) if value is not None],
                             check_tail_evidence=False)
    merged['state'] = 'complete' if len(merged['records']) == len(task['cases']) else 'fragment_complete'
    merged['publication_owner'] = dict(run_id=work['run_id'], worker_id=work['worker_id'])
    return put_document(api, path, merged, sha)


def csv_values(value, cast=str):
    values = [cast(part.strip()) for part in str(value or '').split(',') if part.strip()]
    if len(set(values)) != len(values):
        raise ValueError('Duplicate selection values')
    return values


def all_artifacts(api, run_id):
    result = []
    for page in range(1, 101):
        items = api.request('GET', f'/actions/runs/{int(run_id)}/artifacts?per_page=100&page={page}')['artifacts']
        result.extend(items)
        if len(items) < 100:
            return result
    raise ValueError('Too many checkpoint artifacts')


def campaign_platform(plan):
    platform = plan.get('platform', PLATFORM)
    if platform not in PLATFORMS:
        raise ValueError('Unsupported benchmark platform')
    return platform


def load_legacy(api, run_id, platform=PLATFORM):
    run = api.request('GET', f'/actions/runs/{int(run_id)}')
    artifact = select_resume_artifacts(all_artifacts(api, run_id), platform=platform).get(platform)
    if artifact is None:
        raise ValueError(f'No saved {platform} measurements for run {run_id}')
    archive = api.archive(artifact['id'])
    digest = 'sha256:' + hashlib.sha256(archive).hexdigest()
    if artifact.get('digest') and artifact['digest'] != digest:
        raise ValueError('Baseline artifact does not match its GitHub digest')
    interrupted = run.get('conclusion') in ('cancelled', 'failure')
    settings = checkpoint_settings(archive, api.repository, run['head_sha'], platform,
                                   allow_unexported=interrupted)
    values, records = read_checkpoint(archive, allow_unexported=interrupted)
    cases = core.build_cases(settings['configuration'], settings['profile'],
                            settings['software'].split(','), settings['repetitions'])
    exported = {row['case_id']: row for row in values['results.json']}
    rows = []
    for row in core.summarize(cases, records, {}):
        prior = exported[row['case_id']]
        row.update({key: prior[key] for key in REFERENCE_FIELDS if key in prior})
        if row['status'] == 'ok' and row.get('reference_ts_db') is not None:
            row['delta_ts_db'] = row['ts_db'] - row['reference_ts_db']
        rows.append(row)
    retained = values.get('retained-results.json', dict(schema_version=1, rows=[], provenance=[]))
    rows += retained_comparison_rows(retained, cases)
    evidence = list(_environments(archive)['evidence'].values())
    for proof in retained.get('provenance', []):
        evidence.extend(proof.get('environments', {}).get('evidence', {}).values())
    return dict(configuration=settings['configuration'], profile=settings['profile'], rows=rows,
                environments=evidence, baseline_run_id=int(run_id), archive_sha256=digest, platform=platform)


def read_packet_blob(api, sha):
    """Reuse unchanged decoded records within a bounded worker-local cache."""
    cache = api.__dict__.setdefault('_packet_blob_cache',
        dict(entries=OrderedDict(), bytes=0, lock=threading.RLock()))
    with cache['lock']:
        if sha in cache['entries']:
            cache['entries'].move_to_end(sha)
            return cache['entries'][sha][0]
    blob = api.request('GET', '/git/blobs/' + sha)
    raw = base64.b64decode(''.join(blob['content'].split()), validate=True)
    if hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() != sha:
        raise ValueError('Worker packet does not match its Git digest')
    packet = _unpack_document(api, raw)
    size = len(json.dumps(packet, separators=(',', ':')).encode())
    if size <= PACKET_CACHE_BYTES:
        with cache['lock']:
            if sha not in cache['entries']:
                while cache['entries'] and cache['bytes'] + size > PACKET_CACHE_BYTES:
                    _, (_, removed) = cache['entries'].popitem(last=False)
                    cache['bytes'] -= removed
                cache['entries'][sha] = (packet, size)
                cache['bytes'] += size
    return packet


def read_packets(api, plan, *, cache=None):
    cache = {} if cache is None else cache
    tasks = plan['tasks']
    _, _, tree = _progress_tree(api)
    def read(task):
        name = filename(plan['campaign_id'], 'task', task['id'])
        pattern = re.compile(r'spectra-' + re.escape(plan['campaign_id']) +
            r'-worker-([1-9][0-9]*)-([a-f0-9]{24})-' + re.escape(task['id']) + r'\.json')
        entries = sorted((path, entry) for path, entry in tree.items()
                         if path == name or pattern.fullmatch(path))
        if not entries:
            return task['id'], None
        digest = core.fingerprint([(path, entry['sha']) for path, entry in entries])
        if cache.get(task['id'], {}).get('_blob_sha') == digest:
            return task['id'], cache[task['id']]
        canonical = read_packet_blob(api, tree[name]['sha']) if name in tree else None
        integrated = (canonical or {}).get('integrated_worker_packets', {})
        if not isinstance(integrated, dict):
            raise ValueError('Invalid consolidated worker packet index')
        packets = [canonical] if canonical is not None else []
        worker_shas = {}
        for path, entry in entries:
            if path == name:
                continue
            worker_shas[path] = entry['sha']
            if integrated.get(path) == entry['sha']:
                continue
            packet = read_packet_blob(api, entry['sha'])
            match = pattern.fullmatch(path)
            if match and packet.get('publication_owner') != dict(run_id=int(match[1]), worker_id=match[2]):
                raise ValueError('Worker packet publication owner differs from its path')
            packets.append(packet)
        packet = combine_packets(plan, task, packets)
        packet['_blob_sha'] = digest
        packet['_worker_packet_shas'] = worker_shas
        return task['id'], packet
    # Downloads do not execute numerical solves.
    with ThreadPoolExecutor(max_workers=4) as pool:
        for task_id, packet in pool.map(read, tasks):
            if packet is not None:
                cache[task_id] = packet
    return cache


def read_task_packet(api, plan, task):
    return read_packets(api, dict(plan, tasks=[task])).get(task['id'])


def consolidate_worker_packets(api, plan, packets):
    """Integrate stopped-worker records in bounded atomic batches."""
    updates, merged = [], {}
    def flush():
        if not updates:
            return
        put_documents(api, updates)
        packets.update(merged)
        print(f'CONSOLIDATED_BATCH spectra={len(updates)} '
              f'records={sum(len(value["records"]) for value in merged.values())}', flush=True)
        updates.clear()
        merged.clear()
    for task in plan['tasks']:
        packet = packets.get(task['id'])
        if not packet or not packet.get('_worker_packet_shas'):
            continue
        integrated = packet['_worker_packet_shas']
        if packet.get('integrated_worker_packets') == integrated:
            continue
        path = filename(plan['campaign_id'], 'task', task['id'])
        current, sha = get_document(api, path)
        consolidated = combine_packets(plan, task, [value for value in (current, packet) if value is not None])
        consolidated['integrated_worker_packets'] = dict(integrated)
        for key in list(consolidated):
            if key.startswith('_'):
                consolidated.pop(key)
        if any(consolidated['records'].get(identity) != row for identity, row in packet['records'].items()):
            raise ValueError('Consolidation changed a saved measurement')
        updates.append((path, consolidated, sha))
        merged[task['id']] = consolidated
        if len(updates) == 32:
            flush()
    flush()


def validate_packet(plan, task, packet, *, check_tail_evidence=True):
    if (packet.get('campaign_id') != plan['campaign_id'] or packet.get('task_id') != task['id']
            or packet.get('source_ref') != plan['source_ref']):
        raise ValueError('Worker packet belongs to a different campaign or task')
    cases = {case['case_id']: case for case in task['cases']}
    records = packet.get('records')
    if not isinstance(records, dict) or records.keys() - cases.keys():
        raise ValueError('Worker packet contains unexpected physical cases')
    for identity, row in records.items():
        case = cases[identity]
        if ('prediction_attempted' in row and not isinstance(row['prediction_attempted'], bool) or
                row.get('timeout_basis') is not None and row.get('prediction_attempted') is not False):
            raise ValueError('Invalid prediction attempt provenance')
        if (any(row.get(key) != value for key, value in case.items()) or
                row.get('source_sha') != plan['sources']['sources'][case['software']]['sha'] or
                row.get('status') not in core.STATUSES or
                row.get('measurement_id') != core.fingerprint([plan['campaign_id'], identity]) or
                row.get('invocation_id') not in packet.get('environments', {}) or
                row.get('allocation_id') != row.get('invocation_id')):
            raise ValueError('Worker observation does not match its physical inputs or provenance')
        environment = packet['environments'][row['invocation_id']]
        if (environment.get('platform') != PLATFORMS[campaign_platform(plan)][1] or
                str(environment.get('github_run_id')) != str(row.get('measurement_origin_run_id')) or
                environment.get('runtime', {}).get('source_shas', {}).get(case['software']) != row['source_sha']):
            raise ValueError('Timing origin or source differs from its worker environment')
        if row['status'] == 'ok' and (not math.isfinite(row.get('elapsed_seconds', float('nan')))
                                     or row['elapsed_seconds'] <= 0 or not math.isfinite(row['ts_db'])):
            raise ValueError('Successful timing must have a finite positive solve duration')
        if row.get('prediction_attempted') is False:
            cutoff = timeout_basis_frequency(task, records, row.get('timeout_basis'),
                                             check_records=check_tail_evidence)
            if (row['status'] != 'timeout' or case['frequency_hz'] <= cutoff or
                    any(value is not None for key, value in row.items() if key in (
                        'elapsed_seconds', 'wall_seconds', 'solve_lower_bound_seconds', 'ts_db', 'delta_ts_db',
                        'amplitude_real_m', 'amplitude_imag_m') or key.startswith('memory_'))):
                raise ValueError('Inferred timeout requires ten measured timeouts and no invented measurements')
    if packet.get('state') == 'complete' and records.keys() != cases.keys():
        raise ValueError('Incomplete spectrum was marked complete')


def recover_artifacts(api, plan, wave):
    """Recover final local checkpoints when a worker lost its publication connection."""
    tasks = {task['id']: task for task in plan['tasks'] if task['id'] in wave['tasks']}
    queue = get_document(api, wave['queue'])[0] if wave.get('queue') else None
    frequency_queue = queue is not None and queue.get('schema_version') == 2
    recovered_tasks = {}
    print(f'RECOVERY run={wave["run_id"]} stage=artifacts', flush=True)
    for artifact in all_artifacts(api, wave['run_id']):
        match = re.fullmatch(r'spectrum-([a-f0-9]{24})-([1-9][0-9]*)', artifact['name'])
        valid_workers = wave['workers'] if frequency_queue else tasks
        if not match or match[1] not in valid_workers or artifact.get('expired'):
            continue
        if queue is not None and match[1] not in wave['workers']:
            raise ValueError('Recovery artifact belongs to an unknown worker')
        if not wave.get('queue'):
            task = tasks[match[1]]
            path = filename(plan['campaign_id'], 'task', task['id'])
            current, sha = get_document(api, path)
            if current and current.get('state') == 'complete':
                continue
        if frequency_queue:
            claimed = {queue['units'][identity]['task_id'] for identity, owner in queue['claims'].items()
                       if owner == match[1]}
            if claimed - tasks.keys():
                raise ValueError('Recovery worker claims a spectrum outside the allocation')
            complete = bool(claimed)
            for task_id in sorted(claimed):
                if task_id not in recovered_tasks:
                    recovered_tasks[task_id] = read_task_packet(api, plan, tasks[task_id])
                published = recovered_tasks[task_id]
                if published is None:
                    complete = False
                    continue
                validate_packet(plan, tasks[task_id], published)
                complete = complete and published.get('state') == 'complete'
            if complete:
                print(f'RECOVERY artifact={artifact["id"]} already_complete=true tasks={len(claimed)}', flush=True)
                continue
        print(f'RECOVERY artifact={artifact["id"]} stage=download', flush=True)
        raw = api.archive(artifact['id'])
        if artifact.get('digest') and artifact['digest'] != 'sha256:' + hashlib.sha256(raw).hexdigest():
            raise ValueError('Recovery artifact digest does not match GitHub')
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = [entry for entry in archive.infolist() if Path(entry.filename).name == 'checkpoint.json']
            if not entries:
                continue
            if (len(entries) > len(tasks) or (not wave.get('queue') and len(entries) != 1)
                    or any(entry.file_size > MAX_BYTES for entry in entries)):
                raise ValueError('Ambiguous or oversized recovery checkpoint')
            saved_packets = [json.loads(archive.read(entry)) for entry in entries]
        seen = set()
        for saved in saved_packets:
            task_id = saved.get('task_id')
            if task_id not in tasks or task_id in seen or (not wave.get('queue') and task_id != match[1]):
                raise ValueError('Recovery artifact contains an unexpected or duplicate spectrum')
            if queue is not None and not frequency_queue and queue['claims'].get(task_id) != match[1]:
                raise ValueError('Recovery spectrum differs from the saved worker ownership')
            seen.add(task_id)
            task = tasks[task_id]
            path = filename(plan['campaign_id'], 'task', task_id)
            if frequency_queue:
                if task_id not in recovered_tasks:
                    print(f'RECOVERY artifact={artifact["id"]} task={task_id} stage=read_saved', flush=True)
                    recovered_tasks[task_id] = read_task_packet(api, plan, task)
                current = recovered_tasks[task_id]
                validate_packet(plan, task, saved, check_tail_evidence=False)
                # Shared snapshots may include records published by other owners.
                owned = set()
                for unit_id, owner in queue['claims'].items():
                    unit = queue['units'][unit_id]
                    if owner != match[1] or unit['task_id'] != task_id:
                        continue
                    owned.update([unit['case_id']] if unit['kind'] == 'frequency' else
                                 unit['case_ids'] if unit['kind'] == 'timeout_tail' else
                                 [case['case_id'] for case in task['cases']])
                if not owned:
                    raise ValueError('Recovery artifact has no claimed frequency or sweep')
                for identity, row in saved.get('records', {}).items():
                    if identity not in owned and (current or {}).get('records', {}).get(identity) != row:
                        raise ValueError('Recovery observation belongs to another frequency owner')
                known = current is not None
                for field in ('records', 'environments'):
                    for identity, value in saved[field].items():
                        existing = (current or {}).get(field, {})
                        if identity in existing and existing[identity] != value:
                            raise ValueError(f'Recovery artifact conflicts with published {field}')
                        known = known and identity in existing
                if known and (not saved.get('slow_sweep') or current.get('slow_sweep')):
                    print(f'RECOVERY artifact={artifact["id"]} task={task_id} already_saved=true', flush=True)
                    continue
                recovered = combine_packets(plan, task, [value for value in (current, saved) if value is not None])
                if current and current.get('_worker_packet_shas'):
                    recovered['integrated_worker_packets'] = dict(current['_worker_packet_shas'])
                recovered_tasks[task_id] = merge_task_packet(api, plan, task, recovered)
            else:
                current, sha = get_document(api, path)
                restore_packet(api, plan, task, saved, current, sha)
            print(f'RECOVERY artifact={artifact["id"]} task={task_id} saved=true', flush=True)
    print(f'RECOVERY run={wave["run_id"]} stage=complete', flush=True)


def restore_packet(api, plan, task, saved, current, sha):
    """Merge a recovered spectrum without replacing any published observation."""
    validate_packet(plan, task, saved)
    if current:
        validate_packet(plan, task, current)
        for identity, row in current['records'].items():
            if identity in saved['records'] and saved['records'][identity] != row:
                raise ValueError('Recovery artifact conflicts with a published observation')
            saved['records'][identity] = row
        saved['environments'].update(current['environments'])
    if len(saved['records']) == len(task['cases']):
        saved['state'] = 'complete'
    validate_packet(plan, task, saved)
    if current != saved:
        put_document(api, filename(plan['campaign_id'], 'task', task['id']), saved, sha)


def merge_dataset(dataset, tasks, packets):
    rows = {row['case_id']: copy.deepcopy(row) for row in dataset['rows']}
    if len(rows) != len(dataset['rows']):
        raise ValueError('Baseline contains duplicate observations')
    for task in tasks:
        packet = packets.get(task['id'], {})
        for identity, observation in packet.get('records', {}).items():
            old = rows[identity]
            if old.get('status') != 'pending':
                old_measurement = {key: value for key, value in old.items()
                                   if not key.startswith('reference_') and key != 'delta_ts_db'}
                if old_measurement != observation:
                    raise ValueError('Refusing to overwrite an already recorded timing')
                continue
            references = {key: old[key] for key in REFERENCE_FIELDS if key in old}
            rows[identity] = dict(observation, **references)
            if observation['status'] == 'ok' and references.get('reference_ts_db') is not None:
                rows[identity]['delta_ts_db'] = observation['ts_db'] - references['reference_ts_db']
    return list(rows.values())


def load_parallel(api, campaign_id, size, platform=PLATFORM):
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    if plan is None:
        raise ValueError('Saved parallel benchmark plan is missing')
    if campaign_platform(plan) != platform:
        raise ValueError('Saved parallel benchmark belongs to another platform')
    dataset = plan['datasets'][format(size, '.15g')]
    packets = read_packets(api, plan)
    tasks = [task for task in plan['tasks'] if task['size'] == format(size, '.15g')]
    environments = list(dataset.get('environments', []))
    for task in tasks:
        environments.extend(packets.get(task['id'], {}).get('environments', {}).values())
    return dict(dataset, rows=merge_dataset(dataset, tasks, packets), environments=environments,
                baseline_run_id=int(campaign_id), platform=platform)


def prepare_dataset(baseline, inputs, sources):
    config = copy.deepcopy(baseline['configuration'])
    profile = baseline['profile']
    selection = config['profiles'][profile]
    programs = csv_values(inputs.get('software', 'acousticts'))
    if not programs or set(programs) - (core.CAPABILITIES.keys() - {'acousticscattering'}):
        raise ValueError('Select supported timing software')
    for field, cast in [('aspect_ratios', float), ('incidences_deg', float), ('boundaries', str)]:
        chosen = csv_values(inputs.get(field), cast)
        if chosen:
            prior = selection.get(field, config['geometry'].get(field, config.get(field, [])))
            selection[field] = [next((old for old in prior if old == value), value) for value in chosen]
    supported = set().union(*(core.CAPABILITIES[program] for program in programs))
    selection['boundaries'] = [boundary for boundary in selection.get('boundaries', config['boundaries'])
                               if boundary in supported]
    if not selection['boundaries']:
        raise ValueError('The selected software supports none of the requested boundaries')
    grid = [str(inputs.get('frequency_' + name + '_khz', '')).strip() for name in ('start', 'end', 'interval')]
    if any(grid):
        if not all(grid):
            raise ValueError('Supply all three frequency inputs or leave all three blank')
        values = [float(value) * 1000 for value in grid]
        if not all(math.isfinite(value) and value > 0 for value in values) or values[1] < values[0]:
            raise ValueError('Invalid frequency grid')
        config['frequency'] = dict(start_hz=values[0], stop_hz=values[1], step_hz=values[2], include_stop=True)
    previous = [row for row in baseline['rows'] if row['software'] in programs]
    repeats = max([row['repetition'] for row in previous] or [0])
    mode = inputs['mode']
    if mode not in ('extend', 'refresh', 'repeat', 'retry'):
        raise ValueError('Mode must be extend, refresh, repeat or retry')
    additional = int(inputs.get('additional_repetitions', 1))
    if not 1 <= additional <= 20:
        raise ValueError('Additional repetitions must be between 1 and 20')
    first_new = repeats + 1
    if mode == 'repeat':
        repeats += additional
    elif mode == 'refresh':
        repeats = additional
    else:
        repeats = max(1, repeats)
    config.update(software=programs, repetitions=repeats, case_order='boundary_major')
    cases = [case for case in core.build_cases(config, profile, programs, repeats)
             if case['boundary'] in core.CAPABILITIES[case['software']]]
    active_physical = {(case['software'], case['physical_id']) for case in cases}
    by_id = {case['case_id']: case for case in cases}
    rows, original = [], {}
    retry_ids = set()
    archived_rows = copy.deepcopy(baseline.get('archived_rows', []))
    reference_fields = {}
    for row in baseline['rows']:
        if row.get('reference_ts_db') is not None:
            reference_fields[row['physical_id']] = {k: row[k] for k in REFERENCE_FIELDS if k in row}
        if (row['software'], row['physical_id']) not in active_physical:
            rows.append(copy.deepcopy(row))
            continue
        if mode == 'refresh' and row['status'] != 'pending':
            archived_rows.append(copy.deepcopy(row))
        if row['status'] != 'pending' and mode != 'refresh':
            case = by_id.get(row['case_id'])
            if case is None or any(row.get(key) != value for key, value in case.items()):
                raise ValueError('Existing selected measurements use different solver settings')
            if row.get('source_sha') != sources['sources'][row['software']]['sha']:
                raise ValueError('Selected software revision changed. Use refresh, not append or extend')
            if mode == 'retry' and row['status'] in ('worker_error', 'numerical_error', 'nonconverged', 'timeout'):
                archived_rows.append(copy.deepcopy(row))
                retry_ids.add(row['case_id'])
            else:
                original[row['case_id']] = row
    pending = []
    saved_case_ids = {row['case_id'] for row in baseline['rows']}
    for case in cases:
        if mode == 'retry' and case['case_id'] not in saved_case_ids:
            continue
        row = copy.deepcopy(original.get(case['case_id'], dict(case, status='pending')))
        row.pop('measurement_role', None)
        row.pop('retained_from_run_id', None)
        if case['case_id'] not in original:
            row.update(reference_fields.get(case['physical_id'], {}))
        rows.append(row)
        if (row['status'] == 'pending' and (mode != 'repeat' or case['repetition'] >= first_new)
                and (mode != 'retry' or case['case_id'] in retry_ids)):
            pending.append(case)
    compatibility = {}
    for program in programs:
        expected = sources['sources'][program]['sha']
        values = []
        for env in baseline.get('environments', []):
            runtime = env.get('runtime', {})
            if runtime.get('source_shas', {}).get(program) == expected:
                value = _runtime_selection(runtime, program)
                if value not in values:
                    values.append(value)
        reused = any(row['software'] == program for row in original.values())
        if reused and mode != 'refresh' and len(values) != 1:
            raise ValueError('Saved measurements require one verified build configuration')
        compatibility[program] = values if mode != 'refresh' else []
    return dict(configuration=config, profile=profile, rows=rows,
                archived_rows=archived_rows,
                environments=baseline.get('environments', []), compatibility=compatibility,
                baseline_run_id=baseline['baseline_run_id']), pending


def make_tasks(campaign_id, pending_by_size):
    groups = {}
    for size, pending in pending_by_size.items():
        for case in pending:
            key = (size, *(case[field] for field in TASK_FIELDS))
            groups.setdefault(key, []).append(case)
    tasks = []
    for key, cases in groups.items():
        cases.sort(key=lambda case: case['frequency_hz'])
        first = cases[0]
        task_id = core.fingerprint([campaign_id, key])[:24]
        name = (f"{first['software']}-{first['boundary']}-{first['incidence_deg']:g}deg-"
                f"ar{first['aspect_ratio']:g}-a{first['semimajor_m']:g}-rep{first['repetition']}")
        tasks.append(dict(id=task_id, name=name, size=key[0], software=first['software'], cases=cases))
    random.Random(int(campaign_id)).shuffle(tasks)
    return tasks


def write_outputs(values):
    for key, value in values.items():
        text = json.dumps(value, separators=(',', ':')) if isinstance(value, (dict, list, bool)) else str(value)
        print(f'{key}={text}', flush=True)
        if os.environ.get('GITHUB_OUTPUT'):
            with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as stream:
                stream.write(f'{key}={text}\n')


def rolling_workflow(source):
    """Change dispatch concurrency without changing compiler or numerical steps."""
    old = "  group: ${{ format('spheroid-parallel-{0}', inputs.platform) }}\n"
    new = "  group: ${{ inputs.rolling_workers && format('spheroid-pool-{0}-{1}', inputs.campaign_id || github.run_id, github.run_id) || format('spheroid-parallel-{0}', inputs.platform) }}\n"
    option = "      rolling_workers:\n        description: Replace completed worker jobs while remaining jobs continue\n        type: boolean\n        default: true\n"
    if source.count(old) != 1 or source.count('      platform:\n') != 1:
        return source
    return source.replace(old,new,1).replace('      platform:\n',option+'      platform:\n',1)


def controller_files_allowed(api, files, previous, current, allowed=None):
    """Allow exact reporting and scheduling repairs without changing solver setup."""
    allowed = CONTROLLER_FILES if allowed is None else allowed
    for item in files:
        if not isinstance(item, dict) or item.get('status') == 'renamed':
            return False
        path = item.get('filename')
        if path in allowed:
            continue
        if path != '.github/workflows/parallel-spectra.yml' or item.get('status') != 'modified':
            return False
        texts = []
        for revision in (previous, current):
            document = api.request('GET', f'/contents/{path}?ref={revision}')
            if (document.get('encoding') != 'base64' or
                    not isinstance(document.get('size'), int) or document['size'] > 256 * 1024):
                return False
            texts.append(base64.b64decode(document['content'], validate=False).decode('utf-8'))
        old = '          ${{ runner.temp }}/spectrum\n'
        excluded = '          !${{ runner.temp }}/spectrum/**/fort.*\n'
        new = ''.join('          ${{ runner.temp }}/spectrum/**/' + name + '\n' for name in
                      ('checkpoint.json', 'manifest.json', 'results.json', 'environments/*.json'))
        before, after = texts
        expected = before
        if old in expected:
            expected = expected.replace(old + excluded if old + excluded in expected else old, new, 1)
        if expected == after:
            continue
        if rolling_workflow(expected) != after:
            return False
    return True


def select_controller(api, plan, state):
    """Permit a recorded orchestration repair while retaining the original numerical plan."""
    current = os.environ['GITHUB_SHA']
    previous = state.get('controller_ref', plan['source_ref'])
    if current == previous:
        return
    comparison = api.request('GET', f'/compare/{previous}...{current}')
    files = comparison.get('files', [])
    if (comparison.get('status') != 'ahead' or not files or len(files) >= 300 or
            not controller_files_allowed(api, files, previous, current)):
        raise ValueError('A controller repair must leave numerical sources, inputs and timing adapters unchanged')
    tag = f"spectra-{plan['campaign_id']}-controller-{current[:12]}"
    try:
        existing = api.request('GET', '/git/ref/tags/' + tag)
    except APIError as error:
        if error.status != 404:
            raise
        api.request('POST', '/git/refs', dict(ref='refs/tags/' + tag, sha=current))
    else:
        if existing['object']['sha'] != current:
            raise ValueError('Controller tag differs from its recorded revision')
    state.setdefault('controller_history', []).append(dict(previous=previous, current=current,
        files=[item['filename'] for item in files], changed_at=core.utc_now()))
    state.update(controller_ref=current, controller_tag=tag)
    state.pop('controller_upgrade_pending', None)


def pool_allocations(api, state):
    """Count live and queued slots across allocations sharing one claim queue."""
    root = state['worker_pool']['run_id']
    allocations = []
    for wave in state['waves']:
        if wave['run_id'] != root and wave.get('queue_run_id') != root:
            continue
        run = api.request('GET', f"/actions/runs/{wave['run_id']}")
        jobs = api.request('GET', f"/actions/runs/{wave['run_id']}/jobs?per_page=100")['jobs']
        names = dict(zip(wave['workers'], wave['worker_names']))
        by_name = {job['name']: job for job in jobs}
        stopped = {worker for worker, name in names.items()
                   if by_name.get(name, {}).get('status') == 'completed' or run['status'] == 'completed'}
        # Missing matrix jobs still reserve slots while the workflow is active.
        active = set(names) - stopped
        legacy_finishing = (not wave.get('pool') and not active and run['status'] != 'completed')
        allocations.append(dict(wave=wave, stopped=stopped, active=active,
                                legacy_finishing=legacy_finishing, run=run))
    return allocations


def pool_requeue(plan, queue, packets, stopped, blocked):
    """Release stopped owners only after final artifact recovery."""
    tasks = {task['id']: task for task in plan['tasks']}
    retriable = set()
    for identity, owner in queue['claims'].items():
        if owner not in stopped:
            continue
        task_id = queue['units'][identity]['task_id']
        retriable.add(task_id)
        if queue['owners'].get(owner) == identity:
            queue['owners'][owner] = None
            queue['finished'][identity] = 'recovered'
    occupied = {queue['units'][identity]['task_id'] for identity in
                queue['pending'] + queue['deferred'] + [value for value in queue['owners'].values() if value]}
    for task_id in sorted(retriable - occupied - set(blocked)):
        if packets.get(task_id, {}).get('state') == 'complete':
            continue
        if task_id not in tasks:
            raise ValueError('Recovered queue task is outside the campaign')
        generation = sum(unit['task_id'] == task_id for unit in queue['units'].values())
        identity = core.fingerprint([plan['campaign_id'], queue['run_id'], task_id, 'retry', generation])
        queue['units'][identity] = dict(kind='sweep', task_id=task_id)
        queue['pending'].append(identity)


def plan_pool(api, inputs, plan, state, directory):
    """Register replacement slots atomically without moving active claims."""
    campaign_id = plan['campaign_id']
    current_run = int(os.environ['GITHUB_RUN_ID'])
    if not state.get('worker_pool'):
        root = state['waves'][-1]
        queue, _ = get_document(api, root['queue'])
        if not queue or queue.get('schema_version') != 2 or queue.get('frequency_deferral') is not False:
            raise ValueError('Rolling workers require the whole-spectrum claim queue')
        state['worker_pool'] = dict(run_id=root['run_id'], queue=root['queue'])
    original_pool = state['worker_pool']
    allocations = pool_allocations(api, state)
    if any(item['legacy_finishing'] for item in allocations):
        raise RuntimeError('Wait for existing final publication before registering replacements')
    # Preserve queued workers on the old controller until every worker has started.
    previous_ref = (state.get('controller_history') or [{}])[-1].get('previous')
    if previous_ref and previous_ref != os.environ['GITHUB_SHA']:
        for allocation in allocations:
            if allocation['wave'].get('pool'):
                continue
            jobs = api.request('GET', f"/actions/runs/{allocation['wave']['run_id']}/jobs?per_page=100")['jobs']
            if any(job['name'] in allocation['wave']['worker_names'] and job['status'] == 'queued' for job in jobs):
                raise ValueError('Wait for queued workers before upgrading the controller')
    queue, _ = get_document(api, original_pool['queue'])
    if not queue['pending'] and not queue['deferred']:
        recover_pool(api, plan, state, allocations)
    maximum = int(inputs.get('max_parallel') or state.get('max_parallel', plan['max_parallel']))
    if not 1 <= maximum <= 16:
        raise ValueError('Concurrent workers must be between 1 and 16')
    controller = {key:copy.deepcopy(state[key]) for key in
                  ('controller_ref', 'controller_tag', 'controller_history') if key in state}
    for attempt in range(32):
        latest, state_sha = get_document(api, filename(campaign_id, 'state'))
        latest.update(controller)
        latest['worker_pool'] = original_pool
        queue, queue_sha = get_document(api, original_pool['queue'])
        live = pool_allocations(api, latest)
        if any(item['legacy_finishing'] for item in live):
            raise RuntimeError('Existing final publication is still active')
        existing = next((wave for wave in latest['waves'] if wave['run_id'] == current_run), None)
        if existing:
            wave = existing
            break
        count = min(maximum - sum(len(item['active']) for item in live), len(queue['pending']) + len(queue['deferred']))
        count = max(0, count)
        workers = [core.fingerprint([campaign_id, current_run, 'pool-worker', index])[:24] for index in range(count)]
        if set(workers) & set(queue['owners']):
            raise ValueError('Replacement worker identity already exists')
        tasks = sorted({unit['task_id'] for unit in queue['units'].values()})
        programs = ','.join(sorted({task['software'] for task in plan['tasks'] if task['id'] in tasks}))
        wave = dict(run_id=current_run, tasks=tasks, queue=original_pool['queue'],
                    queue_run_id=original_pool['run_id'], workers=workers, pool=True,
                    worker_names=[f'{programs} spectrum worker {index + 1:02d}' for index in range(count)],
                    execution_policy=execution_policy(), started_at=core.utc_now())
        queue['owners'].update({worker:None for worker in workers})
        queue.pop('pool_dispatch', None)
        queue.pop('pool_dispatch_blocked', None)
        queue['pool_dispatch_failures'] = 0
        latest['waves'].append(wave)
        latest.update(max_parallel=maximum, status='active', updated_at=core.utc_now())
        try:
            put_documents(api, [(original_pool['queue'], queue, queue_sha),
                                (filename(campaign_id, 'state'), latest, state_sha)])
            state = latest
            break
        except (DocumentConflict, PublicationConflict):
            if attempt == 31:
                raise
    tasks_by_id = {task['id']: task for task in plan['tasks']}
    programs = ','.join(sorted({tasks_by_id[task]['software'] for task in wave['tasks']}))
    matrix = [dict(id=worker, name=name, software=programs)
              for worker,name in zip(wave['workers'],wave['worker_names'])]
    print(f"POOL_REGISTERED run={current_run} replacements={len(matrix)} queue_run={original_pool['run_id']}", flush=True)
    directory.mkdir(parents=True, exist_ok=True)
    core.atomic_json(directory / 'wave.json', wave)
    write_outputs(dict(campaign_id=campaign_id, matrix=matrix, has_work=bool(matrix), max_parallel=maximum,
        budget=plan['budget_minutes'], platform=campaign_platform(plan), runner=PLATFORMS[campaign_platform(plan)][0]))


def recover_pool(api, plan, state, allocations):
    """Recover final artifacts before releasing stopped workers or retrying spectra."""
    path = state['worker_pool']['queue']
    queue, _ = get_document(api, path)
    recovered = set(queue.get('pool_recovered_workers', []))
    stopped = set()
    for allocation in allocations:
        missing = allocation['stopped'] - recovered
        if missing:
            recover_artifacts(api, plan, dict(allocation['wave'], workers=sorted(missing)))
            stopped.update(missing)
    if not stopped:
        return
    packets = read_packets(api, plan)
    for attempt in range(32):
        latest, state_sha = get_document(api, filename(plan['campaign_id'], 'state'))
        queue, queue_sha = get_document(api, path)
        missing = stopped - set(queue.get('pool_recovered_workers', []))
        if not missing:
            return
        attempted = {queue['units'][identity]['task_id'] for identity, owner in queue['claims'].items()
                     if owner in missing}
        for task_id in attempted:
            packet = packets.get(task_id, {})
            previous = latest['attempts'].get(task_id, dict(recorded=0, stalled=0))
            recorded = len(packet.get('records', {}))
            stalled = 0 if recorded > previous['recorded'] else previous['stalled'] + 1
            latest['attempts'][task_id] = dict(recorded=max(recorded, previous['recorded']), stalled=stalled)
            if stalled >= 3 and packet.get('state') != 'complete' and task_id not in latest['blocked']:
                latest['blocked'].append(task_id)
        pool_requeue(plan, queue, packets, missing, latest['blocked'])
        queue['pool_recovered_workers'] = sorted(set(queue.get('pool_recovered_workers', [])) | missing)
        for field in ('pending', 'deferred'):
            queue[field] = [identity for identity in queue[field]
                            if queue['units'][identity]['task_id'] not in latest['blocked']]
        latest['completed'] = sorted(set(latest['completed']) | {
            task for task, packet in packets.items() if packet.get('state') == 'complete'})
        try:
            put_documents(api, [(path, queue, queue_sha),
                                (filename(plan['campaign_id'], 'state'), latest, state_sha)])
            return
        except (DocumentConflict, PublicationConflict):
            if attempt == 31:
                raise


def pool_dispatch_waiting(api, plan, state, reservation):
    """Reconcile uncertain dispatches before retrying a failed planning job."""
    requested = datetime.fromisoformat(reservation['requested_at'].replace('Z', '+00:00'))
    age = time.time() - requested.timestamp()
    if age < 180:
        return True
    title = f"Continue spectra {plan['campaign_id']}"
    # Workflow creation can be delayed. Match every recent campaign dispatch conservatively.
    for page in range(1, 11):
        response = api.request('GET', f'/actions/workflows/{WORKFLOW}/runs?event=workflow_dispatch&per_page=100&page={page}')
        runs = response['workflow_runs']
        for run in runs:
            created = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
            if created.timestamp() < requested.timestamp() - 5:
                continue
            if run.get('display_title') == title and run['status'] != 'completed':
                return True
        if len(runs) < 100 or any(datetime.fromisoformat(run['created_at'].replace('Z', '+00:00')) < requested for run in runs):
            return False
    # Incomplete pagination cannot prove that a replacement allocation is absent.
    return True


def replenish_pool(api, plan, *, cache=None):
    """Recover stopped slots and reserve one dispatch without a batch-wide wait."""
    cache = {} if cache is None else cache
    if time.monotonic() < cache.get('next_poll', 0):
        return
    cache['next_poll'] = time.monotonic() + 60
    state, _ = get_document(api, filename(plan['campaign_id'], 'state'))
    if not state.get('worker_pool') or not plan['automatic_continue'] or state['status'] != 'active':
        return
    live = pool_allocations(api, state)
    if any(item['legacy_finishing'] for item in live):
        return
    active = sum(len(item['active']) for item in live)
    if active >= state.get('max_parallel', plan['max_parallel']):
        return
    path = state['worker_pool']['queue']
    queue, sha = get_document(api, path)
    if not queue['pending'] and not queue['deferred']:
        recover_pool(api, plan, state, live)
        queue, sha = get_document(api, path)
    reservation = queue.get('pool_dispatch')
    if reservation and pool_dispatch_waiting(api, plan, state, reservation):
        print(f"POOL_DISPATCH_PENDING active={active} reserved_at={reservation['requested_at']}", flush=True)
        return
    if reservation:
        queue.pop('pool_dispatch')
        queue['pool_dispatch_failures'] = queue.get('pool_dispatch_failures', 0) + 1
    ready = bool(queue['pending'] or queue['deferred'])
    if queue.get('pool_dispatch_failures', 0) >= 3:
        queue['pool_dispatch_blocked'] = 'Three replacement allocations failed before registration'
        put_document(api, path, queue, sha)
        print('::error::POOL_DISPATCH_BLOCKED inspect failed planning jobs', flush=True)
        return
    if not ready:
        if reservation:
            put_document(api, path, queue, sha)
        return
    queue['pool_dispatch'] = dict(requested_at=core.utc_now(), active=active)
    try:
        put_document(api, path, queue, sha)
    except (DocumentConflict, PublicationConflict):
        return
    continuation = dict(campaign_id=plan['campaign_id'], platform=campaign_platform(plan),
                        max_parallel=str(state.get('max_parallel', plan['max_parallel'])), rolling_workers='true')
    try:
        api.request('POST', f'/actions/workflows/{WORKFLOW}/dispatches',
                    dict(ref=state.get('controller_tag', f"spectra-{plan['campaign_id']}"), inputs=continuation))
    except (APIError, OSError, TimeoutError) as error:
        print(f'POOL_DISPATCH_UNCONFIRMED error={error}', flush=True)
        return
    print(f"POOL_REPLACEMENTS_DISPATCHED active={active} maximum={state.get('max_parallel', plan['max_parallel'])}", flush=True)


def plan_campaign(api, inputs, directory):
    print('PLANNING stage=load', flush=True)
    campaign_id = inputs.get('campaign_id', '').strip()
    if campaign_id:
        plan, _ = get_document(api, filename(campaign_id, 'plan'))
        state, state_sha = get_document(api, filename(campaign_id, 'state'))
        if plan is None or state is None:
            raise ValueError('Cannot continue a missing parallel campaign')
        if inputs.get('platform', campaign_platform(plan)) != campaign_platform(plan):
            raise ValueError('Continuation platform differs from the saved campaign')
        select_controller(api, plan, state)
        if state['status'] not in ('active', 'paused', 'publishing'):
            raise ValueError('Campaign has no resumable work')
        if state.get('worker_pool') or str(inputs.get('rolling_workers', 'false')).lower() == 'true':
            return plan_pool(api, inputs, plan, state, directory)
        if state['waves'] and state['waves'][-1]['run_id'] not in state.get('artifacts_recovered_runs', []):
            recover_artifacts(api, plan, state['waves'][-1])
    else:
        campaign_id = identifier(os.environ['GITHUB_RUN_ID'])
        existing, _ = get_document(api, filename(campaign_id, 'plan'))
        if existing is not None:
            return plan_campaign(api, dict(inputs, campaign_id=campaign_id), directory)
        platform = campaign_platform(inputs)
        sizes = csv_values(inputs.get('semimajor_axes', '0.07,0.1'), float)
        if not sizes or set(sizes) - {0.001, 0.01, 0.07, 0.1, 0.5}:
            raise ValueError('Select supported semimajor axes')
        baselines = {}
        for entry in csv_values(inputs.get('baseline_runs')):
            size, run_id = entry.split('=', 1)
            baselines[float(size)] = int(identifier(run_id))
        sources = core.read_json(core.ROOT / 'sources.lock.json')
        datasets, pending = {}, {}
        for size in sizes:
            snapshot, _ = existing_snapshot(api, platform, snapshot_filename(platform, size))
            if snapshot and snapshot.get('parallel_campaign_id'):
                prior_state, _ = get_document(api, filename(snapshot['parallel_campaign_id'], 'state'))
                if prior_state and prior_state['status'] != 'complete':
                    raise ValueError('Finish or continue the existing parallel campaign for this size first')
            run_id = baselines.get(size, snapshot.get('run_id') if snapshot else None)
            prior, _ = get_document(api, filename(run_id, 'plan')) if run_id else (None, None)
            if not run_id:
                config = copy.deepcopy(core.read_json(core.ROOT / 'benchmark.json'))
                config['geometry']['semimajor_m'] = size
                baseline = dict(configuration=config, profile='full', rows=[], environments=[],
                                baseline_run_id=None, platform=platform)
            elif prior is not None:
                baseline = load_parallel(api, run_id, size, platform)
            elif snapshot and size not in baselines and snapshot.get('parallel_campaign_id'):
                baseline = load_parallel(api, snapshot['parallel_campaign_id'], size, platform)
            else:
                baseline = load_legacy(api, run_id, platform)
            if baseline['configuration']['geometry']['semimajor_m'] != size:
                raise ValueError('Saved run belongs to a different semimajor axis')
            key = format(size, '.15g')
            datasets[key], pending[key] = prepare_dataset(baseline, inputs, sources)
        parallel = int(inputs.get('max_parallel', 8))
        if not 1 <= parallel <= 16:
            raise ValueError('Concurrent workers must be between 1 and 16')
        budget = int(inputs.get('timing_budget_minutes', 330))
        if budget not in (45, 120, 240, 330):
            raise ValueError('Unsupported worker budget')
        plan = dict(schema_version=1, campaign_id=campaign_id, repository=api.repository,
                    platform=platform,
                    source_ref=os.environ['GITHUB_SHA'], sources=sources, inputs=inputs,
                    datasets=datasets, tasks=make_tasks(campaign_id, pending),
                    max_parallel=parallel, budget_minutes=budget,
                    automatic_continue=str(inputs.get('automatic_continue', 'true')).lower() == 'true',
                    created_at=core.utc_now())
        state = dict(campaign_id=campaign_id, status='active', attempts={}, completed=[], blocked=[], waves=[])
        put_document(api, filename(campaign_id, 'plan'), plan, immutable=True)
        state_sha = put_document(api, filename(campaign_id, 'state'), state)
    try:
        tag = api.request('GET', f'/git/ref/tags/spectra-{campaign_id}')
    except APIError as error:
        if error.status != 404:
            raise
        api.request('POST', '/git/refs', dict(ref=f'refs/tags/spectra-{campaign_id}', sha=plan['source_ref']))
    else:
        if tag['object']['sha'] != plan['source_ref']:
            raise ValueError('Campaign tag changed since its creation')
    print('PLANNING stage=read_records', flush=True)
    packets = read_packets(api, plan)
    print('PLANNING stage=consolidate', flush=True)
    consolidate_worker_packets(api, plan, packets)
    if state.get('publish_recovered_before_resume'):
        print('PLANNING stage=publish_recovered', flush=True)
        publish_datasets(api, plan, packets, directory)
        state.pop('publish_recovered_before_resume')
        state.setdefault('recovery_publications', []).append(dict(
            run_id=int(os.environ['GITHUB_RUN_ID']), published_at=core.utc_now(),
            observations=sum(len(packet.get('records', {})) for packet in packets.values())))
    print('PLANNING stage=schedule', flush=True)
    policy = execution_policy()
    if state.get('execution_policy') != policy:
        state.setdefault('execution_policy_history', []).append(dict(
            previous=state.get('execution_policy'), current=policy,
            original_timeouts={size: dataset['configuration']['case_timeout_seconds']
                               for size, dataset in plan['datasets'].items()},
            controller_ref=os.environ['GITHUB_SHA'], changed_at=core.utc_now()))
        state['execution_policy'] = policy
    completed = {task_id for task_id, packet in packets.items() if packet.get('state') == 'complete'}
    candidates = [task for task in plan['tasks'] if task['id'] not in completed | set(state['blocked'])]
    parallel = int(inputs.get('max_parallel') or state.get('max_parallel', plan['max_parallel']))
    if not 1 <= parallel <= 16:
        raise ValueError('Concurrent workers must be between 1 and 16')
    run_id = int(os.environ['GITHUB_RUN_ID'])
    queue, workers = scheduling_queue(plan, candidates, packets, run_id, parallel)
    queue_path = queue_filename(campaign_id, run_id)
    saved_wave = next((wave for wave in state['waves'] if wave['run_id'] == run_id), None)
    if saved_wave is not None:
        if saved_wave.get('queue') != queue_path:
            raise ValueError('Continue legacy allocations with a new workflow run')
        existing_queue, _ = get_document(api, queue_path)
        if existing_queue is None or set(existing_queue['owners']) != set(saved_wave['workers']):
            raise ValueError('Saved worker slots differ from the spectrum queue')
        tasks_by_id = {task['id']: task for task in plan['tasks']}
        candidates = [tasks_by_id[task_id] for task_id in saved_wave['tasks']]
        workers = saved_wave['workers']
    else:
        put_document(api, queue_path, queue, immutable=True)
    programs = ','.join(sorted({task['software'] for task in candidates}))
    matrix = [dict(id=worker, name=f'{programs} spectrum worker {index + 1:02d}',
                    software=programs) for index, worker in enumerate(workers)]
    state.update(status='active', completed=sorted(completed), max_parallel=parallel)
    if saved_wave is None:
        state['waves'].append(dict(run_id=run_id, tasks=[task['id'] for task in candidates],
            queue=queue_path, workers=workers,
            execution_policy=policy,
            worker_names=[item['name'] for item in matrix], started_at=core.utc_now()))
    if str(inputs.get('rolling_workers', 'false')).lower() == 'true':
        state['worker_pool'] = dict(run_id=run_id, queue=queue_path)
        state['waves'][-1].update(pool=True, queue_run_id=run_id)
    put_document(api, filename(campaign_id, 'state'), state, state_sha)
    directory.mkdir(parents=True, exist_ok=True)
    core.atomic_json(directory / 'plan.json', plan)
    core.atomic_json(directory / 'wave.json', state['waves'][-1])
    write_outputs(dict(campaign_id=campaign_id, matrix=matrix, has_work=bool(workers), max_parallel=parallel,
        budget=plan['budget_minutes'], platform=campaign_platform(plan),
        runner=PLATFORMS[campaign_platform(plan)][0]))


def task_environment(runtime):
    env = core.environment()
    try:
        fields = [line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                  if line.startswith('model name')]
        if fields:
            env['processor'] = fields[0]
    except OSError:
        pass
    if host_platform.system() == 'Windows':
        import subprocess
        try:
            result = subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command',
                '(Get-CimInstance Win32_Processor | Select-Object -First 1).Name'],
                capture_output=True, text=True, check=True, timeout=15,
                creationflags=subprocess.CREATE_NO_WINDOW)
            env['processor'] = result.stdout.strip() or env.get('processor')
        except (OSError, subprocess.SubprocessError):
            pass
    elif host_platform.system() == 'Darwin':
        import subprocess
        try:
            result = subprocess.run(['sysctl', '-n', 'machdep.cpu.brand_string'],
                capture_output=True, text=True, check=True, timeout=10)
            env['processor'] = result.stdout.strip() or env.get('processor')
        except (OSError, subprocess.SubprocessError):
            pass
    env.update(runtime=runtime, started_at=core.utc_now(), github_job=os.getenv('GITHUB_JOB'))
    return env


def export_task(plan, task, packet, runtime, directory):
    """Keep worker artifacts compatible with the durable SQLite/CSV collector."""
    configuration = plan['datasets'][task['size']]['configuration']
    adapters = {name: hashlib.sha256((core.ROOT / 'src' / name).read_bytes()).hexdigest()
                for name in ('benchmark.py', 'process_control.py', 'process_gate.py',
                             'worker_acousticts.R', 'worker_python.py', 'worker_julia.jl')}
    identity = dict(configuration=configuration, sources=plan['sources'],
        profile=plan['datasets'][task['size']]['profile'], cases=[case['case_id'] for case in task['cases']],
        adapter_hashes=adapters, platform=PLATFORMS[campaign_platform(plan)][1],
        architecture=host_platform.machine(),
        environment_fingerprint=core.fingerprint(dict(runtime=runtime['environment_fingerprint'],
            memory_observer=core.memory_provenance())),
        workload='fresh_process_warmed_distinct_problem_single_prediction')
    core.atomic_json(directory / 'manifest.json', dict(identity, fingerprint=core.fingerprint(identity),
                                                      active_execution_policy=execution_policy()))
    core.atomic_json(directory / 'results.json',
        [packet['records'].get(case['case_id'], dict(case, status='pending')) for case in task['cases']])
    for invocation, environment in packet['environments'].items():
        core.atomic_json(directory / 'environments' / f'{invocation}.json', environment)


def run_task(api, campaign_id, task_id, runtime_path, directory, deadline, *, context=None, work=None):
    started = time.monotonic()
    process_seconds = 0.0
    publication_seconds = 0.0
    if context is None:
        plan, _ = get_document(api, filename(campaign_id, 'plan'))
        state, _ = get_document(api, filename(campaign_id, 'state'))
    else:
        plan, state = context
    if (state or {}).get('controller_ref', plan['source_ref']) != os.environ['GITHUB_SHA']:
        raise ValueError('Worker source differs from the pinned campaign')
    task = next(task for task in plan['tasks'] if task['id'] == task_id)
    if host_platform.system() != PLATFORMS[campaign_platform(plan)][1]:
        raise ValueError('Worker operating system differs from the pinned campaign')
    runtime = core.read_json(runtime_path)
    program = task['software']
    if runtime.get('source_shas', {}).get(program) != plan['sources']['sources'][program]['sha']:
        raise ValueError('Prepared software differs from the pinned source')
    expected = plan['datasets'][task['size']]['compatibility'].get(program, [])
    if expected and _runtime_selection(runtime, program) not in expected:
        raise ValueError('Prepared compiler or dependencies differ from saved timing samples')
    if runtime.get('availability', {}).get(program, {}).get('status') != 'ready':
        raise ValueError('Prepared solver is unavailable')
    path = filename(campaign_id, 'task', task_id)
    packet_sha = None
    if work is None:
        packet, packet_sha = get_document(api, path)
    else:
        packet = read_task_packet(api, plan, task)
    if packet is None:
        packet = dict(campaign_id=campaign_id, task_id=task_id, source_ref=plan['source_ref'],
                      records={}, environments={}, state='running')
    validate_packet(plan, task, packet)
    load_seconds = time.monotonic() - started
    invocation = f"{os.environ['GITHUB_RUN_ID']}-{task_id}-{time.time_ns()}"
    packet['environments'][invocation] = task_environment(runtime)
    packet['environments'][invocation]['execution_policy'] = execution_policy()
    directory.mkdir(parents=True, exist_ok=True)
    next_publication = time.monotonic() + 120
    publication_complete = False
    def checkpoint(force=False, local_only=False):
        nonlocal packet_sha, next_publication, publication_complete, publication_seconds
        packet['updated_at'] = core.utc_now()
        core.atomic_json(directory / 'checkpoint.json', packet)
        if force or time.monotonic() >= next_publication:
            export_task(plan, task, packet, runtime, directory)
            if local_only:
                return
            publication_started = time.monotonic()
            try:
                if work is None:
                    packet_sha = put_document(api, path, packet, packet_sha)
                else:
                    publish_worker_packet(api, plan, task, packet, work)
                publication_complete = True
            except (PublicationConflict, DocumentConflict, APIError, OSError, TimeoutError) as error:
                publication_complete = False
                print(f"CHECKPOINT_DEFERRED {task['name']} error={type(error).__name__} "
                      f"status={getattr(error, 'status', None)} local_records={len(packet['records'])}", flush=True)
            finally:
                publication_seconds += time.monotonic() - publication_started
            next_publication = time.monotonic() + 120 + random.uniform(0,60)
            print(f"CHECKPOINT {task['name']} recorded={len(packet['records'])}/{len(task['cases'])}", flush=True)
    packet['state'] = 'running'
    timeout = FREQUENCY_TIMEOUT_SECONDS
    checkpoint(True, local_only=work is not None)
    tail = work is not None and work['kind'] == 'timeout_tail'
    if tail:
        cutoff = timeout_basis_frequency(task, packet['records'], work['timeout_basis'])
        selected = set(work['case_ids'])
        if not selected or any(case['frequency_hz'] <= cutoff for case in task['cases']
                               if case['case_id'] in selected):
            raise ValueError('Timeout-tail assignment includes a frequency before the trigger')
    try:
        for case in task['cases']:
            if work is not None and work['kind'] == 'frequency' and case['case_id'] != work['case_id']:
                continue
            if tail and case['case_id'] not in selected:
                continue
            if case['case_id'] in packet['records']:
                continue
            basis = work['timeout_basis'] if tail else None
            if work is None or work['kind'] == 'sweep':
                cutoff = timeout_streak(task, packet['records'])
                if cutoff and case['frequency_hz'] > cutoff['after_frequency_hz']:
                    basis = {key: cutoff[key] for key in ('rule', 'trigger_case_ids')}
            inferred = basis is not None
            if time.time() + (0 if inferred else timeout) + 60 >= deadline:
                packet['state'] = 'budget_paused'
                break
            print(f"START {task['name']} f={case['frequency_hz']/1000:g} kHz "
                  f"recorded={len(packet['records'])}/{len(task['cases'])}", flush=True)
            process_started = time.monotonic()
            result = (dict(status='timeout', prediction_attempted=False, timeout_basis=basis)
                      if inferred else core.measure(case, runtime, directory / 'work' / case['case_id'], timeout))
            if not inferred:
                process_seconds += time.monotonic() - process_started
            if not inferred:
                result['prediction_attempted'] = True
            # A setup failure is not a numerical observation and should stop this worker.
            if result['status'] in ('setup_error', 'setup_timeout', 'setup_unavailable'):
                packet['state'] = 'setup_failed'
                packet['error'] = result['status']
                break
            row = dict(case, **{key: value for key, value in result.items() if key not in case},
                       invocation_id=invocation, source_sha=plan['sources']['sources'][program]['sha'],
                       measurement_id=core.fingerprint([plan['campaign_id'], case['case_id']]),
                       measurement_origin_run_id=int(os.environ['GITHUB_RUN_ID']),
                       allocation_id=invocation, processor=packet['environments'][invocation]['processor'],
                       case_timeout_seconds=timeout,
                       finished_at=core.utc_now())
            packet['records'][case['case_id']] = row
            if work is not None:
                row['measurement_worker_id'] = work['worker_id']
            print(f"DONE {task['name']} f={case['frequency_hz']/1000:g} kHz "
                  f"status={row['status']} solve_seconds={row.get('elapsed_seconds')}", flush=True)
            if work is None or work['kind'] == 'sweep':
                checkpoint()
            else:
                core.atomic_json(directory / 'checkpoint.json', packet)
        else:
            packet['state'] = 'complete' if len(packet['records']) == len(task['cases']) else 'fragment_complete'
        if len(packet['records']) == len(task['cases']):
            packet['state'] = 'complete'
    finally:
        if packet['state'] == 'running':
            packet['state'] = 'interrupted'
        checkpoint(True)
    packet['_publication_complete'] = publication_complete
    packet['_execution_timings'] = dict(load_seconds=load_seconds, process_seconds=process_seconds,
                                        publication_seconds=publication_seconds)
    return packet


def run_worker(api, campaign_id, worker_id, runtime_path, directory, deadline):
    """Keep one serial measurement stream busy across complete spectra."""
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, _ = get_document(api, filename(campaign_id, 'state'))
    if (state or {}).get('controller_ref', plan['source_ref']) != os.environ['GITHUB_SHA']:
        raise ValueError('Worker source differs from the pinned campaign')
    run_id = int(os.environ['GITHUB_RUN_ID'])
    wave = next(item for item in reversed(state['waves']) if item['run_id'] == run_id)
    if not wave.get('queue'):
        return run_task(api, campaign_id, worker_id, runtime_path, directory, deadline,
                        context=(plan, state))
    if worker_id not in wave['workers']:
        raise ValueError('Unknown spectrum worker slot')
    queue, _ = get_document(api, wave['queue'])
    if queue.get('schema_version') == 2:
        return run_frequency_worker(api, plan, state, wave, worker_id, runtime_path, directory, deadline)
    directory.mkdir(parents=True, exist_ok=True)
    bundle_path = directory / 'spectrum-bundle.json'
    bundle = core.read_json(bundle_path) if bundle_path.exists() else dict(
        schema_version=1, campaign_id=campaign_id, worker_id=worker_id, task_ids=[])
    if bundle.get('campaign_id') != campaign_id or bundle.get('worker_id') != worker_id:
        raise ValueError('Local spectrum bundle belongs to another worker')
    tasks = {task['id']: task for task in plan['tasks']}
    minimum_timeout = FREQUENCY_TIMEOUT_SECONDS
    while time.time() + minimum_timeout + 60 < deadline:
        task_id = change_queue(api, campaign_id, run_id, worker_id)
        if task_id is None:
            print(f'QUEUE_EMPTY worker={worker_id}', flush=True)
            return
        if task_id not in wave['tasks']:
            raise ValueError('Claimed spectrum is outside the saved allocation')
        if task_id not in bundle['task_ids']:
            bundle['task_ids'].append(task_id)
            core.atomic_json(bundle_path, bundle)
        task = tasks[task_id]
        print(f'SPECTRUM_CLAIMED worker={worker_id} task={task["name"]}', flush=True)
        packet = run_task(api, campaign_id, task_id, runtime_path, directory / task_id,
                          deadline, context=(plan, state))
        change_queue(api, campaign_id, run_id, worker_id, task_id, packet['state'])
        print(f'SPECTRUM_FINISHED worker={worker_id} task={task["name"]} state={packet["state"]}', flush=True)
        if packet['state'] != 'complete':
            return
    print(f'WORKER_BUDGET_REACHED worker={worker_id}', flush=True)


def idle_owners_finished(api, plan, wave, worker_id, cache):
    """Release idle jobs only after remaining assignment owners have stopped."""
    now = time.monotonic()
    if now < cache.get('next_poll', 0):
        return False
    cache['next_poll'] = now + 60
    workers, names = wave.get('workers'), wave.get('worker_names')
    if (not isinstance(workers, list) or not isinstance(names, list) or
            len(workers) != len(names) or worker_id not in workers or
            any(not isinstance(value, str) for value in workers + names) or
            len(set(workers)) != len(workers) or len(set(names)) != len(names)):
        return False
    try:
        response = api.request('GET', f"/actions/runs/{wave['run_id']}/jobs?per_page=100")
        # Work added during the job lookup takes precedence over idle exit.
        queue, _ = get_document(api, wave['queue'])
    except (APIError, OSError, TimeoutError):
        return False
    if (not isinstance(queue, dict) or queue.get('schema_version') != 2 or
            queue.get('campaign_id') != plan['campaign_id'] or queue.get('run_id') != wave['run_id'] or
            not isinstance(queue.get('owners'), dict) or worker_id not in queue['owners'] or
            not isinstance(queue.get('pending'), list) or not isinstance(queue.get('deferred'), list) or
            queue['pending'] or queue['deferred']):
        return False
    jobs = response.get('jobs') if isinstance(response, dict) else None
    if not isinstance(jobs, list):
        return False
    mapping = dict(zip(workers, names))
    for owner, unit in queue['owners'].items():
        if unit is None:
            continue
        if owner not in mapping:
            return False
        matches = [job for job in jobs if isinstance(job, dict) and job.get('name') == mapping[owner]]
        if len(matches) != 1 or matches[0].get('status') != 'completed':
            return False
    return True


def run_frequency_worker(api, plan, state, wave, worker_id, runtime_path, directory, deadline):
    """Keep each sweep on one worker and accept historical queue assignments."""
    directory.mkdir(parents=True, exist_ok=True)
    tasks = {task['id']: task for task in plan['tasks']}
    minimum_timeout = FREQUENCY_TIMEOUT_SECONDS
    queue_run_id = wave.get('queue_run_id', wave['run_id'])
    idle_cache = {}
    next_work, work_ready = None, False
    while time.time() + minimum_timeout + 60 < deadline:
        iteration_started = time.monotonic()
        try:
            work = next_work if work_ready else change_work_queue(api, plan, queue_run_id, worker_id)
            work_ready = False
        except (PublicationConflict, APIError, OSError, TimeoutError) as error:
            print(f'QUEUE_DEFERRED worker={worker_id} error={type(error).__name__}', flush=True)
            time.sleep(15)
            continue
        if work is None:
            print(f'QUEUE_EMPTY worker={worker_id}', flush=True)
            return
        if work['kind'] == 'wait':
            if wave.get('pool'):
                print(f'POOL_QUEUE_DRAINED worker={worker_id}', flush=True)
                return
            if idle_owners_finished(api, plan, wave, worker_id, idle_cache):
                print(f'IDLE_OWNERS_FINISHED worker={worker_id}', flush=True)
                return
            time.sleep(15)
            continue
        task = tasks[work['task_id']]
        work.update(run_id=wave['run_id'], worker_id=worker_id)
        print(f'WORK_CLAIMED worker={worker_id} kind={work["kind"]} task={task["name"]} '
              f'case={work.get("case_id", "sweep")}', flush=True)
        packet = run_task(api, plan['campaign_id'], task['id'], runtime_path, directory / task['id'],
                          deadline, context=(plan, state), work=work)
        # A successful atomic publication confirms durable storage of every new observation.
        if not packet.pop('_publication_complete', False):
            print(f'PUBLICATION_PENDING worker={worker_id} task={task["name"]}', flush=True)
            return
        timings = packet.pop('_execution_timings', {})
        completion = packet
        stopping = packet['state'] in ('budget_paused', 'setup_failed', 'interrupted')
        try:
            next_work = change_work_queue(api, plan, queue_run_id, worker_id, work['id'], completion,
                                           claim_next=not stopping)
            work_ready = True
        except (PublicationConflict, APIError, OSError, TimeoutError) as error:
            print(f'COMPLETION_DEFERRED worker={worker_id} error={type(error).__name__}', flush=True)
            return
        print(f'WORK_FINISHED worker={worker_id} kind={work["kind"]} task={task["name"]} '
              f'state={completion["state"]}', flush=True)
        print(f'WORK_TIMING worker={worker_id} total_seconds={time.monotonic()-iteration_started:.3f} '
              + ' '.join(f'{key}={value:.3f}' for key, value in timings.items()), flush=True)
        if stopping:
            return
    print(f'WORKER_BUDGET_REACHED worker={worker_id}', flush=True)


def attach_latest_references(api, rows, size):
    # Reference publishers update this shared curve file without modifying timing observations.
    from reference_curves import _validated
    curves = {}
    try:
        value = api.request('GET', f'/contents/qualified-reference-curves.json?ref={BRANCH}')
        if value.get('encoding') != 'base64' or not value.get('content'):
            value = api.request('GET', '/git/blobs/' + value['sha'])
        content = json.loads(base64.b64decode(''.join(value['content'].split())))
        curves = _validated(content['reference_curves'])
    except APIError as error:
        if error.status != 404:
            raise
    for row in rows:
        reference = curves.get(row['physical_id'])
        if reference is not None:
            if row.get('reference_id') != reference['reference_id']:
                for key in REFERENCE_FIELDS:
                    row.pop(key, None)
            row.update(reference_id=reference['reference_id'], reference_source_sha=reference['source_sha'],
                       reference_ts_db=reference['ts_db'], reference_uncertainty_db=reference['uncertainty_db'])
            if row['status'] == 'ok':
                row['delta_ts_db'] = row['ts_db'] - reference['ts_db']
    return rows


def publish_datasets(api, plan, packets, directory, finished=False, *, run_id=None):
    platform = campaign_platform(plan)
    for size, dataset in plan['datasets'].items():
        tasks = [task for task in plan['tasks'] if task['size'] == size]
        rows = merge_dataset(dataset, tasks, packets)
        rows = attach_latest_references(api, rows, size)
        # The shared view counts all retained software, while task progress describes this request.
        for row in rows:
            row.pop('measurement_role', None)
            row.pop('retained_from_run_id', None)
        completed = sum(packets.get(task['id'], {}).get('state') == 'complete' for task in tasks)
        payload = compact_payload(api.repository, platform, int(plan['campaign_id']), 1, rows,
                                  state='completed' if finished and completed == len(tasks) else 'running')
        payload.update(parallel_campaign_id=plan['campaign_id'], parallel_tasks=dict(planned=len(tasks),
            completed=completed, pending=len(tasks)-completed), timing_statistic='arithmetic_mean')
        payload['active_execution_policy'] = execution_policy()
        artifact_run = run_id or os.environ['GITHUB_RUN_ID']
        payload['artifact_url'] = f"https://github.com/{api.repository}/actions/runs/{artifact_run}#artifacts"
        current, _ = existing_snapshot(api, platform, snapshot_filename(platform, float(size)))
        if current and int(current['run_id']) > int(plan['campaign_id']):
            raise ValueError('A newer campaign owns this benchmark. Refusing to replace its measurements')
        target = directory / size
        target.mkdir(parents=True, exist_ok=True)
        core.atomic_json(target / 'results.json', rows)
        core.atomic_json(target / 'parallel-plan.json', dict(campaign_id=plan['campaign_id'],
            platform=platform, source_ref=plan['source_ref'], configuration=dataset['configuration'],
            active_execution_policy=execution_policy(), sources=plan['sources']))
        if dataset.get('archived_rows'):
            core.atomic_json(target / 'archived-results.json', dict(platform=platform,
                reason='Superseded selected software measurements', rows=dataset['archived_rows']))
        from report import render_report
        render_report(target / 'index.html', rows,
                      dict(state='complete' if completed == len(tasks) else 'running',
                           planned=len(rows), recorded=sum(row['status'] != 'pending' for row in rows)))
        publish(api, payload)
        print(f"PUBLISHED a={size} m tasks={completed}/{len(tasks)} observations="
              f"{sum(row['status'] != 'pending' for row in rows)}", flush=True)


def monitor(api, campaign_id, directory, *, run_id=None, deadline=None):
    run_id = int(identifier(run_id or os.environ['GITHUB_RUN_ID']))
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, _ = get_document(api, filename(campaign_id, 'state'))
    wave = next(item for item in reversed(state['waves']) if item['run_id'] == run_id)
    names = set(wave.get('worker_names') or
                [task['name'] for task in plan['tasks'] if task['id'] in wave['tasks']])
    packets = {}
    next_update = 0
    next_job_poll = 0
    done = False
    pool_cache = {}
    while deadline is None or time.monotonic() < deadline:
        try:
            state, _ = get_document(api, filename(campaign_id, 'state'))
        except (APIError, OSError, TimeoutError, PublicationConflict) as error:
            print(f'POOL_STATE_DEFERRED error={error}', flush=True)
            time.sleep(30)
            continue
        if state.get('worker_pool'):
            try:
                if not pool_publisher(api, state, run_id):
                    print(f'POOL_PUBLISHER_ALREADY_ACTIVE run={run_id}', flush=True)
                    return True
                replenish_pool(api, plan, cache=pool_cache)
            except (APIError, OSError, TimeoutError, PublicationConflict) as error:
                print(f'POOL_REFILL_DEFERRED error={error}', flush=True)
        if time.time() >= next_job_poll:
            try:
                jobs = api.request('GET', f"/actions/runs/{run_id}/jobs?per_page=100")['jobs']
                workers = [job for job in jobs if job['name'] in names]
                done = len(workers) == len(names) and all(job['status'] == 'completed' for job in workers)
                if state.get('worker_pool'):
                    current, _ = get_document(api, filename(campaign_id, 'state'))
                    queue, _ = get_document(api, current['worker_pool']['queue'])
                    done = (not any(item['active'] for item in pool_allocations(api, current))
                            and (not plan['automatic_continue'] or (
                                not queue['pending'] and not queue['deferred']
                                and not any(queue['owners'].values()) and not queue.get('pool_dispatch'))))
            except APIError as error:
                if error.status not in (403,429,500,502,503,504):
                    raise
                next_job_poll = max(time.time()+60,getattr(error,'rate_reset',0)+2,
                    time.time()+getattr(error,'retry_after',0))
                print(f'JOB_STATUS_DEFERRED status={error.status} retry_epoch={next_job_poll:.0f}',flush=True)
        if time.monotonic() >= next_update or done:
            try:
                packets = read_packets(api, plan, cache=packets)
                if not state.get('worker_pool'):
                    done = done or all(packets.get(task_id,{}).get('state') == 'complete' for task_id in wave['tasks'])
                if done and state.get('worker_pool'):
                    finish_pool(api, plan, wave, directory)
                else:
                    publish_datasets(api, plan, packets, directory, run_id=run_id)
            except (PublicationConflict, APIError, OSError, TimeoutError) as error:
                print(f'PUBLICATION_DEFERRED {error}. Retrying in 30 seconds', flush=True)
                next_update = time.monotonic() + 30
            else:
                if done:
                    return True
                next_update = time.monotonic() + 180
        time.sleep(30)
    return False


def relay_progress(api, run_id, directory, *, campaign_id='', budget_seconds=18000):
    """Replace an expired publisher while retaining the original calculation jobs."""
    run_id = int(identifier(run_id))
    if not 60 <= budget_seconds <= 18000:
        raise ValueError('Progress relay budget must be between 60 and 18000 seconds')
    run = api.request('GET', f'/actions/runs/{run_id}')
    if (run.get('event') != 'workflow_dispatch' or
            run.get('head_repository', {}).get('full_name') != api.repository or
            run.get('path', '').split('@')[0] != '.github/workflows/parallel-spectra.yml'):
        raise ValueError('Progress relay requires a benchmark run from the same repository')
    match = re.fullmatch(r'Continue spectra ([1-9][0-9]*)', run.get('display_title', ''))
    expected_campaign = match[1] if match else str(run_id)
    campaign_id = identifier(campaign_id or expected_campaign)
    if campaign_id != expected_campaign:
        raise ValueError('Progress relay campaign differs from the benchmark run')
    deadline = time.monotonic() + budget_seconds
    while time.monotonic() < deadline:
        try:
            state, _ = get_document(api, filename(campaign_id, 'state'))
            jobs = api.request('GET', f'/actions/runs/{run_id}/jobs?per_page=100')['jobs']
        except (APIError, OSError, TimeoutError) as error:
            if isinstance(error, APIError) and error.status not in (403,429,500,502,503,504):
                raise
            print(f'PROGRESS_RELAY_STATUS_DEFERRED run={run_id} error={error}', flush=True)
            time.sleep(30)
            continue
        if state and not state.get('worker_pool') and state.get('waves') and state['waves'][-1]['run_id'] != run_id:
            if any(wave['run_id'] == run_id for wave in state['waves']):
                print(f'PROGRESS_RELAY_SUPERSEDED run={run_id}', flush=True)
                return
        planners = [job for job in jobs if job['name'] == 'plan']
        if any(job['status'] == 'completed' and job['conclusion'] != 'success' for job in planners):
            print(f'PROGRESS_RELAY_NO_PLAN run={run_id}', flush=True)
            return
        primary = [job for job in jobs if job['name'] == 'progress']
        if len(primary) == 1 and primary[0]['status'] == 'completed':
            if primary[0]['conclusion'] in ('success', 'skipped'):
                print(f'PROGRESS_RELAY_PRIMARY_COMPLETE run={run_id}', flush=True)
                return
            if not state or not any(wave['run_id'] == run_id for wave in state.get('waves', [])):
                raise ValueError('Progress relay has no saved benchmark allocation')
            print(f'PROGRESS_RELAY_TAKEOVER campaign={campaign_id} run={run_id}', flush=True)
            if monitor(api, campaign_id, directory, run_id=run_id, deadline=deadline):
                print(f'PROGRESS_RELAY_COMPLETE run={run_id}', flush=True)
                return
            break
        print(f'PROGRESS_RELAY_WAIT run={run_id} primary=active_or_pending', flush=True)
        time.sleep(60)
    api.request('POST', '/actions/workflows/publish-progress.yml/dispatches',
                dict(ref='main', inputs=dict(run_id=str(run_id), campaign_id=campaign_id)))
    print(f'PROGRESS_RELAY_RENEWED campaign={campaign_id} run={run_id}', flush=True)


def wake_ordered_queue(api, campaign_id):
    """Wake the saved sequence after complete publication without retrying dispatch."""
    try:
        workflow = api.request('GET', '/actions/workflows/queue.yml')
        if not isinstance(workflow, dict):
            raise ValueError('Invalid ordered queue workflow metadata')
        if workflow.get('state') != 'active':
            print(f'QUEUE_HANDOFF_SKIPPED campaign={campaign_id} workflow_inactive', flush=True)
            return
        api.request('POST', '/actions/workflows/queue.yml/dispatches',
                    dict(ref='main', inputs=dict(wait_seconds='120')))
    except (APIError, OSError, TimeoutError, ValueError) as error:
        print(f'::warning::QUEUE_HANDOFF_UNCONFIRMED campaign={campaign_id} '
              f'error={type(error).__name__} status={getattr(error, "status", None)}. '
              'Published results remain complete. Dispatch was not retried.', flush=True)
        return
    print(f'QUEUE_HANDOFF_DISPATCHED campaign={campaign_id}', flush=True)


def pool_publisher(api, state, run_id):
    """Lease one campaign publisher across overlapping worker allocations."""
    path = state['worker_pool']['queue']
    queue, sha = get_document(api, path)
    lease = queue.get('pool_publisher', {})
    if lease.get('run_id') != run_id and lease.get('expires_epoch', 0) > time.time():
        return False
    queue['pool_publisher'] = dict(run_id=run_id, expires_epoch=time.time() + 600)
    try:
        put_document(api, path, queue, sha)
    except DocumentConflict as error:
        raise PublicationConflict('Publisher lease changed concurrently') from error
    return True


def finish_pool(api, plan, wave, directory):
    """Recover finished workers without waiting for other allocations."""
    campaign_id = plan['campaign_id']
    initial, _ = get_document(api, filename(campaign_id, 'state'))
    live = pool_allocations(api, initial)
    if any(item['legacy_finishing'] for item in live):
        print('POOL_FINISH_DEFERRED existing final publication is active', flush=True)
        return
    recover_pool(api, plan, initial, live)
    packets = read_packets(api, plan)
    for attempt in range(32):
        state, state_sha = get_document(api, filename(campaign_id, 'state'))
        path = state['worker_pool']['queue']
        queue, queue_sha = get_document(api, path)
        state['completed'] = sorted(set(state['completed']) | {
            task for task, packet in packets.items() if packet.get('state') == 'complete'})
        unfinished = {task['id'] for task in plan['tasks']} - set(state['completed']) - set(state['blocked'])
        live = pool_allocations(api, state)
        if unfinished or any(item['active'] for item in live):
            state['status'] = 'active' if plan['automatic_continue'] else 'paused'
        else:
            state['status'] = 'blocked' if state['blocked'] else 'complete'
        state['updated_at'] = core.utc_now()
        try:
            put_documents(api, [(path, queue, queue_sha), (filename(campaign_id,'state'),state,state_sha)])
            break
        except (DocumentConflict, PublicationConflict):
            if attempt == 31:
                raise
    if state['status'] == 'active':
        replenish_pool(api, plan)
    publish_datasets(api, plan, packets, directory, finished=state['status'] == 'complete')
    write_outputs(dict(pending=len(unfinished), completed=len(state['completed']), blocked=len(state['blocked']),
                       state=state['status']))
    if state['status'] == 'complete':
        # A single finalizer hands the completed campaign back to the ordered sequence.
        latest, sha = get_document(api, filename(campaign_id,'state'))
        if latest.get('pool_handoff_done'):
            return
        latest['pool_handoff_done'] = core.utc_now()
        try:
            put_document(api, filename(campaign_id,'state'), latest, sha)
        except (DocumentConflict, PublicationConflict):
            return
        following = latest.get('next_platform_campaign')
        if following:
            inputs = dict(campaign_id=identifier(following['campaign_id']), platform=campaign_platform(following),
                          max_parallel='16', rolling_workers='true')
            api.request('POST', f'/actions/workflows/{WORKFLOW}/dispatches',
                        dict(ref=latest.get('controller_tag',f'spectra-{campaign_id}'),inputs=inputs))
        else:
            wake_ordered_queue(api, campaign_id)


def finish(api, campaign_id, directory):
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, state_sha = get_document(api, filename(campaign_id, 'state'))
    wave = next(item for item in reversed(state['waves']) if item['run_id'] == int(os.environ['GITHUB_RUN_ID']))
    if state.get('worker_pool') and wave.get('pool'):
        return finish_pool(api, plan, wave, directory)
    recover_artifacts(api, plan, wave)
    packets = read_packets(api, plan)
    attempted = wave['tasks']
    if wave.get('queue'):
        queue, _ = get_document(api, wave['queue'])
        attempted = (sorted({queue['units'][identity]['task_id'] for identity, owner in queue['claims'].items()
                               if not wave.get('pool') or owner in wave['workers']})
                     if queue.get('schema_version') == 2 else list(queue['claims']))
        if set(attempted) - set(wave['tasks']):
            raise ValueError('Spectrum queue contains claims outside the saved selection')
    for task_id in attempted:
        packet = packets.get(task_id, {})
        previous = state['attempts'].get(task_id, dict(recorded=0, stalled=0))
        count = len(packet.get('records', {}))
        stalled = 0 if count > previous['recorded'] else previous['stalled'] + 1
        state['attempts'][task_id] = dict(recorded=count, stalled=stalled)
        if stalled >= 3 and packet.get('state') != 'complete' and task_id not in state['blocked']:
            state['blocked'].append(task_id)
    state['completed'] = sorted(task_id for task_id, packet in packets.items() if packet.get('state') == 'complete')
    pending = [task for task in plan['tasks'] if task['id'] not in set(state['completed']) | set(state['blocked'])]
    work_status = ('active' if plan['automatic_continue'] else 'paused') if pending else (
        'blocked' if state['blocked'] else 'complete')
    state['status'] = work_status if pending else 'publishing'
    state['updated_at'] = core.utc_now()
    state_sha = put_document(api, filename(campaign_id, 'state'), state, state_sha)
    try:
        publish_datasets(api, plan, packets, directory, finished=True)
    except PublicationConflict as error:
        state['publication_pending'] = True
        state['publication_attempts'] = state.get('publication_attempts', 0) + 1
        if not pending and state['publication_attempts'] >= 3:
            state['status'] = 'publication_blocked'
        print(f'PUBLICATION_DEFERRED {error}. Saved observations remain available', flush=True)
    else:
        state.update(status=work_status, publication_pending=False, publication_attempts=0)
    put_document(api, filename(campaign_id, 'state'), state, state_sha)
    write_outputs(dict(pending=len(pending), completed=len(state['completed']), blocked=len(state['blocked']),
                       state=state['status']))
    if (pending or state['status'] == 'publishing') and plan['automatic_continue']:
        continuation = dict(campaign_id=campaign_id)
        if 'platform' in plan:
            continuation['platform'] = campaign_platform(plan)
        if 'max_parallel' in state:
            continuation['max_parallel'] = str(state['max_parallel'])
        api.request('POST', f'/actions/workflows/{WORKFLOW}/dispatches',
                    dict(ref=state.get('controller_tag', f'spectra-{campaign_id}'), inputs=continuation))
        print(f'CONTINUATION_DISPATCHED campaign={campaign_id} unfinished_tasks={len(pending)}', flush=True)
    if state['status'] == 'publication_blocked':
        raise PublicationConflict('All timing tasks are saved, but final dashboard publication requires recovery')
    following = state.get('next_platform_campaign')
    if state['status'] == 'complete' and following and not state.get('next_platform_dispatched'):
        next_inputs = dict(campaign_id=identifier(following['campaign_id']),
            platform=campaign_platform(following), max_parallel='16')
        api.request('POST', f'/actions/workflows/{WORKFLOW}/dispatches',
            dict(ref=state.get('controller_tag', f'spectra-{campaign_id}'), inputs=next_inputs))
        state['next_platform_dispatched'] = core.utc_now()
        put_document(api, filename(campaign_id, 'state'), state)
        print(f"NEXT_PLATFORM_DISPATCHED campaign={next_inputs['campaign_id']} platform={next_inputs['platform']}", flush=True)
    elif state['status'] == 'complete' and not following:
        wake_ordered_queue(api, campaign_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'worker', 'monitor', 'relay', 'finish'))
    parser.add_argument('--campaign-id', default='')
    parser.add_argument('--task-id')
    parser.add_argument('--run-id')
    parser.add_argument('--budget-seconds', type=int, default=18000)
    parser.add_argument('--runtime', type=Path)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--deadline', type=float, default=float('inf'))
    args = parser.parse_args()
    api = LiveGitHub(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'], timeout_seconds=90)
    api.wait_on_rate_limit = args.action not in ('monitor', 'relay')
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        api.data_store = GitDataStore(core.ROOT)
    if args.action == 'plan':
        plan_campaign(api, json.loads(os.environ['SPECTRA_INPUTS']), args.directory)
    elif args.action == 'worker':
        run_worker(api, args.campaign_id, args.task_id, args.runtime, args.directory, args.deadline)
    elif args.action == 'relay':
        relay_progress(api, args.run_id, args.directory, campaign_id=args.campaign_id,
                       budget_seconds=args.budget_seconds)
    elif args.action == 'monitor':
        monitor(api, args.campaign_id, args.directory)
    else:
        finish(api, args.campaign_id, args.directory)


if __name__ == '__main__':
    main()


"""Run independent, serial spectra and merge immutable timing observations."""
from __future__ import annotations

import argparse
import base64
import copy
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
DOCUMENT_PART_BYTES = 4 * 1024 * 1024
SLOW_FREQUENCY_SECONDS = 60.0
FREQUENCY_TIMEOUT_SECONDS = 120
TASK_FIELDS = ('software', 'semimajor_m', 'aspect_ratio', 'boundary', 'incidence_deg', 'repetition')
REFERENCE_FIELDS = ('reference_id', 'reference_source_sha', 'reference_ts_db',
                    'reference_qualification', 'reference_fingerprint', 'reference_uncertainty_db')
CONTROLLER_FILES = {'src/parallel_campaign.py', 'src/publish_progress.py', 'src/dataset.py', 'src/report.py',
                    'tests/test_parallel_campaign.py', 'tests/test_publish_progress.py'}


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
        result = subprocess.run(['git', *arguments], cwd=self.root, input=data,
            capture_output=True, env=env, timeout=180)
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
                return dict(sha=self.git('write-tree',env=env).stdout.decode().strip())
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


def pack(value):
    raw = json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('Spectrum state exceeds its size limit')
    return dict(encoding='gzip-base64', sha256=hashlib.sha256(raw).hexdigest(),
                data=base64.b64encode(gzip.compress(raw, mtime=0)).decode())


def unpack(value):
    if value.get('encoding') != 'gzip-base64':
        raise ValueError('Unrecognized spectrum state encoding')
    with gzip.GzipFile(fileobj=io.BytesIO(base64.b64decode(value['data'], validate=True))) as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != value['sha256']:
        raise ValueError('Spectrum state content does not match its digest')
    return json.loads(raw)


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


def put_document(api, path, value, previous_sha=None, *, immutable=False):
    previous, actual_sha = get_document(api, path)
    if immutable and previous is not None:
        if previous != value:
            raise ValueError('Refusing to replace an existing spectrum plan')
        return actual_sha
    if previous_sha is not None and actual_sha != previous_sha:
        if previous == value:
            return actual_sha
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


def slow_frequency(row):
    """Classify scheduling cost without changing the numerical timeout."""
    elapsed = row.get('elapsed_seconds')
    return row.get('status') == 'timeout' or (
        isinstance(elapsed, (int, float)) and math.isfinite(elapsed)
        and elapsed >= SLOW_FREQUENCY_SECONDS)


def execution_policy():
    """Report the active frequency budget and scheduling threshold."""
    return dict(case_timeout_seconds=FREQUENCY_TIMEOUT_SECONDS,
                slow_frequency_seconds=SLOW_FREQUENCY_SECONDS)


def scheduling_queue(plan, candidates, packets, run_id, parallel):
    """Prioritize whole sweeps and distribute previously expensive tails."""
    queue = dict(schema_version=2, campaign_id=plan['campaign_id'], run_id=run_id,
                 slow_frequency_seconds=SLOW_FREQUENCY_SECONDS,
                 pending=[], deferred=[], units={}, owners={}, claims={}, finished={})
    resumed_sweeps = []
    for task in candidates:
        packet = packets.get(task['id'], {})
        records = packet.get('records', {})
        remaining = [case['case_id'] for case in task['cases'] if case['case_id'] not in records]
        if not remaining:
            continue
        if packet.get('slow_sweep') or any(slow_frequency(row) for row in records.values()):
            for case_id in remaining:
                queue['units'][case_id] = dict(kind='frequency', task_id=task['id'], case_id=case_id)
                queue['deferred'].append(case_id)
        else:
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


def change_work_queue(api, plan, run_id, worker_id, completed_unit=None, packet=None):
    """Atomically claim one sweep or one frequency, with fresh sweeps first."""
    path = queue_filename(plan['campaign_id'], run_id)
    tasks = {task['id']: task for task in plan['tasks']}
    for attempt in range(32):
        queue, sha = get_document(api, path)
        if (not queue or queue.get('schema_version') != 2 or
                queue.get('campaign_id') != plan['campaign_id'] or
                queue.get('run_id') != int(run_id) or worker_id not in queue['owners']):
            raise ValueError('Worker does not belong to the frequency queue')
        if completed_unit is None:
            selected = queue['owners'][worker_id]
            if selected is not None:
                return dict(queue['units'][selected], id=selected)
            waiting = queue['pending'] or queue['deferred']
            if not waiting:
                # Active sweeps can still release tails into the secondary queue.
                return dict(kind='wait') if any(queue['owners'].values()) else None
            selected = waiting.pop(0)
            if selected in queue['claims']:
                raise ValueError('Frequency queue contains an already claimed assignment')
            queue['claims'][selected] = worker_id
            queue['owners'][worker_id] = selected
        else:
            selected = completed_unit
            if queue['claims'].get(selected) != worker_id:
                raise ValueError('Frequency assignment belongs to another worker')
            if selected in queue['finished']:
                return dict(queue['units'][selected], id=selected)
            if queue['owners'][worker_id] != selected or packet is None:
                raise ValueError('Frequency completion differs from the active assignment')
            unit = queue['units'][selected]
            task = tasks[unit['task_id']]
            validate_packet(plan, task, packet)
            outcome = packet['state']
            if unit['kind'] == 'frequency' and outcome not in ('budget_paused', 'setup_failed'):
                if unit['case_id'] not in packet['records']:
                    raise ValueError('Frequency assignment has no saved observation')
            if unit['kind'] == 'sweep' and packet.get('slow_sweep'):
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
        queue['updated_at'] = core.utc_now()
        try:
            put_document(api, path, queue, sha)
            return dict(queue['units'][selected], id=selected)
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


def read_packets(api, plan, *, cache=None):
    cache = {} if cache is None else cache
    tasks = plan['tasks']
    _, _, tree = _progress_tree(api)
    def read(task):
        name = filename(plan['campaign_id'], 'task', task['id'])
        entry = tree.get(name)
        if entry is None:
            return task['id'], None
        if cache.get(task['id'], {}).get('_blob_sha') == entry['sha']:
            return task['id'], cache[task['id']]
        blob = api.request('GET', '/git/blobs/' + entry['sha'])
        raw = base64.b64decode(''.join(blob['content'].split()), validate=True)
        if hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest() != entry['sha']:
            raise ValueError('Worker packet does not match its Git digest')
        packet = _unpack_document(api, raw)
        validate_packet(plan, task, packet)
        packet['_blob_sha'] = entry['sha']
        return task['id'], packet
    # Downloads do not execute numerical solves.
    with ThreadPoolExecutor(max_workers=4) as pool:
        for task_id, packet in pool.map(read, tasks):
            if packet is not None:
                cache[task_id] = packet
    return cache


def validate_packet(plan, task, packet):
    if (packet.get('campaign_id') != plan['campaign_id'] or packet.get('task_id') != task['id']
            or packet.get('source_ref') != plan['source_ref']):
        raise ValueError('Worker packet belongs to a different campaign or task')
    cases = {case['case_id']: case for case in task['cases']}
    records = packet.get('records')
    if not isinstance(records, dict) or records.keys() - cases.keys():
        raise ValueError('Worker packet contains unexpected physical cases')
    for identity, row in records.items():
        case = cases[identity]
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
    if packet.get('state') == 'complete' and records.keys() != cases.keys():
        raise ValueError('Incomplete spectrum was marked complete')


def recover_artifacts(api, plan, wave):
    """Recover final local checkpoints when a worker lost its publication connection."""
    tasks = {task['id']: task for task in plan['tasks'] if task['id'] in wave['tasks']}
    queue = get_document(api, wave['queue'])[0] if wave.get('queue') else None
    frequency_queue = queue is not None and queue.get('schema_version') == 2
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
            current, sha = get_document(api, path)
            if frequency_queue:
                # Shared snapshots may include records published by other owners.
                owned = set()
                for unit_id, owner in queue['claims'].items():
                    unit = queue['units'][unit_id]
                    if owner != match[1] or unit['task_id'] != task_id:
                        continue
                    owned.update([unit['case_id']] if unit['kind'] == 'frequency' else
                                 [case['case_id'] for case in task['cases']])
                if not owned:
                    raise ValueError('Recovery artifact has no claimed frequency or sweep')
                for identity, row in saved.get('records', {}).items():
                    if identity not in owned and (current or {}).get('records', {}).get(identity) != row:
                        raise ValueError('Recovery observation belongs to another frequency owner')
                merge_task_packet(api, plan, task, saved)
            else:
                restore_packet(api, plan, task, saved, current, sha)


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
    if mode not in ('extend', 'refresh', 'repeat'):
        raise ValueError('Mode must be extend, refresh or repeat')
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
            original[row['case_id']] = row
    pending = []
    for case in cases:
        row = copy.deepcopy(original.get(case['case_id'], dict(case, status='pending')))
        row.pop('measurement_role', None)
        row.pop('retained_from_run_id', None)
        if case['case_id'] not in original:
            row.update(reference_fields.get(case['physical_id'], {}))
        rows.append(row)
        if row['status'] == 'pending' and (mode != 'repeat' or case['repetition'] >= first_new):
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


def select_controller(api, plan, state):
    """Permit a recorded orchestration repair while retaining the original numerical plan."""
    current = os.environ['GITHUB_SHA']
    previous = state.get('controller_ref', plan['source_ref'])
    if current == previous:
        return
    comparison = api.request('GET', f'/compare/{previous}...{current}')
    files = comparison.get('files', [])
    if (comparison.get('status') != 'ahead' or not files or len(files) >= 300 or
            any(item['filename'] not in CONTROLLER_FILES or item.get('status') == 'renamed' for item in files)):
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


def plan_campaign(api, inputs, directory):
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
    packets = read_packets(api, plan)
    if state.get('publish_recovered_before_resume'):
        publish_datasets(api, plan, packets, directory)
        state.pop('publish_recovered_before_resume')
        state.setdefault('recovery_publications', []).append(dict(
            run_id=int(os.environ['GITHUB_RUN_ID']), published_at=core.utc_now(),
            observations=sum(len(packet.get('records', {})) for packet in packets.values())))
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
    packet, packet_sha = get_document(api, path)
    if packet is None:
        packet = dict(campaign_id=campaign_id, task_id=task_id, source_ref=plan['source_ref'],
                      records={}, environments={}, state='running')
    validate_packet(plan, task, packet)
    invocation = f"{os.environ['GITHUB_RUN_ID']}-{task_id}-{time.time_ns()}"
    packet['environments'][invocation] = task_environment(runtime)
    packet['environments'][invocation]['execution_policy'] = execution_policy()
    directory.mkdir(parents=True, exist_ok=True)
    next_publication = 0.0
    def checkpoint(force=False):
        nonlocal packet_sha, next_publication
        packet['updated_at'] = core.utc_now()
        core.atomic_json(directory / 'checkpoint.json', packet)
        if force or time.monotonic() >= next_publication:
            export_task(plan, task, packet, runtime, directory)
            try:
                if work is None:
                    packet_sha = put_document(api, path, packet, packet_sha)
                else:
                    merge_task_packet(api, plan, task, packet)
            except (APIError, OSError, TimeoutError) as error:
                print(f"CHECKPOINT_DEFERRED {task['name']} error={type(error).__name__} "
                      f"status={getattr(error, 'status', None)} local_records={len(packet['records'])}", flush=True)
            next_publication = time.monotonic() + 120 + random.uniform(0,60)
            print(f"CHECKPOINT {task['name']} recorded={len(packet['records'])}/{len(task['cases'])}", flush=True)
    packet['state'] = 'running'
    timeout = FREQUENCY_TIMEOUT_SECONDS
    checkpoint(True)
    try:
        for case in task['cases']:
            if work is not None and work['kind'] == 'frequency' and case['case_id'] != work['case_id']:
                continue
            if case['case_id'] in packet['records']:
                continue
            if time.time() + timeout + 60 >= deadline:
                packet['state'] = 'budget_paused'
                break
            print(f"START {task['name']} f={case['frequency_hz']/1000:g} kHz "
                  f"recorded={len(packet['records'])}/{len(task['cases'])}", flush=True)
            result = core.measure(case, runtime, directory / 'work' / case['case_id'], timeout)
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
            print(f"DONE {task['name']} f={case['frequency_hz']/1000:g} kHz "
                  f"status={row['status']} solve_seconds={row.get('elapsed_seconds')}", flush=True)
            checkpoint(len(packet['records']) == 1)
            if work is not None and work['kind'] == 'sweep' and slow_frequency(row):
                packet['slow_sweep'] = True
                packet['state'] = 'split_pending'
                print(f'SWEEP_YIELDED task={task["name"]} threshold_seconds={SLOW_FREQUENCY_SECONDS:g} '
                      f'recorded={len(packet["records"])}/{len(task["cases"])}', flush=True)
                break
        else:
            packet['state'] = 'complete' if len(packet['records']) == len(task['cases']) else 'fragment_complete'
        if len(packet['records']) == len(task['cases']):
            packet['state'] = 'complete'
    finally:
        if packet['state'] == 'running':
            packet['state'] = 'interrupted'
        checkpoint(True)
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
    """Run fresh sweeps before individual frequencies from expensive sweeps."""
    directory.mkdir(parents=True, exist_ok=True)
    tasks = {task['id']: task for task in plan['tasks']}
    minimum_timeout = FREQUENCY_TIMEOUT_SECONDS
    idle_cache = {}
    while time.time() + minimum_timeout + 60 < deadline:
        work = change_work_queue(api, plan, wave['run_id'], worker_id)
        if work is None:
            print(f'QUEUE_EMPTY worker={worker_id}', flush=True)
            return
        if work['kind'] == 'wait':
            if idle_owners_finished(api, plan, wave, worker_id, idle_cache):
                print(f'IDLE_OWNERS_FINISHED worker={worker_id}', flush=True)
                return
            time.sleep(15)
            continue
        task = tasks[work['task_id']]
        print(f'WORK_CLAIMED worker={worker_id} kind={work["kind"]} task={task["name"]} '
              f'case={work.get("case_id", "sweep")}', flush=True)
        packet = run_task(api, plan['campaign_id'], task['id'], runtime_path, directory / task['id'],
                          deadline, context=(plan, state), work=work)
        # Release work only after every local observation has durable storage.
        saved, _ = get_document(api, filename(plan['campaign_id'], 'task', task['id']))
        if saved is None or any(saved.get('records', {}).get(identity) != row
                                for identity, row in packet['records'].items()):
            print(f'PUBLICATION_PENDING worker={worker_id} task={task["name"]}', flush=True)
            return
        if packet.get('slow_sweep') and not saved.get('slow_sweep'):
            raise ValueError('Slow sweep classification has not been saved')
        completion = dict(saved, state=packet['state'])
        if len(saved['records']) == len(task['cases']):
            completion['state'] = 'complete'
        change_work_queue(api, plan, wave['run_id'], worker_id, work['id'], completion)
        print(f'WORK_FINISHED worker={worker_id} kind={work["kind"]} task={task["name"]} '
              f'state={completion["state"]}', flush=True)
        if packet['state'] in ('budget_paused', 'setup_failed', 'interrupted'):
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


def publish_datasets(api, plan, packets, directory, finished=False):
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
        payload['artifact_url'] = f"https://github.com/{api.repository}/actions/runs/{os.environ['GITHUB_RUN_ID']}#artifacts"
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


def monitor(api, campaign_id, directory):
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, _ = get_document(api, filename(campaign_id, 'state'))
    wave = next(item for item in reversed(state['waves']) if item['run_id'] == int(os.environ['GITHUB_RUN_ID']))
    names = set(wave.get('worker_names') or
                [task['name'] for task in plan['tasks'] if task['id'] in wave['tasks']])
    packets = {}
    next_update = 0
    next_job_poll = 0
    done = False
    while True:
        if time.time() >= next_job_poll:
            try:
                jobs = api.request('GET', f"/actions/runs/{os.environ['GITHUB_RUN_ID']}/jobs?per_page=100")['jobs']
                workers = [job for job in jobs if job['name'] in names]
                done = len(workers) == len(names) and all(job['status'] == 'completed' for job in workers)
            except APIError as error:
                if error.status not in (403,429,500,502,503,504):
                    raise
                next_job_poll = max(time.time()+60,getattr(error,'rate_reset',0)+2,
                    time.time()+getattr(error,'retry_after',0))
                print(f'JOB_STATUS_DEFERRED status={error.status} retry_epoch={next_job_poll:.0f}',flush=True)
        if time.monotonic() >= next_update or done:
            packets = read_packets(api, plan, cache=packets)
            done = done or all(packets.get(task_id,{}).get('state') == 'complete' for task_id in wave['tasks'])
            try:
                publish_datasets(api, plan, packets, directory)
            except (PublicationConflict, APIError, OSError, TimeoutError) as error:
                print(f'PUBLICATION_DEFERRED {error}. Retrying in 30 seconds', flush=True)
                next_update = time.monotonic() + 30
            else:
                next_update = time.monotonic() + 180
        if done:
            return
        time.sleep(30)


def finish(api, campaign_id, directory):
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, state_sha = get_document(api, filename(campaign_id, 'state'))
    wave = next(item for item in reversed(state['waves']) if item['run_id'] == int(os.environ['GITHUB_RUN_ID']))
    recover_artifacts(api, plan, wave)
    packets = read_packets(api, plan)
    attempted = wave['tasks']
    if wave.get('queue'):
        queue, _ = get_document(api, wave['queue'])
        attempted = (sorted({queue['units'][identity]['task_id'] for identity in queue['claims']})
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('plan', 'worker', 'monitor', 'finish'))
    parser.add_argument('--campaign-id', default='')
    parser.add_argument('--task-id')
    parser.add_argument('--runtime', type=Path)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--deadline', type=float, default=float('inf'))
    args = parser.parse_args()
    api = LiveGitHub(os.environ['GITHUB_REPOSITORY'], os.environ['GITHUB_TOKEN'], timeout_seconds=90)
    api.wait_on_rate_limit = args.action != 'monitor'
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        api.data_store = GitDataStore(core.ROOT)
    if args.action == 'plan':
        plan_campaign(api, json.loads(os.environ['SPECTRA_INPUTS']), args.directory)
    elif args.action == 'worker':
        run_worker(api, args.campaign_id, args.task_id, args.runtime, args.directory, args.deadline)
    elif args.action == 'monitor':
        monitor(api, args.campaign_id, args.directory)
    else:
        finish(api, args.campaign_id, args.directory)


if __name__ == '__main__':
    main()

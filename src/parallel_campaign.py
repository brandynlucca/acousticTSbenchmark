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
TASK_FIELDS = ('software', 'semimajor_m', 'aspect_ratio', 'boundary', 'incidence_deg', 'repetition')
REFERENCE_FIELDS = ('reference_id', 'reference_source_sha', 'reference_ts_db',
                    'reference_qualification', 'reference_fingerprint', 'reference_uncertainty_db')
CONTROLLER_FILES = {'src/parallel_campaign.py', 'src/publish_progress.py',
                    'tests/test_parallel_campaign.py', 'tests/test_publish_progress.py'}


class LiveGitHub(GitHub):
    """Give every network operation a bounded deadline during long-running jobs."""
    def request(self, method, path, body=None):
        self.deadline = time.monotonic() + 90
        return super().request(method, path, body)

    def archive(self, artifact_id):
        self.deadline = time.monotonic() + 180
        return super().archive(artifact_id)


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


def get_document(api, path):
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
    return unpack(json.loads(raw)), entry['sha']


def put_document(api, path, value, previous_sha=None, *, immutable=False):
    raw = (json.dumps(pack(value), separators=(',', ':')) + '\n').encode()
    for attempt in range(6):
        if previous_sha is None:
            previous, previous_sha = get_document(api, path)
            if previous is not None and immutable:
                if previous != value:
                    raise ValueError('Refusing to replace an existing spectrum plan')
                return previous_sha
        body = dict(branch=BRANCH, message=f'Checkpoint {path}',
                    content=base64.b64encode(raw).decode())
        if previous_sha:
            body['sha'] = previous_sha
        try:
            return api.request('PUT', '/contents/' + path, body)['content']['sha']
        except APIError as error:
            if error.status not in (409, 422) or attempt == 5:
                raise
            current, current_sha = get_document(api, path)
            if current == value:
                return current_sha
            if current_sha != previous_sha:
                raise ValueError('Another writer changed this task or campaign state') from error
            time.sleep(min(2 ** attempt, 8))
    raise RuntimeError('Unable to checkpoint spectrum state')


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
        packet = unpack(json.loads(raw))
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
    for artifact in all_artifacts(api, wave['run_id']):
        match = re.fullmatch(r'spectrum-([a-f0-9]{24})-([1-9][0-9]*)', artifact['name'])
        if not match or match[1] not in tasks or artifact.get('expired'):
            continue
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
            if len(entries) != 1 or entries[0].file_size > MAX_BYTES:
                raise ValueError('Ambiguous or oversized recovery checkpoint')
            saved = json.loads(archive.read(entries[0]))
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
        put_document(api, path, saved, sha)


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
        if state['waves']:
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
    completed = {task_id for task_id, packet in packets.items() if packet.get('state') == 'complete'}
    candidates = [task for task in plan['tasks'] if task['id'] not in completed | set(state['blocked'])]
    wave = candidates[:plan['max_parallel']]
    state.update(status='active', completed=sorted(completed))
    state['waves'].append(dict(run_id=int(os.environ['GITHUB_RUN_ID']),
                               tasks=[task['id'] for task in wave], started_at=core.utc_now()))
    put_document(api, filename(campaign_id, 'state'), state, state_sha)
    directory.mkdir(parents=True, exist_ok=True)
    core.atomic_json(directory / 'plan.json', plan)
    core.atomic_json(directory / 'wave.json', state['waves'][-1])
    write_outputs(dict(campaign_id=campaign_id, matrix=[dict(id=task['id'], name=task['name'],
        software=task['software']) for task in wave], has_work=bool(wave), max_parallel=plan['max_parallel'],
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
    core.atomic_json(directory / 'manifest.json', dict(identity, fingerprint=core.fingerprint(identity)))
    core.atomic_json(directory / 'results.json',
        [packet['records'].get(case['case_id'], dict(case, status='pending')) for case in task['cases']])
    for invocation, environment in packet['environments'].items():
        core.atomic_json(directory / 'environments' / f'{invocation}.json', environment)


def run_task(api, campaign_id, task_id, runtime_path, directory, deadline):
    plan, _ = get_document(api, filename(campaign_id, 'plan'))
    state, _ = get_document(api, filename(campaign_id, 'state'))
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
    directory.mkdir(parents=True, exist_ok=True)
    next_publication = 0.0
    def checkpoint(force=False):
        nonlocal packet_sha, next_publication
        packet['updated_at'] = core.utc_now()
        core.atomic_json(directory / 'checkpoint.json', packet)
        if force or time.monotonic() >= next_publication:
            export_task(plan, task, packet, runtime, directory)
            packet_sha = put_document(api, path, packet, packet_sha)
            next_publication = time.monotonic() + 120
            print(f"CHECKPOINT {task['name']} recorded={len(packet['records'])}/{len(task['cases'])}", flush=True)
    packet['state'] = 'running'
    timeout = plan['datasets'][task['size']]['configuration']['case_timeout_seconds']
    checkpoint(True)
    try:
        for case in task['cases']:
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
                       finished_at=core.utc_now())
            packet['records'][case['case_id']] = row
            print(f"DONE {task['name']} f={case['frequency_hz']/1000:g} kHz "
                  f"status={row['status']} solve_seconds={row.get('elapsed_seconds')}", flush=True)
            checkpoint(len(packet['records']) == 1)
        else:
            packet['state'] = 'complete'
    finally:
        if packet['state'] == 'running':
            packet['state'] = 'interrupted'
        checkpoint(True)


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
        payload['artifact_url'] = f"https://github.com/{api.repository}/actions/runs/{os.environ['GITHUB_RUN_ID']}#artifacts"
        current, _ = existing_snapshot(api, platform, snapshot_filename(platform, float(size)))
        if current and int(current['run_id']) > int(plan['campaign_id']):
            raise ValueError('A newer campaign owns this benchmark. Refusing to replace its measurements')
        target = directory / size
        target.mkdir(parents=True, exist_ok=True)
        core.atomic_json(target / 'results.json', rows)
        core.atomic_json(target / 'parallel-plan.json', dict(campaign_id=plan['campaign_id'],
            platform=platform, source_ref=plan['source_ref'], configuration=dataset['configuration'], sources=plan['sources']))
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
    names = {task['name'] for task in plan['tasks'] if task['id'] in wave['tasks']}
    packets = {}
    next_update = 0
    while True:
        jobs = api.request('GET', f"/actions/runs/{os.environ['GITHUB_RUN_ID']}/jobs?per_page=100")['jobs']
        workers = [job for job in jobs if job['name'] in names]
        done = len(workers) == len(names) and all(job['status'] == 'completed' for job in workers)
        if time.monotonic() >= next_update or done:
            packets = read_packets(api, plan, cache=packets)
            try:
                publish_datasets(api, plan, packets, directory)
            except PublicationConflict as error:
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
    for task_id in wave['tasks']:
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
        api.request('POST', f'/actions/workflows/{WORKFLOW}/dispatches',
                    dict(ref=state.get('controller_tag', f'spectra-{campaign_id}'), inputs=continuation))
        print(f'CONTINUATION_DISPATCHED campaign={campaign_id} unfinished_tasks={len(pending)}', flush=True)
    if state['status'] == 'publication_blocked':
        raise PublicationConflict('All timing tasks are saved, but final dashboard publication requires recovery')


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
    if args.action == 'plan':
        plan_campaign(api, json.loads(os.environ['SPECTRA_INPUTS']), args.directory)
    elif args.action == 'worker':
        run_task(api, args.campaign_id, args.task_id, args.runtime, args.directory, args.deadline)
    elif args.action == 'monitor':
        monitor(api, args.campaign_id, args.directory)
    else:
        finish(api, args.campaign_id, args.directory)


if __name__ == '__main__':
    main()

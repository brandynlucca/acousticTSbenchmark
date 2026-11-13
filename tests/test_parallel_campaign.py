"""Preservation, repetition, serial worker, recovery and publication contracts."""
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import parallel_campaign as parallel
import benchmark as core


def legacy_timeout_streak(task, records):
    """Build historical ten-timeout proofs for saved-queue compatibility tests."""
    streak = []
    for case in sorted(task['cases'], key=lambda case: case['frequency_hz']):
        row = records.get(case['case_id'], {})
        if row.get('status') == 'timeout' and row.get('prediction_attempted') is not False:
            streak.append(case['case_id'])
            if len(streak) == 10:
                return dict(after_frequency_hz=case['frequency_hz'],
                            rule='ten_consecutive_prediction_timeouts', trigger_case_ids=streak)
        else:
            streak = []
    return None


def legacy_scheduling_queue(plan, candidates, packets, run_id, maximum):
    """Represent persisted frequency queues created before whole-sweep scheduling."""
    queue = dict(schema_version=2, campaign_id=plan['campaign_id'], run_id=run_id,
                 slow_frequency_seconds=60.0, pending=[], deferred=[], units={}, owners={},
                 claims={}, finished={}, timeout_streaks={}, actual_timeouts={})
    resumed = []
    for task in candidates:
        packet = packets.get(task['id'], {})
        records = packet.get('records', {})
        queue['actual_timeouts'][task['id']] = sorted(identity for identity, row in records.items()
            if row.get('status') == 'timeout' and row.get('prediction_attempted') is not False)
        cutoff = legacy_timeout_streak(task, records)
        if cutoff:
            queue['timeout_streaks'][task['id']] = cutoff
        remaining = [case['case_id'] for case in task['cases'] if case['case_id'] not in records]
        if not remaining:
            continue
        expensive = packet.get('slow_sweep') or any(row.get('status') == 'timeout' or
            isinstance(row.get('elapsed_seconds'), (int, float)) and row['elapsed_seconds'] >= 60
            for row in records.values())
        if expensive:
            for identity in remaining:
                queue['units'][identity] = dict(kind='frequency', task_id=task['id'], case_id=identity)
                queue['deferred'].append(identity)
        else:
            queue['units'][task['id']] = dict(kind='sweep', task_id=task['id'])
            (resumed if records else queue['pending']).append(task['id'])
    queue['pending'].extend(resumed)
    count = min(maximum, len(queue['pending']) + len(queue['deferred']))
    workers = [task['id'] for task in candidates[:count]]
    while len(workers) < count:
        workers.append(core.fingerprint([plan['campaign_id'], run_id, 'worker', len(workers)])[:24])
    queue['owners'] = {worker: None for worker in workers}
    return queue, workers


def fixture():
    config = dict(geometry=dict(semimajor_m=.07, aspect_ratios=[1.001, 5]),
        incidences_deg=[0, 37, 90], boundaries=list(core.BOUNDARIES),
        software=['acousticts', 'prol_spheroid'], repetitions=1, warmup=True,
        case_timeout_seconds=600, frequency=dict(start_hz=1000, stop_hz=5000, step_hz=2000),
        medium=dict(density_kg_m3=1026.8, sound_speed_m_s=1477.3),
        materials=dict(liquid=dict(density_kg_m3=1028.9, sound_speed_m_s=1480.3),
                       gas=dict(density_kg_m3=1.24, sound_speed_m_s=345.0)),
        controls={name: {} for name in core.CAPABILITIES}, profiles=dict(full={}))
    sources = dict(sources={name: dict(sha='a'*40) for name in core.CAPABILITIES})
    runtime = dict(source_shas=dict(acousticts='a'*40), environment_fingerprint='e'*64,
        availability=dict(acousticts=dict(status='ready')),
        environment_compatibility=dict(r_packages=dict(R='4.5.2')))
    cases = [case for case in core.build_cases(config, 'full')
             if case['boundary'] in core.CAPABILITIES[case['software']]]
    rows = [dict(case, status='ok', elapsed_seconds=.5, ts_db=-20., source_sha='a'*40,
        reference_ts_db=-20.001, reference_id='b'*64) for case in cases]
    baseline = dict(configuration=config, profile='full', rows=rows, baseline_run_id=10,
                    environments=[dict(runtime=runtime)])
    return baseline, sources, runtime


def pending_plan():
    baseline, sources, runtime = fixture()
    selected = [case for case in core.build_cases(baseline['configuration'], 'full', ['acousticts'])
                if case['boundary'] == 'rigid' and case['aspect_ratio'] == 5 and case['incidence_deg'] == 37]
    tasks = parallel.make_tasks('100', {'0.07':selected})
    dataset = dict(baseline, rows=[dict(case, status='pending') for case in selected], compatibility={})
    plan = dict(campaign_id='100', source_ref='c'*40, sources=sources, tasks=tasks, datasets={'0.07':dataset})
    return plan, tasks[0], runtime


def packet_for(plan, task, runtime, count=1):
    env = dict(platform='Linux', processor='Test CPU', github_run_id='100', github_run_attempt='1', runtime=runtime)
    records = {case['case_id']:dict(case, status='ok', elapsed_seconds=.3, ts_db=-20.,
        source_sha='a'*40, invocation_id='inv', allocation_id='inv', measurement_origin_run_id=100,
        measurement_id=core.fingerprint([plan['campaign_id'],case['case_id']])) for case in task['cases'][:count]}
    return dict(campaign_id='100', task_id=task['id'], source_ref=plan['source_ref'], records=records,
                environments={'inv':env}, state='complete' if count == len(task['cases']) else 'interrupted')


class PlanningTests(unittest.TestCase):
    def test_interrupted_measurements_are_published_before_worker_allocation(self):
        for failure in (False, True):
            with self.subTest(publication_failure=failure):
                plan, task, runtime = pending_plan()
                plan.update(max_parallel=16, budget_minutes=330, automatic_continue=True)
                original = copy.deepcopy(plan)
                state = dict(campaign_id='100', status='active', attempts={}, completed=[], blocked=[],
                    waves=[dict(run_id=100, tasks=[task['id']])], publish_recovered_before_resume=True)
                documents = {parallel.filename('100', 'plan'): plan, parallel.filename('100', 'state'): state}
                order = []
                def get(api, path):
                    return copy.deepcopy(documents.get(path)), 's'
                def put(api, path, value, *args, **kwargs):
                    order.append('allocate' if 'queue' in path else 'save')
                    documents[path] = copy.deepcopy(value)
                    return 's'
                def publish(*args, **kwargs):
                    order.append('publish')
                    if failure:
                        raise parallel.PublicationConflict('retry publication')
                api = Mock(repository='owner/repo')
                api.request.return_value = dict(object=dict(sha='c'*40))
                with tempfile.TemporaryDirectory() as temporary, \
                     patch.dict(os.environ, GITHUB_SHA='c'*40, GITHUB_RUN_ID='101'), \
                     patch.object(parallel, 'get_document', side_effect=get), \
                     patch.object(parallel, 'put_document', side_effect=put), \
                     patch.object(parallel, 'recover_artifacts', side_effect=lambda *args: order.append('recover')), \
                     patch.object(parallel, 'read_packets', return_value={task['id']: packet_for(plan, task, runtime)}), \
                     patch.object(parallel, 'publish_datasets', side_effect=publish), \
                     patch.object(parallel, 'write_outputs'):
                    if failure:
                        with self.assertRaises(parallel.PublicationConflict):
                            parallel.plan_campaign(api, dict(campaign_id='100'), Path(temporary))
                    else:
                        parallel.plan_campaign(api, dict(campaign_id='100'), Path(temporary))
                self.assertEqual(order[:2], ['recover', 'publish'])
                self.assertEqual(documents[parallel.filename('100', 'plan')], original)
                saved = documents[parallel.filename('100', 'state')]
                if failure:
                    self.assertEqual(order, ['recover', 'publish'])
                    self.assertTrue(saved['publish_recovered_before_resume'])
                else:
                    self.assertNotIn('publish_recovered_before_resume', saved)
                    self.assertEqual(saved['execution_policy'], parallel.execution_policy())
                    self.assertEqual(saved['waves'][-1]['execution_policy'], parallel.execution_policy())
                    self.assertEqual(saved['recovery_publications'][0]['observations'], 1)

    def test_completed_wave_continues_only_remaining_selected_spectra(self):
        base,sources,runtime=fixture()
        documents={};packets={};outputs=[]
        def get(api,path): return copy.deepcopy(documents.get(path)), 's' if path in documents else None
        def put(api,path,value,*args,**kwargs): documents[path]=copy.deepcopy(value);return 's'
        api=Mock(repository='owner/repo')
        api.request.return_value=dict(object=dict(sha='c'*40))
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ,GITHUB_SHA='c'*40,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=get), \
             patch.object(parallel,'put_document',side_effect=put), \
             patch.object(parallel,'existing_snapshot',return_value=(None,None)), \
             patch.object(parallel,'load_legacy',return_value=base), \
             patch.object(core,'read_json',return_value=sources), \
             patch.object(parallel,'read_packets',side_effect=lambda *args,**kwargs:packets), \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'publish_datasets'), \
             patch.object(parallel,'write_outputs',side_effect=outputs.append):
            directory=Path(temporary)
            inputs=dict(mode='refresh',software='acousticts',semimajor_axes='0.07',baseline_runs='0.07=10',
                        boundaries='rigid,pressure_release',max_parallel=2,automatic_continue=True)
            parallel.plan_campaign(api,inputs,directory)
            plan=documents[parallel.filename('100','plan')]
            first=outputs[-1]['matrix']
            self.assertEqual(len(first),2)
            self.assertEqual({case['boundary'] for task in plan['tasks'] for case in task['cases']},
                             {'rigid','pressure_release'})
            for task in plan['tasks']:
                if task['id'] in {item['id'] for item in first}:
                    packets[task['id']]=packet_for(plan,task,runtime,len(task['cases']))
            parallel.finish(api,'100',directory)
            self.assertEqual(outputs[-1]['completed'],2)
            self.assertTrue(any(call.args[:2]==('POST','/actions/workflows/parallel-spectra.yml/dispatches')
                                for call in api.request.call_args_list))
            os.environ['GITHUB_RUN_ID']='101'
            saved_plan = copy.deepcopy(plan)
            parallel.plan_campaign(api,dict(campaign_id='100',max_parallel='16'),directory)
            self.assertFalse({item['id'] for item in first}&{item['id'] for item in outputs[-1]['matrix']})
            self.assertEqual(outputs[-1]['max_parallel'],16)
            self.assertEqual(documents[parallel.filename('100','plan')],saved_plan)
            parallel.finish(api,'100',directory)
            self.assertEqual(api.request.call_args.args[2]['inputs']['max_parallel'],'16')

    def test_extend_preserves_recorded_software_and_references(self):
        base, sources, _ = fixture()
        original = copy.deepcopy(base['rows'])
        missing = base['rows'][0]
        base['rows'][0] = {key:value for key,value in missing.items()
                          if key not in ('status','elapsed_seconds','ts_db','source_sha')}
        base['rows'][0]['status'] = 'pending'
        data, pending = parallel.prepare_dataset(base, dict(mode='extend', software='acousticts'), sources)
        self.assertEqual([row['case_id'] for row in pending], [missing['case_id']])
        after = {row['case_id']:row for row in data['rows']}
        for row in original[1:]:
            self.assertEqual(row, after[row['case_id']])

    def test_two_additional_repetitions_keep_original_and_independent_tasks(self):
        base, sources, _ = fixture()
        data, pending = parallel.prepare_dataset(base, dict(mode='repeat', software='acousticts',
            additional_repetitions=2, boundaries='rigid,pressure_release'), sources)
        self.assertEqual({case['repetition'] for case in pending}, {2,3})
        self.assertEqual(len(pending), 72)
        after = {row['case_id']:row for row in data['rows']}
        for row in base['rows']:
            self.assertEqual(row, after[row['case_id']])
        tasks = parallel.make_tasks('100', {'0.07':pending})
        self.assertEqual(len(tasks), 24)
        self.assertEqual(len({task['id'] for task in tasks}), 24)
        for task in tasks:
            self.assertEqual(len({tuple(case[field] for field in parallel.TASK_FIELDS) for case in task['cases']}),1)
            self.assertEqual([case['frequency_hz'] for case in task['cases']],[1000,3000,5000])

    def test_refresh_replaces_only_selected_scope(self):
        base, sources, _ = fixture()
        sources['sources']['acousticts']['sha'] = 'd'*40
        data, pending = parallel.prepare_dataset(base, dict(mode='refresh', software='acousticts',
            boundaries='rigid', incidences_deg='37', aspect_ratios='5'), sources)
        self.assertEqual(len(pending),3)
        after = {row['case_id']:row for row in data['rows']}
        replaced = {case['case_id'] for case in pending}
        for row in base['rows']:
            if row['case_id'] not in replaced:
                self.assertEqual(row,after[row['case_id']])
        self.assertEqual(sum(row['status']=='pending' for row in data['rows']),3)

    def test_append_rejects_source_control_and_missing_environment_changes(self):
        for kind in ('source','controls','environment'):
            with self.subTest(kind=kind):
                base, sources, _ = fixture()
                if kind == 'source': sources['sources']['acousticts']['sha']='d'*40
                elif kind == 'controls': base['configuration']['controls']['acousticts']={'precision':'double'}
                else: base['environments']=[]
                with self.assertRaises(ValueError):
                    parallel.prepare_dataset(base,dict(mode='extend',software='acousticts'),sources)

    def test_prol_only_liquid_and_gas(self):
        base,sources,_=fixture()
        data,pending=parallel.prepare_dataset(base,dict(mode='refresh',software='prol_spheroid'),sources)
        self.assertEqual({case['boundary'] for case in pending},{'liquid','gas'})

    def test_pack_roundtrip_and_tamper(self):
        value={'sample':[1,2,3], 'source':'a'*40}
        packed=parallel.pack(value)
        self.assertEqual(parallel.unpack(packed),value)
        packed['sha256']='0'*64
        with self.assertRaises(ValueError): parallel.unpack(packed)


class ObservationTests(unittest.TestCase):
    def test_publication_outage_does_not_stop_measurements_or_lose_local_records(self):
        plan, task, runtime = pending_plan()
        packet = packet_for(plan, task, runtime)
        def read(api, path):
            if path.endswith('-plan.json'):
                return copy.deepcopy(plan), 'p'
            if path.endswith('-state.json'):
                return {}, 's'
            return copy.deepcopy(packet), 't'
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            core.atomic_json(directory/'runtime.json', runtime)
            with patch.dict(os.environ,GITHUB_RUN_ID='100',GITHUB_RUN_ATTEMPT='1',GITHUB_SHA='c'*40), \
                 patch.object(parallel.host_platform,'system',return_value='Linux'), \
                 patch.object(parallel,'get_document',side_effect=read), \
                 patch.object(parallel,'put_document',side_effect=parallel.APIError(403,'rate limited')), \
                 patch.object(parallel,'task_environment',return_value=packet['environments']['inv']), \
                 patch.object(core,'measure',return_value=dict(status='ok',elapsed_seconds=.4,ts_db=-20.)) as measure:
                parallel.run_task(Mock(), '100',task['id'],directory/'runtime.json',directory/'worker',float('inf'))
            self.assertEqual(measure.call_count, 2)
            saved = core.read_json(directory/'worker/checkpoint.json')
            self.assertEqual(saved['state'], 'complete')
            self.assertEqual(len(saved['records']), 3)
            parallel.validate_packet(plan,task,saved)
            self.assertEqual(len(core.read_json(directory/'worker/results.json')),3)

    def test_packet_identity_origin_and_finite_validation(self):
        plan,task,runtime=pending_plan()
        packet=packet_for(plan,task,runtime)
        parallel.validate_packet(plan,task,packet)
        for key,value in [('source_sha','d'*40),('incidence_deg',90),('elapsed_seconds',float('nan')),
                          ('measurement_origin_run_id',200),('allocation_id','wrong')]:
            bad=copy.deepcopy(packet)
            next(iter(bad['records'].values()))[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError): parallel.validate_packet(plan,task,bad)
        packet['state']='complete'
        with self.assertRaises(ValueError): parallel.validate_packet(plan,task,packet)

    def test_idempotent_merge_and_conflicting_duplicate(self):
        plan,task,runtime=pending_plan()
        packet=packet_for(plan,task,runtime)
        data=plan['datasets']['0.07']
        data['rows'][0].update(reference_ts_db=-20.001,reference_id='b'*64)
        merged=parallel.merge_dataset(data,[task],{task['id']:packet})
        self.assertAlmostEqual(merged[0]['delta_ts_db'],.001)
        data['rows']=merged
        self.assertEqual(parallel.merge_dataset(data,[task],{task['id']:packet}),merged)
        next(iter(packet['records'].values()))['elapsed_seconds']=9
        with self.assertRaises(ValueError): parallel.merge_dataset(data,[task],{task['id']:packet})

    def test_worker_runs_serially_skips_saved_frequency_and_exports(self):
        plan,task,runtime=pending_plan()
        packet=packet_for(plan,task,runtime)
        saved=[]
        def read(api,path): return (copy.deepcopy(plan),'p') if path.endswith('-plan.json') else (copy.deepcopy(packet),'t')
        def write(api,path,value,sha=None,**kwargs): saved.append(copy.deepcopy(value));return 't'
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            core.atomic_json(directory/'runtime.json',runtime)
            with patch.dict(os.environ,GITHUB_RUN_ID='100',GITHUB_RUN_ATTEMPT='1',GITHUB_SHA='c'*40), \
                 patch.object(parallel.host_platform,'system',return_value='Linux'), \
                 patch.object(parallel,'get_document',side_effect=read), \
                 patch.object(parallel,'put_document',side_effect=write), \
                 patch.object(parallel,'task_environment',return_value=packet['environments']['inv']), \
                 patch.object(core,'measure',return_value=dict(status='ok',elapsed_seconds=.4,ts_db=-20.)) as measure:
                parallel.run_task(Mock(), '100',task['id'],directory/'runtime.json',directory/'worker',float('inf'))
            self.assertEqual([call.args[0]['frequency_hz'] for call in measure.call_args_list],[3000,5000])
            self.assertEqual(saved[-1]['state'],'complete')
            self.assertEqual(saved[-1]['records'][task['cases'][0]['case_id']],packet['records'][task['cases'][0]['case_id']])
            parallel.validate_packet(plan,task,saved[-1])
            self.assertTrue((directory/'worker/manifest.json').exists())
            self.assertEqual(len(core.read_json(directory/'worker/results.json')),3)

    def test_budget_pauses_without_fake_failures(self):
        plan,task,runtime=pending_plan()
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);core.atomic_json(directory/'runtime.json',runtime)
            with patch.dict(os.environ,GITHUB_RUN_ID='100',GITHUB_SHA='c'*40), \
                 patch.object(parallel.host_platform,'system',return_value='Linux'), \
                 patch.object(parallel,'get_document',side_effect=[(plan,'p'),(None,None),(None,None)]), \
                 patch.object(parallel,'put_document',return_value='t'), \
                 patch.object(core,'measure') as measure:
                parallel.run_task(Mock(),'100',task['id'],directory/'runtime.json',directory/'worker',0)
            measure.assert_not_called()
            packet=core.read_json(directory/'worker/checkpoint.json')
            self.assertEqual(packet['state'],'budget_paused')
            self.assertEqual(packet['records'],{})

    def test_recover_unpublished_final_artifact(self):
        plan,task,runtime=pending_plan()
        earlier=packet_for(plan,task,runtime)
        final=packet_for(plan,task,runtime,3)
        archive=io.BytesIO()
        with zipfile.ZipFile(archive,'w') as stream: stream.writestr('spectrum/checkpoint.json',json.dumps(final))
        api=Mock();api.archive.return_value=archive.getvalue()
        artifact=dict(id=12,name=f"spectrum-{task['id']}-1")
        with patch.object(parallel,'all_artifacts',return_value=[artifact]), \
             patch.object(parallel,'get_document',return_value=(earlier,'s')), \
             patch.object(parallel,'put_document',return_value='t') as write:
            parallel.recover_artifacts(api,plan,dict(run_id=100,tasks=[task['id']]))
        recovered=write.call_args.args[2]
        self.assertEqual(recovered['state'],'complete')
        self.assertEqual(len(recovered['records']),3)

    def test_worker_artifact_import_is_idempotent_and_preserves_origin(self):
        import dataset
        plan,task,runtime=pending_plan()
        packet=packet_for(plan,task,runtime,3)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            parallel.export_task(plan,task,packet,runtime,directory)
            archive=directory/'worker.zip'
            with zipfile.ZipFile(archive,'w') as stream:
                for path in directory.rglob('*.json'): stream.write(path,path.relative_to(directory))
            connection=dataset.connect_database(directory/'data.sqlite')
            try:
                for artifact_id in (1,2):
                    result=dataset.import_artifact(connection,archive,
                        dict(id=artifact_id,name=f"spectrum-{task['id']}-1",run_id=100),'owner/repo')
                    self.assertEqual(result['observations'],3 if artifact_id==1 else 0)
                rows=connection.execute('SELECT origin_run_id, source_sha, elapsed_seconds FROM observations').fetchall()
                self.assertEqual([tuple(row) for row in rows],[(100,'a'*40,.3)]*3)
            finally: connection.close()

    def test_dataset_collector_recognizes_each_parallel_worker(self):
        from sync_dataset import select_artifacts
        artifacts=[dict(id=index,name=f"spectrum-{'a'*23}{index}-1",created_at='2026-11-02',
                        workflow_run=dict(id=100)) for index in (1,2)]
        self.assertEqual(len(select_artifacts(artifacts)),2)


class PublicationTests(unittest.TestCase):
    def test_platform_queue_advances_only_after_completed_publication(self):
        for complete in (False, True):
            plan,task,runtime=pending_plan();plan['automatic_continue']=True
            packet=packet_for(plan,task,runtime,len(task['cases']) if complete else 1)
            state=dict(waves=[dict(run_id=100,tasks=[task['id']])],attempts={},blocked=[],
                max_parallel=16,next_platform_campaign=dict(campaign_id='200',platform='windows-latest'))
            api=Mock();saved=[]
            with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,GITHUB_RUN_ID='100'), \
                 patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
                 patch.object(parallel,'recover_artifacts'), \
                 patch.object(parallel,'read_packets',return_value={task['id']:packet}), \
                 patch.object(parallel,'put_document',side_effect=lambda api,path,value,*args: saved.append(copy.deepcopy(value)) or 's'), \
                 patch.object(parallel,'publish_datasets'):
                parallel.finish(api,'100',Path(directory))
            inputs=api.request.call_args.args[2]['inputs']
            self.assertEqual(inputs['campaign_id'],'200' if complete else '100')
            self.assertEqual(inputs['max_parallel'],'16')
            self.assertEqual(bool(saved[-1].get('next_platform_dispatched')),complete)

    def test_monitor_survives_exhausted_publication_retries(self):
        plan,task,_=pending_plan()
        state=dict(waves=[dict(run_id=100,tasks=[task['id']])])
        api=Mock()
        api.request.side_effect=[dict(jobs=[dict(name=task['name'],status=status)])
                                 for status in ('in_progress','completed')]
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s'),(state,'s'),(state,'s')]), \
             patch.object(parallel,'read_packets',return_value={}), \
             patch.object(parallel.time,'sleep'), \
             patch.object(parallel,'publish_datasets',side_effect=[parallel.PublicationConflict('busy'),None]) as publish:
            parallel.monitor(api,'100',Path(directory))
        self.assertEqual(publish.call_count,2)

    def test_final_publication_conflict_dispatches_publication_only_recovery(self):
        plan,task,runtime=pending_plan();plan['automatic_continue']=True
        packet=packet_for(plan,task,runtime,len(task['cases']))
        state=dict(waves=[dict(run_id=100,tasks=[task['id']])],attempts={},blocked=[])
        api=Mock();saved=[]
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'read_packets',return_value={task['id']:packet}), \
             patch.object(parallel,'put_document',side_effect=lambda api,path,value,*args: saved.append(copy.deepcopy(value)) or 's'), \
             patch.object(parallel,'publish_datasets',side_effect=parallel.PublicationConflict('busy')):
            parallel.finish(api,'100',Path(directory))
        self.assertEqual(saved[-1]['status'],'publishing')
        self.assertTrue(saved[-1]['publication_pending'])
        self.assertEqual(saved[-1]['completed'],[task['id']])
        self.assertEqual(api.request.call_args.args[2]['inputs'],dict(campaign_id='100'))

    def test_controller_upgrade_records_pin_and_rejects_numerical_changes(self):
        plan,_,_=pending_plan()
        api=Mock();state={}
        with patch.dict(os.environ,GITHUB_SHA='d'*40):
            api.request.side_effect=[dict(status='ahead',files=[dict(filename='src/publish_progress.py')]),
                                     dict(object=dict(sha='d'*40))]
            parallel.select_controller(api,plan,state)
            self.assertEqual(state['controller_ref'],'d'*40)
            self.assertEqual(plan['source_ref'],'c'*40)
            for filename in ('src/worker_acousticts.R','sources.lock.json','.github/workflows/parallel-spectra.yml'):
                api.request.side_effect=[dict(status='ahead',files=[dict(filename=filename)])]
                with self.assertRaises(ValueError): parallel.select_controller(api,plan,{})

    def test_publication_conflict_still_continues_unfinished_spectrum(self):
        plan,task,runtime=pending_plan();plan['automatic_continue']=True
        packet=packet_for(plan,task,runtime,1)
        state=dict(waves=[dict(run_id=100,tasks=[task['id']])],attempts={},blocked=[],controller_tag='fixed-controller')
        api=Mock();saved=[]
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'read_packets',return_value={task['id']:packet}), \
             patch.object(parallel,'put_document',side_effect=lambda api,path,value,*args: saved.append(copy.deepcopy(value)) or 's'), \
             patch.object(parallel,'publish_datasets',side_effect=parallel.PublicationConflict('busy')):
            parallel.finish(api,'100',Path(directory))
        self.assertEqual(saved[-1]['status'],'active')
        self.assertEqual(saved[-1]['attempts'][task['id']]['recorded'],1)
        self.assertEqual(api.request.call_args.args[2],dict(ref='fixed-controller',inputs=dict(campaign_id='100')))

    def test_final_publication_failure_stops_after_three_recovery_attempts(self):
        plan,task,runtime=pending_plan();plan['automatic_continue']=True
        packet=packet_for(plan,task,runtime,len(task['cases']))
        state=dict(waves=[dict(run_id=100,tasks=[])],attempts={},blocked=[],publication_attempts=2)
        api=Mock();saved=[]
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'read_packets',return_value={task['id']:packet}), \
             patch.object(parallel,'put_document',side_effect=lambda api,path,value,*args: saved.append(copy.deepcopy(value)) or 's'), \
             patch.object(parallel,'publish_datasets',side_effect=parallel.PublicationConflict('busy')), \
             self.assertRaises(parallel.PublicationConflict):
            parallel.finish(api,'100',Path(directory))
        self.assertEqual(saved[-1]['status'],'publication_blocked')
        self.assertEqual(saved[-1]['completed'],[task['id']])
        api.request.assert_not_called()


class OrderedQueueHandoffTests(unittest.TestCase):
    def finish_campaign(self, *, complete=True, workflow='active', request_error=None,
                        error_method='GET', publication_error=None, state_changes=None):
        plan, task, runtime = pending_plan()
        plan['automatic_continue'] = True
        packet = packet_for(plan, task, runtime, len(task['cases']) if complete else 1)
        original = copy.deepcopy(packet)
        state = dict(waves=[dict(run_id=100, tasks=[task['id']])], attempts={}, blocked=[])
        state.update(state_changes or {})
        saved, events = [], []
        api = Mock()

        def request(method, path, body=None):
            events.append((method, path))
            if path.startswith('/actions/workflows/queue.yml'):
                self.assertEqual(saved[-1]['status'], 'complete')
                self.assertFalse(saved[-1]['publication_pending'])
                if request_error and method == error_method:
                    raise request_error
                return dict(state=workflow) if method == 'GET' else None
            return None

        def save(api, path, value, *args):
            saved.append(copy.deepcopy(value))
            events.append(('save', value['status']))
            return 's'

        def publish(*args, **kwargs):
            events.append(('publish', None))
            if publication_error:
                raise publication_error

        api.request.side_effect = request
        with patch.dict(os.environ, GITHUB_RUN_ID='100'), \
             patch.object(parallel, 'get_document', side_effect=[(plan, 'p'), (state, 's')]), \
             patch.object(parallel, 'recover_artifacts'), \
             patch.object(parallel, 'read_packets', return_value={task['id']: packet}), \
             patch.object(parallel, 'put_document', side_effect=save), \
             patch.object(parallel, 'publish_datasets', side_effect=publish), \
             patch.object(parallel, 'write_outputs') as outputs, patch('sys.stdout', new_callable=io.StringIO) as log:
            parallel.finish(api, '100', Path('unused'))
        self.assertEqual(packet, original)
        return api, saved, events, outputs.call_args.args[0], log.getvalue()

    def test_complete_publication_wakes_main_queue_after_durable_completion(self):
        api, saved, events, output, log = self.finish_campaign()
        self.assertEqual(events, [('save', 'publishing'), ('publish', None), ('save', 'complete'),
                                 ('GET', '/actions/workflows/queue.yml'),
                                 ('POST', '/actions/workflows/queue.yml/dispatches')])
        self.assertEqual(api.request.call_args.args,
            ('POST', '/actions/workflows/queue.yml/dispatches',
             dict(ref='main', inputs=dict(wait_seconds='120'))))
        self.assertEqual(output['state'], 'complete')
        self.assertIn('QUEUE_HANDOFF_DISPATCHED campaign=100', log)

    def test_disabled_queue_remains_disabled(self):
        for status in ('disabled_manually', 'disabled_inactivity', 'deleted'):
            with self.subTest(status=status):
                api, saved, _, _, log = self.finish_campaign(workflow=status)
                api.request.assert_called_once_with('GET', '/actions/workflows/queue.yml')
                self.assertEqual(saved[-1]['status'], 'complete')
                self.assertIn('QUEUE_HANDOFF_SKIPPED', log)

    def test_incomplete_or_blocked_campaign_never_wakes_ordered_queue(self):
        _, task, _ = pending_plan()
        for blocked in (False, True):
            with self.subTest(blocked=blocked):
                api, saved, _, _, _ = self.finish_campaign(complete=False,
                    state_changes=dict(blocked=[task['id']]) if blocked else None)
                self.assertEqual(saved[-1]['status'], 'blocked' if blocked else 'active')
                self.assertFalse(any('/queue.yml' in call.args[1] for call in api.request.call_args_list))

    def test_incomplete_publication_retains_recovery_without_queue_handoff(self):
        api, saved, _, _, _ = self.finish_campaign(publication_error=parallel.PublicationConflict('busy'))
        self.assertEqual(saved[-1]['status'], 'publishing')
        self.assertTrue(saved[-1]['publication_pending'])
        self.assertEqual(api.request.call_args.args,
            ('POST', '/actions/workflows/parallel-spectra.yml/dispatches',
             dict(ref='spectra-100', inputs=dict(campaign_id='100'))))
        self.assertFalse(any('/queue.yml' in call.args[1] for call in api.request.call_args_list))

    def test_legacy_platform_handoff_does_not_wake_ordered_queue(self):
        for already_dispatched in (False, True):
            with self.subTest(already_dispatched=already_dispatched):
                changes = dict(next_platform_campaign=dict(campaign_id='200', platform='windows-latest'))
                if already_dispatched:
                    changes['next_platform_dispatched'] = 'saved'
                api, saved, _, _, _ = self.finish_campaign(state_changes=changes)
                self.assertEqual(saved[-1]['status'], 'complete')
                self.assertFalse(any('/queue.yml' in call.args[1] for call in api.request.call_args_list))
                self.assertEqual(api.request.call_count, 0 if already_dispatched else 1)

    def test_request_failures_preserve_completion_without_uncertain_dispatch_retry(self):
        for method in ('GET', 'POST'):
            for failure in (parallel.APIError(503, 'Unavailable'), TimeoutError('No receipt')):
                with self.subTest(method=method, failure=type(failure).__name__):
                    api, saved, _, output, log = self.finish_campaign(request_error=failure, error_method=method)
                    self.assertEqual(saved[-1]['status'], 'complete')
                    self.assertFalse(saved[-1]['publication_pending'])
                    self.assertEqual(output['state'], 'complete')
                    self.assertEqual(api.request.call_count, 1 if method == 'GET' else 2)
                    self.assertEqual(sum(call.args[0] == 'POST' for call in api.request.call_args_list),
                                     0 if method == 'GET' else 1)
                    self.assertEqual(len(saved), 2)
                    self.assertIn('::warning::QUEUE_HANDOFF_UNCONFIRMED', log)

    def test_malformed_workflow_metadata_never_dispatches(self):
        api = Mock()
        api.request.return_value = None
        with patch('sys.stdout', new_callable=io.StringIO) as log:
            parallel.wake_ordered_queue(api, '100')
        api.request.assert_called_once_with('GET', '/actions/workflows/queue.yml')
        self.assertIn('::warning::QUEUE_HANDOFF_UNCONFIRMED', log.getvalue())


class DashboardTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'),'Node.js exercises the actual dashboard aggregation')
    def test_means_casewise_ranges_and_failed_samples(self):
        import report
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'index.html'
            report.render_report(path,[],{})
            text=path.read_text(encoding='utf-8')
        functions=text[text.index('function mean('):text.index('const measures=')]
        harness="""
const assert=require('node:assert/strict');
const validValue=(row,key)=>row.status==='ok'&&Number.isFinite(row[key]);
const rows=[];
for(const physical_id of ['p','q'])for(const [index,elapsed_seconds] of [1,2,9].entries())
 rows.push({software:'acousticts',source_sha:'a',physical_id,status:'ok',repetition:index+1,elapsed_seconds});
const complete=timingSummary(rows,2);
assert.equal(complete.total,8);assert.equal(complete.average,4);assert.deepEqual(complete.totalRange,[2,18]);assert.deepEqual(complete.averageRange,[1,9]);
rows[5].status='timeout';
const partial=timingSummary(rows,2);
assert.equal(partial.total,5.5);assert.deepEqual(partial.totalRange,[2,11]);assert.deepEqual(partial.averageRange,[1,5.5]);
assert.deepEqual(timingSummary(rows,3).totalRange,partial.totalRange);
assert.equal(timingSummary([...rows,{...rows[0],source_sha:'b',elapsed_seconds:10}],2).points.length,3);
const comparisons=[];
for(const [index,errors] of [[.03,.04],[.06,.08]].entries())for(const [physical,delta_ts_db] of errors.entries())
 comparisons.push({physical_id:String(physical),status:'ok',reference_id:'reference',repetition:index+1,delta_ts_db});
const error=repetitionRMSE(comparisons);
assert.equal(error.count,2);assert.equal(error.values.length,2);
assert.ok(Math.abs(error.value-(Math.sqrt(.00125)+Math.sqrt(.005))/2)<1e-14);
"""
        subprocess.run([shutil.which('node'),'-e',functions+harness],check=True,capture_output=True,text=True)


class RetryFailedTests(unittest.TestCase):
    def test_retry_replaces_failures_without_new_repetitions_or_changed_successes(self):
        baseline,sources,_=fixture()
        target=[row for row in baseline['rows'] if row['software']=='acousticts']
        target[0].update(status='worker_error',elapsed_seconds=None,ts_db=None,message='Invalid timer')
        target[1].update(status='nonconverged',elapsed_seconds=.3,ts_db=None)
        target[2].update(status='pending',elapsed_seconds=None,ts_db=None)
        before=copy.deepcopy(baseline)
        data,pending=parallel.prepare_dataset(baseline,dict(mode='retry',software='acousticts',additional_repetitions=5),sources)
        self.assertEqual({row['case_id'] for row in pending},{row['case_id'] for row in target[:2]})
        self.assertEqual({row['repetition'] for row in pending},{1})
        self.assertEqual(data['archived_rows'],target[:2])
        repaired={row['case_id']:row for row in data['rows']}
        for row in baseline['rows']:
            if row['case_id'] not in {r['case_id'] for r in target[:3]}:
                self.assertEqual(repaired[row['case_id']],row)
            self.assertEqual(repaired[row['case_id']]['reference_ts_db'],row['reference_ts_db'])
        self.assertEqual(baseline,before)
        changed=copy.deepcopy(sources)
        changed['sources']['acousticts']['sha']='f'*40
        with self.assertRaisesRegex(ValueError,'revision changed'):
            parallel.prepare_dataset(baseline,dict(mode='retry',software='acousticts'),changed)

    def test_retry_does_not_create_new_frequency_cases(self):
        baseline,sources,_=fixture()
        data,pending=parallel.prepare_dataset(baseline,dict(mode='retry',software='acousticts',
            frequency_start_khz=1,frequency_end_khz=9,frequency_interval_khz=2),sources)
        self.assertEqual(pending,[])
        self.assertEqual(data['archived_rows'],[])
        self.assertEqual({row['case_id'] for row in data['rows']},{row['case_id'] for row in baseline['rows']})

    def test_test_only_package_updates_do_not_change_calculation_compatibility(self):
        from refresh import _runtime_selection
        _,_,runtime=fixture()
        runtime['environment_compatibility']['r_packages'].update(
            compiler_config=dict(FC='gfortran14'),
            packages=[dict(Package='Rcpp',Version='1.1.0'),dict(Package='diffobj',Version='0.3.9'),
                      dict(Package='otel',Version='0.2.0')])
        updated=copy.deepcopy(runtime)
        updated['environment_compatibility']['r_packages']['packages'][1]['Version']='0.3.10'
        updated['environment_compatibility']['r_packages']['packages'][2]['Version']='0.3.0'
        self.assertEqual(_runtime_selection(runtime,'acousticts'),_runtime_selection(updated,'acousticts'))
        updated['environment_compatibility']['r_packages']['compiler_config']['FC']='different compiler'
        self.assertNotEqual(_runtime_selection(runtime,'acousticts'),_runtime_selection(updated,'acousticts'))

    def test_timer_dependency_does_not_relax_calculation_build_checks(self):
        from refresh import _runtime_selection
        _,_,runtime=fixture()
        runtime['environment_compatibility']['r_packages']['packages']=[dict(Package='Rcpp',Version='1.1.0')]
        updated=copy.deepcopy(runtime)
        updated['environment_compatibility']['r_packages']['packages'].append(dict(Package='microbenchmark',Version='1.5.0'))
        self.assertEqual(_runtime_selection(runtime,'acousticts'),_runtime_selection(updated,'acousticts'))
        self.assertEqual(len(updated['environment_compatibility']['r_packages']['packages']),2)
        updated['environment_compatibility']['r_packages']['packages'][0]['Version']='1.2.0'
        self.assertNotEqual(_runtime_selection(runtime,'acousticts'),_runtime_selection(updated,'acousticts'))


class PlatformTests(unittest.TestCase):
    def test_five_fresh_repetitions_archive_old_acousticts_and_keep_other_software(self):
        baseline,sources,_=fixture()
        sources['sources']['acousticts']['sha']='d'*40
        data,cases=parallel.prepare_dataset(baseline,dict(mode='refresh',software='acousticts',additional_repetitions=5),sources)
        old=[r for r in baseline['rows'] if r['software']=='acousticts']
        self.assertEqual(data['archived_rows'],old)
        self.assertEqual(len(cases),5*len(old))
        self.assertEqual({r['repetition'] for r in cases},{1,2,3,4,5})
        self.assertTrue(all(r['status']=='pending' for r in data['rows'] if r['software']=='acousticts'))
        self.assertEqual([r for r in data['rows'] if r['software']!='acousticts'],
                         [r for r in baseline['rows'] if r['software']!='acousticts'])
        self.assertTrue(all(r['reference_ts_db']==-20.001 for r in data['rows']))

    def test_repeat_on_empty_platform_starts_at_one(self):
        baseline,sources,_=fixture();baseline.update(rows=[],environments=[])
        data,cases=parallel.prepare_dataset(baseline,dict(mode='repeat',software='acousticts',additional_repetitions=5),sources)
        self.assertEqual({r['repetition'] for r in cases},{1,2,3,4,5})
        self.assertEqual(len(cases),5*72)
        self.assertEqual(data['compatibility']['acousticts'],[])

    def test_new_platform_plan_seeds_configuration_without_foreign_measurements(self):
        baseline,sources,_=fixture()
        for platform in ('windows-latest','macos-latest'):
            documents={};outputs=[]
            def get(api,path):return copy.deepcopy(documents.get(path)),None
            def put(api,path,value,*args,**kwargs):documents[path]=copy.deepcopy(value);return 's'
            def config(path):return sources if path.name=='sources.lock.json' else baseline['configuration']
            api=Mock(repository='owner/repo');api.request.return_value=dict(object=dict(sha='c'*40))
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as tmp, \
                 patch.dict(os.environ,GITHUB_SHA='c'*40,GITHUB_RUN_ID='100'), \
                 patch.object(parallel,'get_document',side_effect=get), \
                 patch.object(parallel,'put_document',side_effect=put), \
                 patch.object(parallel,'existing_snapshot',return_value=(None,None)) as snapshot, \
                 patch.object(core,'read_json',side_effect=config), \
                 patch.object(parallel,'read_packets',return_value={}), \
                 patch.object(parallel,'write_outputs',side_effect=outputs.append):
                parallel.plan_campaign(api,dict(platform=platform,mode='refresh',software='acousticts',
                    semimajor_axes='0.001,0.01,0.07,0.1',additional_repetitions=5),Path(tmp))
            plan=documents[parallel.filename('100','plan')]
            self.assertEqual(plan['platform'],platform)
            self.assertEqual(outputs[-1]['runner'],platform)
            self.assertEqual(len(plan['tasks']),4*2*3*4*5)
            self.assertTrue(all(call.args[1]==platform for call in snapshot.call_args_list))
            self.assertTrue(all(row['status']=='pending' for d in plan['datasets'].values() for row in d['rows']))
            self.assertTrue(all(not d['environments'] for d in plan['datasets'].values()))

    def test_saved_parallel_baseline_rejects_another_platform(self):
        plan,_,_=pending_plan();plan['platform']='windows-latest'
        with patch.object(parallel,'get_document',return_value=(plan,'s')):
            with self.assertRaisesRegex(ValueError,'another platform'):
                parallel.load_parallel(Mock(),'100',.07,'macos-latest')

    def test_platform_export_roundtrip_and_cross_platform_packet_rejection(self):
        import dataset
        for platform,(runner,system) in parallel.PLATFORMS.items():
            plan,task,runtime=pending_plan();plan['platform']=platform
            packet=packet_for(plan,task,runtime,3);packet['environments']['inv']['platform']=system
            parallel.validate_packet(plan,task,packet)
            bad=copy.deepcopy(packet);bad['environments']['inv']['platform']='invalid'
            with self.assertRaisesRegex(ValueError,'worker environment'):parallel.validate_packet(plan,task,bad)
            with self.subTest(platform=platform), tempfile.TemporaryDirectory() as tmp, \
                 patch.object(parallel.host_platform,'machine',return_value='arm64' if system=='Darwin' else 'AMD64'):
                directory=Path(tmp)
                parallel.export_task(plan,task,packet,runtime,directory)
                manifest=core.read_json(directory/'manifest.json')
                self.assertEqual(manifest['platform'],system)
                self.assertEqual(manifest['architecture'],'arm64' if system=='Darwin' else 'AMD64')
                archive=directory/'worker.zip'
                with zipfile.ZipFile(archive,'w') as stream:
                    for path in directory.rglob('*.json'):stream.write(path,path.relative_to(directory))
                connection=dataset.connect_database(directory/'test.sqlite')
                try:
                    result=dataset.import_artifact(connection,archive,dict(id=1,name=f"spectrum-{task['id']}-1",run_id=100),'owner/repo')
                    self.assertEqual(result['observations'],3)
                    self.assertEqual(connection.execute('SELECT DISTINCT platform FROM observations').fetchone()[0],platform)
                finally:connection.close()

    def test_platform_publication_and_archival_leave_other_platform_snapshots_untouched(self):
        for platform in ('windows-latest','macos-latest'):
            plan,task,runtime=pending_plan();plan.update(platform=platform,repository='owner/repo')
            data=plan['datasets']['0.07'];data['archived_rows']=[dict(software='acousticts',source_sha='old')]
            with self.subTest(platform=platform),tempfile.TemporaryDirectory() as tmp, \
                 patch.dict(os.environ,GITHUB_RUN_ID='100'), \
                 patch.object(parallel,'attach_latest_references',side_effect=lambda api,rows,size:rows), \
                 patch.object(parallel,'existing_snapshot',return_value=(None,None)) as read, \
                 patch.object(parallel,'publish') as publish, \
                 patch('report.render_report'):
                parallel.publish_datasets(Mock(repository='owner/repo'),plan,{},Path(tmp))
                self.assertEqual(read.call_args.args[1],platform)
                self.assertEqual(publish.call_args.args[1]['platform'],platform)
                archive=core.read_json(Path(tmp)/'0.07/archived-results.json')
                self.assertEqual(archive['rows'],data['archived_rows'])

    def test_continuation_preserves_windows_platform(self):
        plan,task,runtime=pending_plan();plan.update(platform='windows-latest',automatic_continue=True)
        state=dict(waves=[dict(run_id=100,tasks=[task['id']])],attempts={},blocked=[])
        api=Mock()
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,GITHUB_RUN_ID='100'), \
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
             patch.object(parallel,'recover_artifacts'),patch.object(parallel,'read_packets',return_value={}), \
             patch.object(parallel,'put_document'),patch.object(parallel,'publish_datasets'):
            parallel.finish(api,'100',Path(tmp))
        self.assertEqual(api.request.call_args.args[2]['inputs'],dict(campaign_id='100',platform='windows-latest'))


class FrequencySchedulingTests(unittest.TestCase):
    def documents(self, initial):
        documents = copy.deepcopy(initial)
        versions = {path: 1 for path in documents}
        def get(api, path):
            return copy.deepcopy(documents.get(path)), versions.get(path)
        def put(api, path, value, sha=None, **kwargs):
            if versions.get(path) != sha:
                raise parallel.DocumentConflict('stale writer')
            versions[path] = versions.get(path, 0) + 1
            documents[path] = copy.deepcopy(value)
            return versions[path]
        def read_task(api, plan, task):
            values = [value for name, value in documents.items()
                      if name == parallel.filename(plan['campaign_id'], 'task', task['id']) or
                      (name.startswith(f"spectra-{plan['campaign_id']}-worker-") and name.endswith(task['id'] + '.json'))]
            return parallel.combine_packets(plan, task, values)
        reader = patch.object(parallel, 'read_task_packet', side_effect=read_task)
        self.addCleanup(reader.stop)
        reader.start()
        return documents, get, put

    def two_sweeps(self):
        plan, task, runtime = pending_plan()
        config = plan['datasets']['0.07']['configuration']
        cases = [case for case in core.build_cases(config, 'full', ['acousticts'])
                 if case['boundary'] == 'rigid' and case['aspect_ratio'] == 5
                 and case['incidence_deg'] in (0, 37)]
        tasks = sorted(parallel.make_tasks('100', {'0.07': cases}), key=lambda task: task['name'])
        plan['tasks'] = tasks
        plan['datasets']['0.07']['rows'] = [dict(case, status='pending') for case in cases]
        return plan, tasks, runtime

    def test_active_policy_disables_deferral_and_uses_five_timeouts_at_300_seconds(self):
        self.assertEqual(parallel.execution_policy(), dict(case_timeout_seconds=300,
            scheduling='whole_sweep', frequency_deferral=False, consecutive_timeouts=5))

    def test_fresh_sweep_precedes_every_deferred_frequency(self):
        plan, tasks, runtime = self.two_sweeps()
        saved = packet_for(plan, tasks[0], runtime)
        next(iter(saved['records'].values()))['elapsed_seconds'] = 120
        original = copy.deepcopy(plan)
        queue, workers = legacy_scheduling_queue(plan, tasks, {tasks[0]['id']: saved}, 100, 16)
        self.assertEqual(plan, original)
        self.assertEqual(len(workers), 3)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        with patch.object(parallel, 'get_document', side_effect=get), patch.object(parallel, 'put_document', side_effect=put):
            first = parallel.change_work_queue(Mock(), plan, 100, workers[0])
            second = parallel.change_work_queue(Mock(), plan, 100, workers[1])
            retry = parallel.change_work_queue(Mock(), plan, 100, workers[1])
            third = parallel.change_work_queue(Mock(), plan, 100, workers[2])
        self.assertEqual(first['kind'], 'sweep')
        self.assertEqual(first['task_id'], tasks[1]['id'])
        self.assertEqual(second, retry)
        self.assertEqual(second['kind'], 'frequency')
        self.assertNotEqual(second['case_id'], third['case_id'])
        self.assertEqual(len(documents[path]['claims']), 3)

    def test_resumed_expensive_sweep_keeps_one_owner(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime)
        saved['slow_sweep'] = True
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: saved}, 100, 16)
        self.assertEqual(len(workers), 1)
        self.assertEqual(queue['pending'], [task['id']])
        self.assertEqual(queue['deferred'], [])
        self.assertEqual(queue['units'], {task['id']: dict(kind='sweep', task_id=task['id'])})
        self.assertFalse(queue['frequency_deferral'])

    def test_unstarted_sweep_precedes_resumption_of_a_partial_cheap_sweep(self):
        plan, tasks, runtime = self.two_sweeps()
        partial = packet_for(plan, tasks[0], runtime)
        queue, workers = legacy_scheduling_queue(plan, tasks, {tasks[0]['id']: partial}, 100, 2)
        self.assertEqual(queue['pending'], [tasks[1]['id'], tasks[0]['id']])

    def test_worker_finishes_complete_sweep_before_starting_next_sweep(self):
        plan, tasks, runtime = self.two_sweeps()
        queue, workers = parallel.scheduling_queue(plan, tasks, {}, 100, 1)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        wave = dict(run_id=100, queue=path, workers=workers, tasks=[task['id'] for task in tasks])
        expected = tasks[0]['cases'] + tasks[1]['cases']
        environment = packet_for(plan, tasks[0], runtime)['environments']['inv']
        calls = []
        def measure(case, prepared, directory, timeout):
            calls.append((case['case_id'], timeout))
            return dict(status='ok', elapsed_seconds=180.0 if len(calls) == 1 else .25, ts_db=-20.)
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, GITHUB_RUN_ID='100', GITHUB_SHA='c'*40), \
             patch.object(parallel.host_platform, 'system', return_value='Linux'), \
             patch.object(parallel, 'get_document', side_effect=get), \
             patch.object(parallel, 'put_document', side_effect=put), \
             patch.object(parallel, 'task_environment', return_value=environment), \
             patch.object(core, 'measure', side_effect=measure):
            root = Path(temporary)
            core.atomic_json(root / 'runtime.json', runtime)
            parallel.run_frequency_worker(Mock(), plan, {}, wave, workers[0], root / 'runtime.json',
                                          root / 'worker', float('inf'))
        self.assertEqual(calls, [(case['case_id'], 300) for case in expected])
        self.assertEqual(len({identity for identity, timeout in calls}), 6)
        for task in tasks:
            packet = documents[parallel.worker_packet_filename('100', 100, workers[0], task['id'])]
            parallel.validate_packet(plan, task, packet)
            self.assertEqual(packet['state'], 'complete')
            self.assertEqual(len(packet['records']), 3)
            self.assertTrue(all(row['case_timeout_seconds'] == 300 for row in packet['records'].values()))
            self.assertTrue(all(env['execution_policy'] == parallel.execution_policy()
                                for env in packet['environments'].values()))
        self.assertEqual(plan['datasets']['0.07']['configuration']['case_timeout_seconds'], 600)
        self.assertEqual(documents[path]['deferred'], [])
        self.assertTrue(all(unit['kind'] == 'sweep' for unit in documents[path]['units'].values()))

    def test_idle_worker_waits_for_active_sweep_and_then_takes_new_tail(self):
        plan, tasks, runtime = self.two_sweeps()
        queue, workers = legacy_scheduling_queue(plan, tasks, {}, 100, 2)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        with patch.object(parallel, 'get_document', side_effect=get), patch.object(parallel, 'put_document', side_effect=put):
            a = parallel.change_work_queue(Mock(), plan, 100, workers[0])
            b = parallel.change_work_queue(Mock(), plan, 100, workers[1])
            parallel.change_work_queue(Mock(), plan, 100, workers[1], b['id'], packet_for(plan, tasks[1], runtime, 3))
            self.assertEqual(parallel.change_work_queue(Mock(), plan, 100, workers[1]), dict(kind='wait'))
            slow = packet_for(plan, tasks[0], runtime)
            slow.update(slow_sweep=True, state='split_pending')
            parallel.change_work_queue(Mock(), plan, 100, workers[0], a['id'], slow)
            parallel.change_work_queue(Mock(), plan, 100, workers[0], a['id'], slow)
            tail = parallel.change_work_queue(Mock(), plan, 100, workers[1])
        self.assertEqual(tail['kind'], 'frequency')
        self.assertEqual(len(documents[path]['units']), 4)

    def test_concurrent_packet_merge_preserves_both_results_after_cas_conflict(self):
        plan, task, runtime = pending_plan()
        first = packet_for(plan, task, runtime)
        second = packet_for(plan, task, runtime, 2)
        second['records'].pop(task['cases'][0]['case_id'])
        second['state'] = 'fragment_complete'
        path = parallel.filename('100', 'task', task['id'])
        documents, get, put = self.documents({})
        writes = []
        def competing(api, name, value, sha=None, **kwargs):
            if not writes:
                writes.append(True)
                put(api, name, first)
            return put(api, name, value, sha, **kwargs)
        with patch.object(parallel, 'get_document', side_effect=get), \
             patch.object(parallel, 'put_document', side_effect=competing), patch.object(parallel.time, 'sleep'):
            result = parallel.merge_task_packet(Mock(), plan, task, second)
        self.assertEqual(len(result['records']), 2)
        self.assertEqual(result['records'][task['cases'][0]['case_id']], first['records'][task['cases'][0]['case_id']])
        self.assertNotEqual(result['state'], 'complete')
        final = packet_for(plan, task, runtime, 3)
        with patch.object(parallel, 'get_document', side_effect=get), patch.object(parallel, 'put_document', side_effect=put):
            result = parallel.merge_task_packet(Mock(), plan, task, final)
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(len(documents[path]['records']), 3)

    def test_conflicting_observation_cannot_replace_existing_measurement(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime)
        changed = copy.deepcopy(saved)
        next(iter(changed['records'].values()))['elapsed_seconds'] = 9
        with patch.object(parallel, 'get_document', return_value=(saved, 1)), \
             patch.object(parallel, 'put_document') as put:
            with self.assertRaisesRegex(ValueError, 'observations conflict'):
                parallel.merge_task_packet(Mock(), plan, task, changed)
        put.assert_not_called()

    def test_restart_retains_historical_timeout_inside_one_sweep(self):
        plan, task, runtime = pending_plan()
        packet = packet_for(plan, task, runtime)
        row = next(iter(packet['records'].values()))
        row.update(status='timeout', elapsed_seconds=None, wall_seconds=120,
                   case_timeout_seconds=120, solve_lower_bound_seconds=119)
        original = copy.deepcopy(packet)
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: packet}, 101, 16)
        self.assertEqual(queue['pending'], [task['id']])
        self.assertEqual(queue['deferred'], [])
        self.assertEqual(len(workers), 1)
        self.assertEqual(packet, original)
        self.assertEqual(plan['datasets']['0.07']['configuration']['case_timeout_seconds'], 600)

    def test_numerical_timeout_keeps_sweep_until_all_three_cases_finish(self):
        plan, task, runtime = pending_plan()
        documents, get, put = self.documents({})
        environment = packet_for(plan, task, runtime)['environments']['inv']
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, GITHUB_RUN_ID='100', GITHUB_SHA='c'*40), \
             patch.object(parallel.host_platform, 'system', return_value='Linux'), \
             patch.object(parallel, 'get_document', side_effect=get), \
             patch.object(parallel, 'put_document', side_effect=put), \
             patch.object(parallel, 'task_environment', return_value=environment), \
             patch.object(core, 'measure', return_value=dict(status='timeout', elapsed_seconds=None,
                          wall_seconds=300, solve_lower_bound_seconds=299)) as measure:
            root = Path(temporary)
            core.atomic_json(root / 'runtime.json', runtime)
            result = parallel.run_task(Mock(), '100', task['id'], root / 'runtime.json', root / 'worker',
                                       float('inf'), context=(plan, {}),
                                       work=dict(kind='sweep', run_id=100, worker_id=task['id']))
        self.assertEqual(measure.call_count, 3)
        self.assertEqual(measure.call_args.args[-1], 300)
        self.assertEqual(result['state'], 'complete')
        saved = documents[parallel.worker_packet_filename('100', 100, task['id'], task['id'])]
        self.assertEqual(len(saved['records']), 3)
        self.assertEqual(next(iter(saved['records'].values()))['status'], 'timeout')

    def test_failed_publication_never_releases_a_completed_sweep(self):
        plan, tasks, runtime = self.two_sweeps()
        queue, workers = parallel.scheduling_queue(plan, tasks, {}, 100, 1)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        def outage(api, name, value, sha=None, **kwargs):
            if '-task-' in name or '-worker-' in name:
                raise parallel.APIError(503, 'unavailable')
            return put(api, name, value, sha, **kwargs)
        wave = dict(run_id=100, queue=path, workers=workers, tasks=[task['id'] for task in tasks])
        environment = packet_for(plan, tasks[0], runtime)['environments']['inv']
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, GITHUB_RUN_ID='100', GITHUB_SHA='c'*40), \
             patch.object(parallel.host_platform, 'system', return_value='Linux'), \
             patch.object(parallel, 'get_document', side_effect=get), \
             patch.object(parallel, 'put_document', side_effect=outage), \
             patch.object(parallel, 'task_environment', return_value=environment), \
             patch.object(core, 'measure', return_value=dict(status='ok', elapsed_seconds=120, ts_db=-20.)) as measure:
            root = Path(temporary)
            core.atomic_json(root / 'runtime.json', runtime)
            parallel.run_frequency_worker(Mock(), plan, {}, wave, workers[0], root / 'runtime.json',
                                          root / 'worker', float('inf'))
            local = core.read_json(root / 'worker' / tasks[0]['id'] / 'checkpoint.json')
        self.assertEqual(measure.call_count, 3)
        self.assertEqual(len(local['records']), 3)
        self.assertEqual(documents[path]['owners'][workers[0]], tasks[0]['id'])
        self.assertEqual(documents[path]['pending'], [tasks[1]['id']])

    def test_artifacts_merge_disjoint_frequency_owners_without_losing_shared_rows(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime, 3)
        saved['records'] = {task['cases'][2]['case_id']: saved['records'][task['cases'][2]['case_id']]}
        saved.update(state='fragment_complete', slow_sweep=True)
        queue, workers = legacy_scheduling_queue(plan, [task], {task['id']: saved}, 100, 2)
        queue_path = parallel.queue_filename('100', 100)
        task_path = parallel.filename('100', 'task', task['id'])
        documents, get, put = self.documents({queue_path: queue, task_path: saved})
        archives, artifacts = {}, []
        with patch.object(parallel, 'get_document', side_effect=get), patch.object(parallel, 'put_document', side_effect=put):
            for index, worker in enumerate(workers):
                unit = parallel.change_work_queue(Mock(), plan, 100, worker)
                packet = packet_for(plan, task, runtime, 3)
                packet['records'] = {identity: row for identity, row in packet['records'].items()
                                     if identity in (unit['case_id'], task['cases'][2]['case_id'])}
                packet['state'] = 'fragment_complete'
                archive = io.BytesIO()
                with zipfile.ZipFile(archive, 'w') as stream:
                    stream.writestr('spectrum/' + task['id'] + '/checkpoint.json', json.dumps(packet))
                archives[index] = archive.getvalue()
                artifacts.append(dict(id=index, name=f'spectrum-{worker}-1'))
            api = Mock()
            api.archive.side_effect = lambda identity: archives[identity]
            wave = dict(run_id=100, tasks=[task['id']], queue=queue_path, workers=workers)
            with patch.object(parallel, 'all_artifacts', return_value=artifacts):
                parallel.recover_artifacts(api, plan, wave)
                completed = copy.deepcopy(documents[task_path]['records'])
                parallel.recover_artifacts(api, plan, wave)
                self.assertEqual(documents[task_path]['records'], completed)
        result = documents[task_path]
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(len(result['records']), 3)
        self.assertEqual(result['records'][task['cases'][2]['case_id']], saved['records'][task['cases'][2]['case_id']])

    def test_unowned_unpublished_artifact_record_is_rejected(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime)
        saved.update(slow_sweep=True)
        queue, workers = legacy_scheduling_queue(plan, [task], {task['id']: saved}, 100, 2)
        queue_path = parallel.queue_filename('100', 100)
        task_path = parallel.filename('100', 'task', task['id'])
        documents, get, put = self.documents({queue_path: queue, task_path: saved})
        with patch.object(parallel, 'get_document', side_effect=get), patch.object(parallel, 'put_document', side_effect=put):
            parallel.change_work_queue(Mock(), plan, 100, workers[0])
            # The artifact invents the unclaimed final frequency.
            packet = packet_for(plan, task, runtime, 3)
            archive = io.BytesIO()
            with zipfile.ZipFile(archive, 'w') as stream:
                stream.writestr('spectrum/' + task['id'] + '/checkpoint.json', json.dumps(packet))
            api = Mock()
            api.archive.return_value = archive.getvalue()
            artifact = dict(id=1, name=f'spectrum-{workers[0]}-1')
            with patch.object(parallel, 'all_artifacts', return_value=[artifact]):
                with self.assertRaisesRegex(ValueError, 'another frequency owner'):
                    parallel.recover_artifacts(api, plan, dict(run_id=100, tasks=[task['id']], queue=queue_path,
                                                             workers=workers))
        self.assertEqual(documents[task_path], saved)

    def test_lost_queue_write_acknowledgement_cannot_duplicate_work(self):
        plan, task, runtime = pending_plan()
        queue, workers = legacy_scheduling_queue(plan, [task], {}, 100, 1)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        lost = [True]
        def acknowledgement_lost(api, name, value, sha=None, **kwargs):
            result = put(api, name, value, sha, **kwargs)
            if lost.pop() if lost else False:
                raise parallel.APIError(500, 'response lost')
            return result
        with patch.object(parallel, 'get_document', side_effect=get), \
             patch.object(parallel, 'put_document', side_effect=acknowledgement_lost), \
             patch.object(parallel.time, 'sleep'):
            unit = parallel.change_work_queue(Mock(), plan, 100, workers[0])
            slow = packet_for(plan, task, runtime)
            slow.update(slow_sweep=True, state='split_pending')
            lost.append(True)
            parallel.change_work_queue(Mock(), plan, 100, workers[0], unit['id'], slow)
        self.assertEqual(len(documents[path]['claims']), 1)
        self.assertEqual(len(documents[path]['deferred']), 2)
        self.assertEqual(len(set(documents[path]['deferred'])), 2)
        self.assertIsNone(documents[path]['owners'][workers[0]])


class DocumentStorageTests(unittest.TestCase):
    def api(self):
        import base64
        import hashlib
        files, blobs, trees, commits = {}, {}, {}, {}
        state = {'head': 'initial'}
        def digest(raw):
            return hashlib.sha1(f'blob {len(raw)}\0'.encode()+raw).hexdigest()
        def record(raw):
            return dict(encoding='base64', content=base64.b64encode(raw).decode(), sha=digest(raw))
        def request(method, path, body=None):
            if method == 'GET' and path.startswith('/contents/'):
                name = path.removeprefix('/contents/').split('?')[0]
                if name not in files:
                    raise parallel.APIError(404, 'missing')
                return record(files[name])
            if method == 'GET' and path.startswith('/git/ref/'):
                return dict(object=dict(sha=state['head']))
            if method == 'GET' and path.startswith('/git/commits/'):
                return dict(tree=dict(sha='current-tree'))
            if method == 'GET' and path.startswith('/git/trees/'):
                return dict(tree=[dict(path=name,sha=digest(raw),type='blob') for name,raw in files.items()])
            if method == 'GET' and path.startswith('/git/blobs/'):
                return record(blobs[path.split('/')[-1]])
            if method == 'POST' and path == '/git/blobs':
                raw = base64.b64decode(body['content'])
                sha = digest(raw)
                blobs[sha] = raw
                return dict(sha=sha)
            if method == 'POST' and path == '/git/trees':
                sha = 'tree'+str(len(trees))
                trees[sha] = dict(files)
                for entry in body['tree']:
                    trees[sha][entry['path']] = blobs[entry['sha']]
                return dict(sha=sha)
            if method == 'POST' and path == '/git/commits':
                sha = 'commit'+str(len(commits))
                commits[sha] = body
                return dict(sha=sha)
            if method == 'PATCH' and path.startswith('/git/refs/'):
                commit = commits[body['sha']]
                if commit['parents'] != [state['head']]:
                    raise parallel.APIError(409, 'competing writer')
                files.clear()
                files.update(trees[commit['tree']])
                state['head'] = body['sha']
                return dict(object=dict(sha=body['sha']))
            raise AssertionError((method,path))
        return Mock(request=Mock(side_effect=request)), files

    def test_large_plan_roundtrip_preserves_all_values_and_immutability(self):
        api, files = self.api()
        plan, _, _ = pending_plan()
        with patch.object(parallel, 'DOCUMENT_PART_BYTES', 180):
            sha = parallel.put_document(api, 'plan.json', plan, immutable=True)
            self.assertEqual(parallel.get_document(api, 'plan.json'), (plan, sha))
            self.assertGreater(len(files), 2)
            before = dict(files)
            self.assertEqual(parallel.put_document(api, 'plan.json', plan, immutable=True), sha)
            self.assertEqual(files, before)
            with self.assertRaisesRegex(ValueError, 'replace an existing'):
                parallel.put_document(api, 'plan.json', dict(plan, changed=True), immutable=True)
            self.assertEqual(parallel.get_document(api, 'plan.json'), (plan, sha))

    def test_missing_corrupt_and_reordered_parts_are_rejected(self):
        for mode in ('missing', 'corrupt', 'reordered'):
            api, files = self.api()
            with patch.object(parallel, 'DOCUMENT_PART_BYTES', 40):
                parallel.put_document(api, 'plan.json', dict(values=list(range(30))))
            manifest = json.loads(files['plan.json'])
            path = 'spectra-part-' + manifest['parts'][0] + '.part'
            if mode == 'missing':
                del files[path]
            elif mode == 'corrupt':
                files[path] += b'!'
            else:
                manifest['parts'].reverse()
                files['plan.json'] = json.dumps(manifest).encode()
            with self.assertRaises(ValueError):
                parallel.get_document(api, 'plan.json')

    def test_failed_part_upload_leaves_no_visible_plan_and_can_resume(self):
        api, files = self.api()
        original = api.request.side_effect
        uploads = []
        def fail(method, path, body=None):
            if method == 'POST' and path == '/git/blobs':
                uploads.append(path)
            if method == 'POST' and path == '/git/blobs' and len(uploads) == 2:
                raise parallel.APIError(500, 'interrupted')
            return original(method, path, body)
        api.request.side_effect = fail
        value = dict(values=list(range(50)))
        with patch.object(parallel, 'DOCUMENT_PART_BYTES', 40):
            with self.assertRaises(parallel.APIError):
                parallel.put_document(api, 'plan.json', value)
            self.assertEqual(parallel.get_document(api, 'plan.json'), (None, None))
            api.request.side_effect = original
            parallel.put_document(api, 'plan.json', value)
            self.assertEqual(parallel.get_document(api, 'plan.json')[0], value)

    def test_legacy_single_document_remains_readable(self):
        api, files = self.api()
        value = dict(status='active', completed=['saved'])
        parallel.put_document(api, 'state.json', value)
        self.assertEqual(set(files), {'state.json'})
        self.assertEqual(parallel.get_document(api, 'state.json')[0], value)

    def test_atomic_publication_rebases_seven_competing_writes(self):
        import base64
        api, files = self.api()
        original = api.request.side_effect
        conflicts = []
        def competing(method, path, body=None):
            if method == 'PATCH' and len(conflicts) < 7:
                conflicts.append(1)
                sha = original('POST', '/git/blobs', dict(content=base64.b64encode(str(len(conflicts)).encode()).decode()))['sha']
                tree = original('POST', '/git/trees', dict(tree=[dict(path='other-platform.json',sha=sha)]))
                head = original('GET', '/git/ref/heads/data')['object']['sha']
                commit = original('POST', '/git/commits', dict(tree=tree['sha'],parents=[head]))
                original(method,path,dict(sha=commit['sha'],force=False))
                raise parallel.APIError(409,'competing platform')
            return original(method,path,body)
        api.request.side_effect = competing
        with patch.object(parallel.time,'sleep'),patch.object(parallel,'DOCUMENT_PART_BYTES',40):
            parallel.put_document(api,'plan.json',dict(saved=list(range(25))),immutable=True)
        self.assertEqual(files['other-platform.json'],b'7')
        self.assertEqual(parallel.get_document(api,'plan.json')[0],dict(saved=list(range(25))))


class GitDataStorageTests(unittest.TestCase):
    def test_data_tree_preserves_missing_parent_blob_without_hydration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            def git(*arguments, data=None, check=True):
                return subprocess.run(['git', *arguments], cwd=root, input=data,
                                      check=check, capture_output=True)
            git('init')
            missing = 'b' * 40
            tree = git('mktree', '--missing', data=f'100644 blob {missing}\thistorical.json\n'.encode()).stdout.decode().strip()
            blob = git('hash-object', '-w', '--stdin', data=b'{"saved":true}').stdout.decode().strip()
            store = parallel.GitDataStore(root)
            result = store.request('POST', '/git/trees', dict(base_tree=tree,
                tree=[dict(path='new.json', sha=blob)]))
            entries = git('ls-tree', result['sha']).stdout.decode()
            self.assertIn(missing, entries)
            self.assertIn(blob, entries)
            self.assertNotEqual(git('cat-file', '-e', missing, check=False).returncode, 0)
            self.assertEqual(git('cat-file', 'blob', blob).stdout, b'{"saved":true}')

    def test_git_timeout_invalidates_cache_and_retains_safe_retry_error(self):
        store = parallel.GitDataStore('.')
        store.fetched = 100
        with patch.object(parallel.subprocess, 'run', side_effect=subprocess.TimeoutExpired('secret', 180)), \
             self.assertRaises(parallel.APIError) as raised:
            store.git('write-tree')
        self.assertEqual(raised.exception.status, 503)
        self.assertLess(store.fetched, 0)
        self.assertNotIn('secret', str(raised.exception))

    def test_rate_limit_metadata_does_not_echo_response_credentials(self):
        from urllib.error import HTTPError
        error=HTTPError('https://api.github.com/repos/owner/repo/private',403,'Forbidden',
            {'x-ratelimit-remaining':'0','x-ratelimit-reset':'2000000000'},
            io.BytesIO(b'{"message":"API rate limit exceeded, secret token"}'))
        api=parallel.GitHub('owner/repo','private-token')
        with patch.object(api.opener,'open',side_effect=error),self.assertRaises(parallel.APIError) as raised:
            api.request('GET','/contents/private')
        self.assertTrue(raised.exception.rate_limited)
        self.assertEqual(raised.exception.rate_remaining,0)
        self.assertEqual(raised.exception.rate_reset,2000000000)
        self.assertNotIn('secret',str(raised.exception))
        self.assertNotIn('private-token',str(raised.exception))

    def test_existing_checkout_publishes_parts_without_checkout_or_rest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'source'
            remote=Path(temporary)/'remote.git'
            root.mkdir()
            def git(*args):
                return subprocess.run(['git',*args],cwd=root,check=True,capture_output=True).stdout.decode().strip()
            git('init','--bare',str(remote))
            git('init')
            git('config','user.name','test')
            git('config','user.email','test@example.invalid')
            (root/'source.txt').write_text('source remains unchanged')
            git('add','source.txt')
            git('commit','-m','source fixture')
            original=git('rev-parse','HEAD')
            git('remote','add','origin',str(remote))
            git('push','origin',f'HEAD:refs/heads/{parallel.BRANCH}')
            api=parallel.GitDataStore(root)
            value=dict(values=list(range(100)),sources={'acousticts':'pinned'})
            with patch.object(parallel,'DOCUMENT_PART_BYTES',80):
                parallel.put_document(api,'spectra-100-plan.json',value,immutable=True)
            self.assertEqual(parallel.get_document(api,'spectra-100-plan.json')[0],value)
            other=parallel.GitDataStore(root)
            parallel.put_document(other,'other-platform.json',dict(saved=True))
            parallel.put_document(api,'state.json',dict(status='active'))
            self.assertEqual(parallel.get_document(api,'other-platform.json')[0],dict(saved=True))
            self.assertEqual(git('rev-parse','HEAD'),original)
            self.assertEqual(git('status','--porcelain'),'')
            self.assertEqual((root/'source.txt').read_text(),'source remains unchanged')
            self.assertFalse((root/'state.json').exists())

    def test_git_data_routes_exclude_github_actions_and_source_control_metadata(self):
        store=parallel.GitDataStore('.')
        self.assertTrue(store.handles('GET','/contents/state.json?ref='+parallel.BRANCH))
        self.assertTrue(store.handles('PATCH','/git/refs/heads/'+parallel.BRANCH))
        self.assertFalse(store.handles('GET','/actions/runs/100/jobs'))
        self.assertFalse(store.handles('POST','/actions/workflows/run.yml/dispatches'))
        self.assertFalse(store.handles('GET','/contents/sources.lock.json?ref=main'))
        self.assertFalse(store.handles('PATCH','/git/refs/heads/main'))


class IdleOwnerLivenessTests(unittest.TestCase):
    def fixture(self):
        workers = ['a' * 24, 'b' * 24]
        wave = dict(run_id=100, queue='queue.json', workers=workers,
                    worker_names=['worker 01', 'worker 02'])
        queue = dict(schema_version=2, campaign_id='100', run_id=100,
                     owners={workers[0]: 'claimed-frequency', workers[1]: None},
                     pending=[], deferred=[])
        jobs = [dict(name='worker 01', status='completed', conclusion='failure'),
                dict(name='worker 02', status='in_progress', conclusion=None)]
        return dict(campaign_id='100'), wave, queue, jobs

    def test_terminal_owner_releases_idle_job_without_reclaiming_assignment(self):
        plan, wave, queue, jobs = self.fixture()
        original = copy.deepcopy(queue)
        api = Mock()
        api.request.return_value = dict(jobs=jobs)
        with patch.object(parallel, 'get_document', return_value=(queue, 'sha')) as get, \
             patch.object(parallel, 'put_document') as put:
            self.assertTrue(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))
        api.request.assert_called_once_with('GET', '/actions/runs/100/jobs?per_page=100')
        get.assert_called_once_with(api, 'queue.json')
        put.assert_not_called()
        self.assertEqual(queue, original)

    def test_live_missing_duplicate_or_unknown_owner_keeps_idle_job_waiting(self):
        for mode in ('live', 'missing', 'duplicate', 'unknown', 'malformed_jobs'):
            with self.subTest(mode=mode):
                plan, wave, queue, jobs = self.fixture()
                if mode == 'live':
                    jobs[0]['status'] = 'in_progress'
                elif mode == 'missing':
                    jobs.pop(0)
                elif mode == 'duplicate':
                    jobs.append(copy.deepcopy(jobs[0]))
                elif mode == 'unknown':
                    queue['owners']['c' * 24] = 'unknown-frequency'
                else:
                    jobs = None
                api = Mock()
                api.request.return_value = dict(jobs=jobs)
                with patch.object(parallel, 'get_document', return_value=(queue, 'sha')):
                    self.assertFalse(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))

    def test_work_enqueued_during_job_lookup_prevents_idle_exit(self):
        for field in ('pending', 'deferred'):
            with self.subTest(field=field):
                plan, wave, queue, jobs = self.fixture()
                events = []
                def jobs_read(method, path):
                    events.append('jobs')
                    queue[field].append('new-assignment')
                    return dict(jobs=jobs)
                def queue_read(api, path):
                    events.append('queue')
                    return copy.deepcopy(queue), 'sha'
                api = Mock(request=Mock(side_effect=jobs_read))
                with patch.object(parallel, 'get_document', side_effect=queue_read):
                    self.assertFalse(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))
                self.assertEqual(events, ['jobs', 'queue'])

    def test_new_active_owner_seen_after_job_lookup_prevents_idle_exit(self):
        plan, wave, queue, jobs = self.fixture()
        def jobs_read(method, path):
            queue['owners'][wave['workers'][1]] = 'new-assignment'
            return dict(jobs=jobs)
        api = Mock(request=Mock(side_effect=jobs_read))
        with patch.object(parallel, 'get_document', return_value=(queue, 'sha')):
            self.assertFalse(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))

    def test_owner_status_is_polled_at_most_once_per_minute(self):
        plan, wave, queue, jobs = self.fixture()
        active = copy.deepcopy(jobs)
        active[0]['status'] = 'in_progress'
        api = Mock()
        api.request.side_effect = [dict(jobs=active), dict(jobs=jobs)]
        cache = {}
        with patch.object(parallel.time, 'monotonic', side_effect=[0, 15, 59, 60]), \
             patch.object(parallel, 'get_document', return_value=(queue, 'sha')) as get:
            observed = [parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], cache)
                        for _ in range(4)]
        self.assertEqual(observed, [False, False, False, True])
        self.assertEqual(api.request.call_count, 2)
        self.assertEqual(get.call_count, 2)

    def test_network_failures_remain_waiting_and_do_not_modify_claims(self):
        errors = [parallel.APIError(503, 'Unavailable'), TimeoutError('Unavailable'), OSError('Unavailable')]
        for error in errors:
            for stage in ('jobs', 'queue'):
                with self.subTest(error=type(error).__name__, stage=stage):
                    plan, wave, queue, jobs = self.fixture()
                    api = Mock()
                    api.request.return_value = dict(jobs=jobs)
                    if stage == 'jobs':
                        api.request.side_effect = error
                    with patch.object(parallel, 'get_document', side_effect=error if stage == 'queue' else None,
                                      return_value=(queue, 'sha')), patch.object(parallel, 'put_document') as put:
                        self.assertFalse(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))
                    put.assert_not_called()

    def test_missing_mapping_and_foreign_queue_never_authorize_exit(self):
        mutations = [lambda wave, queue: wave.pop('worker_names'),
                     lambda wave, queue: wave.update(worker_names=['duplicate', 'duplicate']),
                     lambda wave, queue: queue.update(campaign_id='other'),
                     lambda wave, queue: queue.update(run_id=101),
                     lambda wave, queue: queue.pop('pending')]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                plan, wave, queue, jobs = self.fixture()
                mutate(wave, queue)
                api = Mock()
                api.request.return_value = dict(jobs=jobs)
                with patch.object(parallel, 'get_document', return_value=(queue, 'sha')):
                    self.assertFalse(parallel.idle_owners_finished(api, plan, wave, wave['workers'][1], {}))

    def test_idle_worker_exits_for_artifact_recovery_after_owner_job_finishes(self):
        plan, task, runtime = pending_plan()
        _, wave, queue, jobs = self.fixture()
        wave['tasks'] = [task['id']]
        api = Mock()
        api.request.return_value = dict(jobs=jobs)
        with tempfile.TemporaryDirectory() as temporary, \
             patch.object(parallel, 'change_work_queue', return_value=dict(kind='wait')) as claim, \
             patch.object(parallel, 'get_document', return_value=(queue, 'sha')), \
             patch.object(parallel, 'run_task') as run, patch.object(parallel.time, 'sleep') as sleep:
            parallel.run_frequency_worker(api, plan, {}, wave, wave['workers'][1],
                                          Path(temporary) / 'runtime.json', Path(temporary), float('inf'))
        claim.assert_called_once()
        run.assert_not_called()
        sleep.assert_not_called()







# Publication repair and timeout-tail regressions.
import base64
import hashlib
def long_plan(repetitions=1):
    baseline, sources, runtime = fixture()
    configuration = copy.deepcopy(baseline["configuration"])
    configuration["frequency"]["stop_hz"] = 49000
    configuration["repetitions"] = repetitions
    cases = [case for case in core.build_cases(configuration, "full", ["acousticts"])
             if case["boundary"] == "rigid" and case["aspect_ratio"] == 5 and case["incidence_deg"] == 37]
    tasks = parallel.make_tasks("100", {"0.07": cases})
    data = dict(baseline, configuration=configuration,
                rows=[dict(case, status="pending") for case in cases], compatibility={})
    return dict(campaign_id="100", source_ref="c" * 40, sources=sources, tasks=tasks,
                datasets={"0.07": data}), runtime


def actual_packet(plan, task, runtime, indices, timeout=True):
    packet = packet_for(plan, task, runtime, len(task["cases"]))
    chosen = {task["cases"][index]["case_id"] for index in indices}
    packet["records"] = {identity: row for identity, row in packet["records"].items() if identity in chosen}
    packet["state"] = "interrupted"
    for row in packet["records"].values():
        row["prediction_attempted"] = True
        if timeout:
            row.update(status="timeout", elapsed_seconds=None, ts_db=None,
                       wall_seconds=300, solve_lower_bound_seconds=299, case_timeout_seconds=300)
    return packet


def documents(initial):
    saved = copy.deepcopy(initial)
    versions = {path: 1 for path in saved}

    def get(api, path):
        return copy.deepcopy(saved.get(path)), versions.get(path)

    def put(api, path, value, sha=None, **kwargs):
        if versions.get(path) != sha:
            raise parallel.DocumentConflict("stale writer")
        versions[path] = versions.get(path, 0) + 1
        saved[path] = copy.deepcopy(value)
        return versions[path]

    return saved, get, put


def git_reader(saved):
    blobs = {}

    def tree(api):
        entries = {}
        for path, packet in saved.items():
            raw = json.dumps(parallel.pack(packet), separators=(",", ":")).encode()
            sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
            entries[path] = dict(sha=sha)
            blobs[sha] = raw
        return "head", "tree", entries

    api = Mock()
    api.request.side_effect = lambda method, path: dict(
        encoding="base64", content=base64.b64encode(blobs[path.rsplit("/", 1)[-1]]).decode())
    return api, tree


@patch.object(parallel, 'timeout_streak', new=legacy_timeout_streak)
class TimeoutTailRegressionTests(unittest.TestCase):



    def test_later_trigger_remains_valid_after_an_earlier_streak_completes(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        packet = actual_packet(plan, task, runtime, range(12, 22))
        later = parallel.timeout_streak(task, packet["records"])
        basis = {key: later[key] for key in ("rule", "trigger_case_ids")}
        row = actual_packet(plan, task, runtime, [24])["records"][task["cases"][24]["case_id"]]
        row = {key: value for key, value in row.items()
               if key not in ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db")}
        row.update(prediction_attempted=False, timeout_basis=basis)
        packet["records"][row["case_id"]] = row
        parallel.validate_packet(plan, task, packet)
        packet["records"].update(actual_packet(plan, task, runtime, range(10))["records"])
        self.assertEqual(parallel.timeout_streak(task, packet["records"])["after_frequency_hz"], 19000)
        self.assertEqual(parallel.timeout_basis_frequency(task, packet["records"], basis), 43000)
        parallel.validate_packet(plan, task, packet)
        reversed_basis = dict(basis, trigger_case_ids=list(reversed(basis["trigger_case_ids"])))
        with self.assertRaises(ValueError):
            parallel.timeout_basis_frequency(task, packet["records"], reversed_basis)

    def test_tail_reservation_excludes_a_real_prediction_already_in_flight(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        early = actual_packet(plan, task, runtime, range(9))
        queue, workers = legacy_scheduling_queue(plan, [task], {task["id"]: early}, 100, 3)
        path = parallel.queue_filename("100", 100)
        saved, get, put = documents({path: queue})
        with patch.object(parallel, "get_document", side_effect=get), \
             patch.object(parallel, "put_document", side_effect=put):
            tenth = parallel.change_work_queue(Mock(), plan, 100, workers[0])
            active = parallel.change_work_queue(Mock(), plan, 100, workers[1])
            completed = actual_packet(plan, task, runtime, range(10))
            completed["state"] = "fragment_complete"
            parallel.change_work_queue(Mock(), plan, 100, workers[0], tenth["id"], completed)
            tail = parallel.change_work_queue(Mock(), plan, 100, workers[2])
            self.assertEqual(tail["kind"], "timeout_tail")
            self.assertEqual(tail["case_ids"], [case["case_id"] for case in task["cases"][11:]])
            self.assertNotIn(active["case_id"], tail["case_ids"])
            self.assertEqual(saved[path]["owners"][workers[1]], active["id"])
            self.assertEqual(saved[path]["claims"][active["id"]], workers[1])
            self.assertEqual(parallel.change_work_queue(Mock(), plan, 100, workers[0]), dict(kind="wait"))

    def test_inferred_rows_reject_measured_timing_error_amplitude_and_memory_fields(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        packet = actual_packet(plan, task, runtime, range(10))
        cutoff = parallel.timeout_streak(task, packet["records"])
        identity = task["cases"][10]["case_id"]
        row = actual_packet(plan, task, runtime, [10])["records"][identity]
        row = {key: value for key, value in row.items()
               if key not in ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db")}
        row.update(prediction_attempted=False,
                   timeout_basis={key: cutoff[key] for key in ("rule", "trigger_case_ids")})
        packet["records"][identity] = row
        parallel.validate_packet(plan, task, packet)
        for field in ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db",
                      "delta_ts_db", "amplitude_real_m", "amplitude_imag_m",
                      "memory_delta_peak_bytes", "memory_samples"):
            with self.subTest(field=field):
                corrupt = copy.deepcopy(packet)
                corrupt["records"][identity][field] = 0
                with self.assertRaises(ValueError):
                    parallel.validate_packet(plan, task, corrupt)

    def test_tail_reservation_rechecks_active_claims_after_compare_and_swap_conflict(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        packet = actual_packet(plan, task, runtime, range(10))
        queue, workers = legacy_scheduling_queue(plan, [task], {task["id"]: packet}, 100, 2)
        path = parallel.queue_filename("100", 100)
        saved, get, put = documents({path: queue})
        active = task["cases"][12]["case_id"]
        raced = []

        def concurrent(api, target, value, sha=None, **kwargs):
            if not raced:
                raced.append(True)
                competing = copy.deepcopy(saved[path])
                competing["deferred"].remove(active)
                competing["owners"][workers[1]] = active
                competing["claims"][active] = workers[1]
                put(api, target, competing, sha)
                raise parallel.DocumentConflict("active claim arrived before tail reservation")
            return put(api, target, value, sha, **kwargs)

        with patch.object(parallel, "get_document", side_effect=get), \
             patch.object(parallel, "put_document", side_effect=concurrent), \
             patch.object(parallel.time, "sleep"):
            tail = parallel.change_work_queue(Mock(), plan, 100, workers[0])
        self.assertNotIn(active, tail["case_ids"])
        self.assertEqual(saved[path]["owners"][workers[1]], active)
        self.assertEqual(len(tail["case_ids"]), len(task["cases"]) - 11)

    def test_timeout_tail_emits_no_prediction_measurements_and_preserves_saved_actual_rows(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        packet = actual_packet(plan, task, runtime, range(10))
        actual = actual_packet(plan, task, runtime, [11], timeout=False)
        packet["records"].update(actual["records"])
        basis = parallel.timeout_streak(task, packet["records"])
        selected = [task["cases"][index]["case_id"] for index in (10, 12)]
        work = dict(kind="timeout_tail", task_id=task["id"], case_ids=selected, run_id=100,
                    worker_id="d" * 24,
                    timeout_basis={key: basis[key] for key in ("rule", "trigger_case_ids")})
        published = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            core.atomic_json(root / "runtime.json", runtime)
            with patch.dict(os.environ, GITHUB_RUN_ID="100", GITHUB_RUN_ATTEMPT="1", GITHUB_SHA="c" * 40), \
                 patch.object(parallel.host_platform, "system", return_value="Linux"), \
                 patch.object(parallel, "read_task_packet", return_value=copy.deepcopy(packet)), \
                 patch.object(parallel, "get_document", return_value=(None, None)), \
                 patch.object(parallel, "task_environment", return_value=packet["environments"]["inv"]), \
                 patch.object(parallel, "publish_worker_packet",
                              side_effect=lambda api, plan, task, value, work: published.append(copy.deepcopy(value))), \
                 patch.object(core, "measure", side_effect=AssertionError("Tail must not run numerical software")) as measure:
                result = parallel.run_task(Mock(), "100", task["id"], root / "runtime.json",
                                           root / "worker", float("inf"), context=(plan, {}), work=work)
            measure.assert_not_called()
            for identity in selected:
                row = result["records"][identity]
                self.assertIs(row["prediction_attempted"], False)
                self.assertEqual(row["status"], "timeout")
                self.assertEqual(row["timeout_basis"], work["timeout_basis"])
                self.assertTrue(all(row.get(key) is None for key in
                                    ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db",
                                     "amplitude_real_m", "amplitude_imag_m")))
            for identity, row in packet["records"].items():
                self.assertEqual(result["records"][identity], row)
            self.assertEqual(len(result["records"]), len(packet["records"]) + 2)
            self.assertEqual(published[-1]["records"], result["records"])
            parallel.validate_packet(plan, task, result)


@patch.object(parallel, 'timeout_streak', new=legacy_timeout_streak)
class WorkerPublicationRegressionTests(unittest.TestCase):
    def test_worker_files_merge_with_legacy_results_and_cache_discovers_new_shards(self):
        plan, task, runtime = pending_plan()
        old = packet_for(plan, task, runtime)
        legacy_path = parallel.filename("100", "task", task["id"])
        saved, get, put = documents({legacy_path: old})
        api, tree = git_reader(saved)
        with patch.object(parallel, "get_document", side_effect=get), \
             patch.object(parallel, "put_document", side_effect=put), \
             patch.object(parallel, "_progress_tree", side_effect=tree):
            cache = parallel.read_packets(api, plan)
            self.assertEqual(len(cache[task["id"]]["records"]), 1)
            for index in (1, 2):
                packet = packet_for(plan, task, runtime, 3)
                packet["records"] = {task["cases"][index]["case_id"]: packet["records"][task["cases"][index]["case_id"]]}
                packet["state"] = "fragment_complete"
                work = dict(run_id=100, worker_id=str(index) * 24)
                next(iter(packet['records'].values()))['measurement_worker_id'] = work['worker_id']
                original = copy.deepcopy(packet)
                parallel.publish_worker_packet(api, plan, task, packet, work)
                self.assertEqual(packet, original)
                cache = parallel.read_packets(api, plan, cache=cache)
                self.assertEqual(len(cache[task["id"]]["records"]), index + 1)
            merged = cache[task["id"]]
            self.assertEqual(merged["state"], "complete")
            self.assertEqual(merged["records"][task["cases"][0]["case_id"]], old["records"][task["cases"][0]["case_id"]])
            self.assertNotIn("publication_owner", merged)
            self.assertEqual(saved[legacy_path], old)
            self.assertEqual(len(saved), 3)

    def test_conflicting_worker_rows_environments_sources_and_owners_are_rejected(self):
        plan, task, runtime = pending_plan()
        old = packet_for(plan, task, runtime)
        for change in ("elapsed_seconds", "environment", "source_ref"):
            with self.subTest(change=change):
                other = copy.deepcopy(old)
                if change == "elapsed_seconds":
                    next(iter(other["records"].values()))["elapsed_seconds"] = 99
                elif change == "environment":
                    other["environments"]["inv"]["processor"] = "different allocation"
                else:
                    other["source_ref"] = "f" * 40
                with self.assertRaises(ValueError):
                    parallel.combine_packets(plan, task, [old, other])
        path = parallel.worker_packet_filename("100", 101, "d" * 24, task["id"])
        saved = {path: dict(old, publication_owner=dict(run_id=102, worker_id="d" * 24))}
        api, tree = git_reader(saved)
        with patch.object(parallel, "_progress_tree", side_effect=tree), \
             self.assertRaisesRegex(ValueError, "publication owner"):
            parallel.read_packets(api, plan)

    def test_interrupted_tail_artifact_recovers_owned_subset_and_keeps_active_real_result(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        current = actual_packet(plan, task, runtime, range(10))
        active = actual_packet(plan, task, runtime, [10], timeout=False)
        current["records"].update(active["records"])
        basis = parallel.timeout_streak(task, current["records"])
        worker = "d" * 24
        tail_ids = [case["case_id"] for case in task["cases"][11:]]
        recovered = copy.deepcopy(current)
        first = actual_packet(plan, task, runtime, [11])["records"][tail_ids[0]]
        first = {key: value for key, value in first.items()
                 if key not in ("elapsed_seconds", "wall_seconds", "solve_lower_bound_seconds", "ts_db")}
        first.update(prediction_attempted=False,
                     timeout_basis={key: basis[key] for key in ("rule", "trigger_case_ids")})
        recovered["records"][tail_ids[0]] = first
        recovered["state"] = "interrupted"
        queue_path = parallel.queue_filename("100", 100)
        task_path = parallel.filename("100", "task", task["id"])
        queue = dict(schema_version=2, claims={"tail": worker}, units={
            "tail": dict(kind="timeout_tail", task_id=task["id"], case_ids=tail_ids)})
        saved, get, put = documents({queue_path: queue, task_path: current})
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as stream:
            stream.writestr("spectrum/" + task["id"] + "/checkpoint.json", json.dumps(recovered))
        api = Mock()
        api.archive.return_value = archive.getvalue()
        artifact = dict(id=1, name=f"spectrum-{worker}-1",
                        digest="sha256:" + hashlib.sha256(archive.getvalue()).hexdigest())
        wave = dict(run_id=100, tasks=[task["id"]], queue=queue_path, workers=[worker])
        with patch.object(parallel, "get_document", side_effect=get), \
             patch.object(parallel, "put_document", side_effect=put), \
             patch.object(parallel, "read_task_packet", side_effect=lambda api, plan, task: copy.deepcopy(saved[task_path])), \
             patch.object(parallel, "all_artifacts", return_value=[artifact]):
            parallel.recover_artifacts(api, plan, wave)
            first_recovery = copy.deepcopy(saved[task_path])
            parallel.recover_artifacts(api, plan, wave)
        self.assertEqual(saved[task_path], first_recovery)
        self.assertEqual(saved[task_path]["records"][tail_ids[0]], first)
        self.assertEqual(saved[task_path]["records"][task["cases"][10]["case_id"]],
                         active["records"][task["cases"][10]["case_id"]])
        self.assertEqual(len(saved[task_path]["records"]), len(current["records"]) + 1)

    def test_recovery_rejects_conflict_with_existing_worker_shard_before_legacy_write(self):
        plan, task, runtime = pending_plan()
        current = packet_for(plan, task, runtime, 2)
        legacy = packet_for(plan, task, runtime, 1)
        owned = task["cases"][1]["case_id"]
        worker = "d" * 24
        saved = copy.deepcopy(current)
        saved["records"][owned]["elapsed_seconds"] = 99
        saved["state"] = "interrupted"
        queue = dict(schema_version=2, claims={owned: worker}, units={
            owned: dict(kind="frequency", task_id=task["id"], case_id=owned)})
        queue_path = parallel.queue_filename("100", 100)
        task_path = parallel.filename("100", "task", task["id"])
        stored, get, put = documents({queue_path: queue, task_path: legacy})
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as stream:
            stream.writestr("spectrum/" + task["id"] + "/checkpoint.json", json.dumps(saved))
        api = Mock()
        api.archive.return_value = archive.getvalue()
        artifact = dict(id=1, name=f"spectrum-{worker}-1")
        wave = dict(run_id=100, tasks=[task["id"]], queue=queue_path, workers=[worker])
        with patch.object(parallel, "get_document", side_effect=get), \
             patch.object(parallel, "put_document", side_effect=put) as write, \
             patch.object(parallel, "read_task_packet", return_value=current), \
             patch.object(parallel, "all_artifacts", return_value=[artifact]):
            with self.assertRaises(ValueError):
                parallel.recover_artifacts(api, plan, wave)
            write.assert_not_called()
        self.assertEqual(stored[task_path], legacy)










# Sparse publication throughput regressions.
"""Sparse publication, immutable cache, and exact consolidation regressions."""
import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import zipfile
from unittest.mock import Mock, patch

import test_parallel_campaign as fixtures

parallel = fixtures.parallel


class ThroughputStore:
    def __init__(self, documents):
        self.documents = copy.deepcopy(documents)
        self.blobs = {}
        self.api = SimpleNamespace(request=Mock(side_effect=self.request))
        self.writes = []

    def entry(self, path):
        raw = json.dumps(parallel.pack(self.documents[path]), separators=(",", ":")).encode()
        sha = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
        self.blobs[sha] = raw
        return dict(sha=sha)

    def tree(self, api):
        return "head", "tree", {path: self.entry(path) for path in self.documents}

    def request(self, method, path):
        if method != "GET" or not path.startswith("/git/blobs/"):
            raise AssertionError((method, path))
        return dict(encoding="base64", content=base64.b64encode(self.blobs[path.rsplit("/", 1)[-1]]).decode())

    def get(self, api, path):
        if path not in self.documents:
            return None, None
        return copy.deepcopy(self.documents[path]), self.entry(path)["sha"]

    def put(self, api, path, value, sha=None, **kwargs):
        current_sha = self.entry(path)["sha"] if path in self.documents else None
        if current_sha != sha:
            raise parallel.DocumentConflict("stale canonical checkpoint")
        self.documents[path] = copy.deepcopy(value)
        self.writes.append(path)
        return self.entry(path)["sha"]


def throughput_packet(plan, task, runtime, indices, worker="1" * 24):
    packet = fixtures.actual_packet(plan, task, runtime, indices, timeout=False)
    packet["environments"] = {}
    for identity, row in packet["records"].items():
        invocation = "inv-" + identity
        row.update(measurement_worker_id=worker, invocation_id=invocation, allocation_id=invocation)
        packet["environments"][invocation] = dict(platform="Linux", processor="Test CPU",
            github_run_id="100", github_run_attempt="1", runtime=copy.deepcopy(runtime),
            diagnostic_padding=identity * 16)
    packet["publication_owner"] = dict(run_id=100, worker_id=worker)
    return packet


class SparsePublicationTests(unittest.TestCase):
    def test_payload_contains_only_owned_rows_and_referenced_environments(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        packet = throughput_packet(plan, task, runtime, range(20), worker="2" * 24)
        identities = list(packet["records"])
        packet["records"][identities[0]]["measurement_worker_id"] = "1" * 24
        original = copy.deepcopy(packet)
        store = ThroughputStore({})
        work = dict(run_id=100, worker_id="1" * 24)
        path = parallel.worker_packet_filename("100", 100, work["worker_id"], task["id"])
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            parallel.publish_worker_packet(store.api, plan, task, packet, work)
            saved = store.documents[path]
            self.assertEqual(set(saved["records"]), {identities[0]})
            self.assertEqual(set(saved["environments"]), {packet["records"][identities[0]]["invocation_id"]})
            self.assertLess(len(json.dumps(saved)), len(json.dumps(packet)) / 4)
            self.assertEqual(packet, original)
            packet["records"][identities[1]]["measurement_worker_id"] = "1" * 24
            parallel.publish_worker_packet(store.api, plan, task, packet, work)
        self.assertEqual(set(store.documents[path]["records"]), set(identities[:2]))
        self.assertEqual(len(store.documents[path]["environments"]), 2)
        self.assertEqual(store.documents[path]["publication_owner"], work)

    def test_other_run_records_cannot_enter_current_worker_fragment(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        packet = throughput_packet(plan, task, runtime, [0])
        store = ThroughputStore({})
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            result = parallel.publish_worker_packet(store.api, plan, task, packet,
                                                     dict(run_id=101, worker_id="1" * 24))
        self.assertIsNone(result)
        self.assertEqual(store.writes, [])

    def test_sparse_tail_fragment_uses_external_proof_without_fabricated_metrics(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        proof = fixtures.actual_packet(plan, task, runtime, range(10))
        tail = throughput_packet(plan, task, runtime, [12])
        row = next(iter(tail["records"].values()))
        row.pop("elapsed_seconds")
        row.pop("ts_db")
        row.update(status="timeout", prediction_attempted=False,
                   timeout_basis=dict(rule="ten_consecutive_prediction_timeouts",
                                      trigger_case_ids=[case["case_id"] for case in task["cases"][:10]]))
        full = parallel.combine_packets(plan, task, [proof, tail])
        canonical = parallel.filename("100", "task", task["id"])
        store = ThroughputStore({canonical: proof})
        work = dict(run_id=100, worker_id="1" * 24)
        path = parallel.worker_packet_filename("100", 100, work["worker_id"], task["id"])
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put), \
             patch.object(parallel, "_progress_tree", side_effect=store.tree):
            parallel.publish_worker_packet(store.api, plan, task, full, work)
            self.assertEqual(set(store.documents[path]["records"]), {row["case_id"]})
            restored = parallel.read_task_packet(store.api, plan, task)
        self.assertEqual(restored["records"], full["records"])
        with self.assertRaises(ValueError):
            parallel.validate_packet(plan, task, store.documents[path])
        bad = copy.deepcopy(proof)
        next(iter(bad["records"].values()))["status"] = "setup_timeout"
        with self.assertRaises(ValueError):
            parallel.combine_packets(plan, task, [bad, store.documents[path]])
        for metric in ("elapsed_seconds", "ts_db", "wall_seconds", "delta_ts_db", "memory_peak_rss_bytes"):
            with self.subTest(metric=metric):
                fabricated = copy.deepcopy(store.documents[path])
                fabricated["records"][row["case_id"]][metric] = 0
                with self.assertRaises(ValueError):
                    parallel.validate_packet(plan, task, fabricated, check_tail_evidence=False)


class PacketCacheAndConsolidationTests(unittest.TestCase):
    def fixture(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        canonical = parallel.filename("100", "task", task["id"])
        first = parallel.worker_packet_filename("100", 100, "1" * 24, task["id"])
        second = parallel.worker_packet_filename("100", 100, "2" * 24, task["id"])
        store = ThroughputStore({canonical: throughput_packet(plan, task, runtime, [0]),
            first: throughput_packet(plan, task, runtime, [1]),
            second: throughput_packet(plan, task, runtime, [2], worker="2" * 24)})
        return plan, task, runtime, canonical, first, second, store

    def test_repeated_task_reads_reuse_blobs_without_exposing_cached_mutability(self):
        plan, task, runtime, canonical, first, second, store = self.fixture()
        with patch.object(parallel, "_progress_tree", side_effect=store.tree):
            original = parallel.read_task_packet(store.api, plan, task)
            self.assertEqual(store.api.request.call_count, 3)
            snapshot = copy.deepcopy(original)
            next(iter(original["records"].values()))["ts_db"] = 999
            next(iter(original["environments"].values()))["runtime"]["source_shas"]["acousticts"] = "f" * 40
            repeated = parallel.read_task_packet(store.api, plan, task)
            self.assertEqual(repeated, snapshot)
            self.assertEqual(store.api.request.call_count, 3)
            store.documents[second] = throughput_packet(plan, task, runtime, [2, 3], worker="2" * 24)
            changed = parallel.read_task_packet(store.api, plan, task)
        self.assertEqual(len(changed["records"]), 4)
        self.assertEqual(store.api.request.call_count, 4)

    def test_consolidation_preserves_exact_union_and_skips_integrated_blob_downloads(self):
        plan, task, runtime, canonical, first, second, store = self.fixture()
        original_fragments = {path: copy.deepcopy(store.documents[path]) for path in (first, second)}
        expected_shas = {path: store.entry(path)["sha"] for path in (first, second)}
        with patch.object(parallel, "_progress_tree", side_effect=store.tree), \
             patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            packets = parallel.read_packets(store.api, plan)
            before = copy.deepcopy(packets[task["id"]])
            parallel.consolidate_worker_packets(store.api, plan, packets)
            saved = store.documents[canonical]
            self.assertEqual(saved["records"], before["records"])
            self.assertEqual(saved["environments"], before["environments"])
            self.assertEqual(saved["integrated_worker_packets"], expected_shas)
            self.assertEqual({path: store.documents[path] for path in original_fragments}, original_fragments)
            self.assertFalse(any(key.startswith("_") for key in saved))
            store.api = SimpleNamespace(request=Mock(side_effect=store.request))
            restored = parallel.read_task_packet(store.api, plan, task)
            self.assertEqual(store.api.request.call_count, 1)
            self.assertEqual(restored["records"], before["records"])
            parallel.consolidate_worker_packets(store.api, plan, {task["id"]: restored})
        self.assertEqual(store.writes, [canonical])

    def test_changed_fragment_after_consolidation_remains_visible(self):
        plan, task, runtime, canonical, first, second, store = self.fixture()
        with patch.object(parallel, "_progress_tree", side_effect=store.tree), \
             patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            packets = parallel.read_packets(store.api, plan)
            parallel.consolidate_worker_packets(store.api, plan, packets)
            store.documents[first] = throughput_packet(plan, task, runtime, [1, 3])
            store.api = SimpleNamespace(request=Mock(side_effect=store.request))
            restored = parallel.read_task_packet(store.api, plan, task)
        self.assertEqual(set(restored["records"]), {case["case_id"] for case in task["cases"][:4]})
        self.assertEqual(store.api.request.call_count, 2)

    def test_changed_fragment_cannot_replace_saved_data_or_change_source(self):
        for mutation in ("duplicate", "source", "owner"):
            with self.subTest(mutation=mutation):
                plan, task, runtime, canonical, first, second, store = self.fixture()
                with patch.object(parallel, "_progress_tree", side_effect=store.tree), \
                     patch.object(parallel, "get_document", side_effect=store.get), \
                     patch.object(parallel, "put_document", side_effect=store.put):
                    packets = parallel.read_packets(store.api, plan)
                    parallel.consolidate_worker_packets(store.api, plan, packets)
                    fragment = store.documents[first]
                    if mutation == "duplicate":
                        next(iter(fragment["records"].values()))["ts_db"] = -99
                    elif mutation == "source":
                        fragment["source_ref"] = "f" * 40
                    else:
                        fragment["publication_owner"]["worker_id"] = "3" * 24
                    with self.assertRaises(ValueError):
                        parallel.read_task_packet(store.api, plan, task)

    def test_consolidation_preserves_canonical_rows_saved_after_initial_read(self):
        plan, task, runtime, canonical, first, second, store = self.fixture()
        with patch.object(parallel, "_progress_tree", side_effect=store.tree), \
             patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            packets = parallel.read_packets(store.api, plan)
            extra = throughput_packet(plan, task, runtime, [3])
            store.documents[canonical] = parallel.combine_packets(plan, task, [store.documents[canonical], extra])
            parallel.consolidate_worker_packets(store.api, plan, packets)
        self.assertEqual(set(store.documents[canonical]["records"]),
                         {case["case_id"] for case in task["cases"][:4]})

    def test_cache_budget_evicts_least_recent_blob_and_rejects_oversized_entry(self):
        plan, task, runtime, canonical, first, second, store = self.fixture()
        paths = (canonical, first, second)
        shas = [store.entry(path)["sha"] for path in paths]
        sizes = [len(json.dumps(store.documents[path], separators=(",", ":")).encode()) for path in paths]
        budget = max(sizes) * 2
        with patch.object(parallel, "PACKET_CACHE_BYTES", budget):
            for index in (0, 1, 0, 2):
                parallel.read_packet_blob(store.api, shas[index])
            cache = store.api.__dict__["_packet_blob_cache"]
            self.assertLessEqual(cache["bytes"], budget)
            self.assertEqual(set(cache["entries"]), {shas[0], shas[2]})
            parallel.read_packet_blob(store.api, shas[1])
            self.assertEqual(store.api.request.call_count, 4)
        store.api = SimpleNamespace(request=Mock(side_effect=store.request))
        with patch.object(parallel, "PACKET_CACHE_BYTES", 1):
            parallel.read_packet_blob(store.api, shas[0])
            parallel.read_packet_blob(store.api, shas[0])
        self.assertEqual(store.api.request.call_count, 2)
        self.assertEqual(store.api.__dict__["_packet_blob_cache"]["bytes"], 0)


@patch.object(parallel, 'timeout_streak', new=legacy_timeout_streak)
class AtomicCompletionAndPublicationTests(unittest.TestCase):
    def test_combined_completion_and_claim_survives_lost_ack_without_duplicate_assignment(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        initial = fixtures.actual_packet(plan, task, runtime, [], timeout=False)
        initial["slow_sweep"] = True
        queue, workers = legacy_scheduling_queue(plan, [task], {task["id"]: initial}, 100, 1)
        path = parallel.queue_filename("100", 100)
        store = ThroughputStore({path: queue})
        lost = [False]

        def put(api, name, value, sha=None, **kwargs):
            result = store.put(api, name, value, sha, **kwargs)
            if lost[0]:
                lost[0] = False
                raise parallel.APIError(500, "write acknowledgement lost")
            return result

        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=put), \
             patch.object(parallel.time, "sleep"):
            first = parallel.change_work_queue(store.api, plan, 100, workers[0])
            completed = fixtures.actual_packet(plan, task, runtime, [0], timeout=False)
            completed["state"] = "fragment_complete"
            before = len(store.writes)
            lost[0] = True
            following = parallel.change_work_queue(store.api, plan, 100, workers[0],
                                                     first["id"], completed, claim_next=True)
            retry = parallel.change_work_queue(store.api, plan, 100, workers[0],
                                                 first["id"], completed, claim_next=True)
        self.assertEqual(following, retry)
        self.assertNotEqual(first["id"], following["id"])
        self.assertEqual(len(store.writes) - before, 1)
        self.assertEqual(len(store.documents[path]["claims"]), 2)
        self.assertEqual(store.documents[path]["owners"][workers[0]], following["id"])
        self.assertEqual(set(store.documents[path]["finished"]), {first["id"]})

    def test_timeout_trigger_assembles_from_separate_stale_completed_packets(self):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        initial = fixtures.actual_packet(plan, task, runtime, [], timeout=False)
        initial["slow_sweep"] = True
        queue, workers = legacy_scheduling_queue(plan, [task], {task["id"]: initial}, 100, 10)
        path = parallel.queue_filename("100", 100)
        store = ThroughputStore({path: queue})
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            units = [parallel.change_work_queue(store.api, plan, 100, worker) for worker in workers]
            for index in (9, 0, 8, 1, 7, 2, 6, 3, 5, 4):
                packet = fixtures.actual_packet(plan, task, runtime, [index])
                packet["state"] = "fragment_complete"
                self.assertEqual(len(packet["records"]), 1)
                parallel.change_work_queue(store.api, plan, 100, workers[index], units[index]["id"], packet)
                if index != 4:
                    self.assertNotIn(task["id"], store.documents[path].get("timeout_streaks", {}))
            tail = parallel.change_work_queue(store.api, plan, 100, workers[0])
        ordered = [case["case_id"] for case in task["cases"][:10]]
        self.assertEqual(set(store.documents[path]["actual_timeouts"][task["id"]]), set(ordered))
        self.assertEqual(tail["kind"], "timeout_tail")
        self.assertEqual(tail["timeout_basis"]["trigger_case_ids"], ordered)
        self.assertEqual(tail["case_ids"], [case["case_id"] for case in task["cases"][10:]])

    def test_slow_single_frequency_publishes_exactly_once_after_measurement(self):
        plan, task, runtime = fixtures.pending_plan()
        store = ThroughputStore({})
        worker = "1" * 24
        work = dict(kind="frequency", run_id=100, worker_id=worker,
                    case_id=task["cases"][0]["case_id"])
        environment = fixtures.packet_for(plan, task, runtime)["environments"]["inv"]
        clock = [0.0]

        def measure(*args):
            clock[0] += 305.0
            return dict(status="timeout", elapsed_seconds=None, wall_seconds=300,
                        solve_lower_bound_seconds=299)

        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, GITHUB_RUN_ID="100", GITHUB_SHA="c" * 40), \
             patch.object(parallel.host_platform, "system", return_value="Linux"), \
             patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put), \
             patch.object(parallel, "read_task_packet", return_value=None), \
             patch.object(parallel, "task_environment", return_value=environment), \
             patch.object(parallel.time, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(parallel.core, "measure", side_effect=measure) as prediction:
            root = Path(temporary)
            parallel.core.atomic_json(root / "runtime.json", runtime)
            result = parallel.run_task(store.api, "100", task["id"], root / "runtime.json",
                                       root / "worker", float("inf"), context=(plan, {}), work=work)
        prediction.assert_called_once()
        self.assertEqual(prediction.call_args.args[-1], 300)
        self.assertEqual(store.writes, [parallel.worker_packet_filename("100", 100, worker, task["id"])])
        self.assertTrue(result["_publication_complete"])
        self.assertEqual(result["_execution_timings"]["process_seconds"], 305)
        saved = store.documents[store.writes[0]]
        self.assertEqual(set(saved["records"]), {work["case_id"]})
        self.assertEqual(saved["records"][work["case_id"]]["status"], "timeout")


class ArtifactRecoveryCacheTests(unittest.TestCase):
    def test_complete_published_spectrum_skips_oversized_archive(self):
        plan, task, runtime, current, store, wave, units = self.fixture()
        current = fixtures.actual_packet(plan, task, runtime, range(len(task["cases"])), timeout=False)
        current["state"] = "complete"
        self.assertEqual(current["state"], "complete")
        artifacts = [dict(id=1, name=f"spectrum-{wave['workers'][0]}-1", size_in_bytes=7*1024**3)]
        store.api.archive = Mock(side_effect=AssertionError("Published results need no archive"))
        with patch.object(parallel,"get_document",side_effect=store.get), \
             patch.object(parallel,"all_artifacts",return_value=artifacts), \
             patch.object(parallel,"read_task_packet",return_value=current), \
             patch.object(parallel,"merge_task_packet") as merge:
            parallel.recover_artifacts(store.api,plan,wave)
        store.api.archive.assert_not_called()
        merge.assert_not_called()
        self.assertEqual(store.writes,[])

    def test_complete_marker_cannot_hide_missing_or_invalid_observations(self):
        for mutation in ("missing", "source"):
            with self.subTest(mutation=mutation):
                plan, task, runtime, current, store, wave, units = self.fixture()
                current = fixtures.actual_packet(plan, task, runtime, range(len(task["cases"])), timeout=False)
                current["state"] = "complete"
                if mutation == "missing":
                    current["records"].pop(next(iter(current["records"])))
                else:
                    current["source_ref"] = "f" * 40
                artifacts=[dict(id=1,name=f"spectrum-{wave['workers'][0]}-1")]
                with patch.object(parallel,"get_document",side_effect=store.get), \
                     patch.object(parallel,"all_artifacts",return_value=artifacts), \
                     patch.object(parallel,"read_task_packet",return_value=current):
                    with self.assertRaises(ValueError):
                        parallel.recover_artifacts(store.api,plan,wave)

    def fixture(self, worker_count=2, recorded=range(16)):
        plan, runtime = fixtures.long_plan()
        task = plan["tasks"][0]
        initial = fixtures.actual_packet(plan, task, runtime, [], timeout=False)
        initial["slow_sweep"] = True
        queue, workers = legacy_scheduling_queue(plan, [task], {task["id"]: initial}, 100, worker_count)
        queue_path = parallel.queue_filename("100", 100)
        task_path = parallel.filename("100", "task", task["id"])
        current = fixtures.actual_packet(plan, task, runtime, recorded, timeout=False)
        store = ThroughputStore({queue_path: queue, task_path: current})
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put):
            units = [parallel.change_work_queue(store.api, plan, 100, worker) for worker in workers]
        store.writes.clear()
        wave = dict(run_id=100, tasks=[task["id"]], queue=queue_path, workers=workers)
        return plan, task, runtime, current, store, wave, units

    def archives(self, task, items):
        artifacts, archives = [], {}
        attempts = {}
        for index, (worker, packet) in enumerate(items):
            attempts[worker] = attempts.get(worker, 0) + 1
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w") as archive:
                archive.writestr("spectrum/" + task["id"] + "/checkpoint.json", json.dumps(packet))
            archives[index] = stream.getvalue()
            artifacts.append(dict(id=index, name=f"spectrum-{worker}-{attempts[worker]}",
                                  digest="sha256:" + hashlib.sha256(archives[index]).hexdigest()))
        return artifacts, archives

    def test_known_copied_artifacts_reuse_task_read_and_perform_zero_writes(self):
        plan, task, runtime, current, store, wave, units = self.fixture(worker_count=16)
        artifacts, archives = self.archives(task, [(worker, current) for worker in wave["workers"]])
        store.api.archive = Mock(side_effect=lambda identity: archives[identity])
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put), \
             patch.object(parallel, "read_task_packet", return_value=copy.deepcopy(current)) as read, \
             patch.object(parallel, "merge_task_packet", wraps=parallel.merge_task_packet) as merge, \
             patch.object(parallel, "all_artifacts", return_value=artifacts):
            parallel.recover_artifacts(store.api, plan, wave)
        self.assertEqual(read.call_count, 1)
        self.assertEqual(store.api.archive.call_count, 16)
        merge.assert_not_called()
        self.assertEqual(store.writes, [])
        self.assertEqual(store.documents[parallel.filename("100", "task", task["id"])], current)

    def test_idempotent_skip_cannot_bypass_conflict_provenance_or_ownership_checks(self):
        for mutation in ("record", "environment", "source", "unowned", "archive_digest"):
            with self.subTest(mutation=mutation):
                plan, task, runtime, current, store, wave, units = self.fixture()
                saved = copy.deepcopy(current)
                if mutation == "record":
                    saved["records"][units[0]["case_id"]]["ts_db"] = -99
                elif mutation == "environment":
                    saved["environments"]["inv"]["processor"] = "Conflicting CPU"
                elif mutation == "source":
                    saved["source_ref"] = "f" * 40
                elif mutation == "unowned":
                    extra = fixtures.actual_packet(plan, task, runtime, [20], timeout=False)
                    saved["records"].update(extra["records"])
                artifacts, archives = self.archives(task, [(wave["workers"][0], saved)])
                if mutation == "archive_digest":
                    artifacts[0]["digest"] = "sha256:" + "0" * 64
                store.api.archive = Mock(side_effect=lambda identity: archives[identity])
                with patch.object(parallel, "get_document", side_effect=store.get), \
                     patch.object(parallel, "put_document", side_effect=store.put), \
                     patch.object(parallel, "read_task_packet", return_value=copy.deepcopy(current)), \
                     patch.object(parallel, "all_artifacts", return_value=artifacts):
                    with self.assertRaises(ValueError):
                        parallel.recover_artifacts(store.api, plan, wave)
                self.assertEqual(store.writes, [])

    def test_recovery_cache_updates_after_new_observation_and_skips_duplicate_attempt(self):
        plan, task, runtime, current, store, wave, units = self.fixture(recorded=[24])
        first = fixtures.actual_packet(plan, task, runtime, [0], timeout=False)
        second = fixtures.actual_packet(plan, task, runtime, [1], timeout=False)
        artifacts, archives = self.archives(task, [(wave["workers"][0], first),
            (wave["workers"][1], second), (wave["workers"][0], first)])
        store.api.archive = Mock(side_effect=lambda identity: archives[identity])
        with patch.object(parallel, "get_document", side_effect=store.get), \
             patch.object(parallel, "put_document", side_effect=store.put), \
             patch.object(parallel, "read_task_packet", return_value=copy.deepcopy(current)) as read, \
             patch.object(parallel, "all_artifacts", return_value=artifacts):
            parallel.recover_artifacts(store.api, plan, wave)
        self.assertEqual(read.call_count, 1)
        self.assertEqual(len(store.writes), 2)
        saved = store.documents[parallel.filename("100", "task", task["id"])]
        self.assertEqual(set(saved["records"]), {task["cases"][index]["case_id"] for index in (0, 1, 24)})
        for packet in (current, first, second):
            for identity, row in packet["records"].items():
                self.assertEqual(saved["records"][identity], row)


class WholeSweepPolicyTests(unittest.TestCase):
    def execute(self, measure, current=None, run_worker=False):
        plan, runtime = fixtures.long_plan()
        task = plan['tasks'][0]
        task_path = parallel.filename('100', 'task', task['id'])
        store = ThroughputStore({task_path: current} if current else {})
        environment = fixtures.packet_for(plan, task, runtime)['environments']['inv']
        queue_path = parallel.queue_filename('100', 100)
        if run_worker:
            queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: current} if current else {}, 100, 16)
            store.documents[queue_path] = queue
            worker = workers[0]
            wave = dict(run_id=100, tasks=[task['id']], workers=workers, queue=queue_path)
        else:
            worker = '1' * 24
        calls = []
        def measured(case, runtime, directory, timeout):
            calls.append(case['case_id'])
            self.assertEqual(timeout, 300)
            if run_worker:
                queue = store.documents[queue_path]
                self.assertEqual(queue['owners'][worker], task['id'])
                self.assertEqual(queue['claims'], {task['id']: worker})
                self.assertEqual(queue['finished'], {})
                self.assertEqual(queue['deferred'], [])
            return measure(len(calls), case)
        with tempfile.TemporaryDirectory() as temporary, \
             patch.dict(os.environ, GITHUB_RUN_ID='100', GITHUB_SHA='c' * 40), \
             patch.object(parallel.host_platform, 'system', return_value='Linux'), \
             patch.object(parallel, 'get_document', side_effect=store.get), \
             patch.object(parallel, 'put_document', side_effect=store.put), \
             patch.object(parallel, 'read_task_packet', return_value=copy.deepcopy(current)), \
             patch.object(parallel, 'task_environment', return_value=environment), \
             patch.object(parallel.core, 'measure', side_effect=measured), \
             patch.object(parallel, 'change_work_queue', wraps=parallel.change_work_queue) as queue_change:
            root = Path(temporary)
            parallel.core.atomic_json(root / 'runtime.json', runtime)
            if run_worker:
                parallel.run_frequency_worker(store.api, plan, {}, wave, worker, root / 'runtime.json',
                                              root / 'worker', float('inf'))
                packet = parallel.core.read_json(root / 'worker' / task['id'] / 'checkpoint.json')
                self.assertEqual(queue_change.call_count, 2)
                self.assertEqual(store.writes.count(queue_path), 2)
            else:
                packet = parallel.run_task(store.api, '100', task['id'], root / 'runtime.json',
                    root / 'worker', float('inf'), context=(plan, {}),
                    work=dict(kind='sweep', run_id=100, worker_id=worker))
        return plan, task, packet, calls

    @staticmethod
    def timeout(index, case):
        return dict(status='timeout', elapsed_seconds=None, wall_seconds=300,
                    solve_lower_bound_seconds=299)

    def test_five_timeouts_infer_remaining_rows_without_releasing_sweep(self):
        plan, task, packet, calls = self.execute(self.timeout, run_worker=True)
        self.assertEqual(calls, [case['case_id'] for case in task['cases'][:5]])
        self.assertEqual(packet['state'], 'complete')
        self.assertEqual(len(packet['records']), len(task['cases']))
        trigger = [case['case_id'] for case in task['cases'][:5]]
        for case in task['cases'][5:]:
            row = packet['records'][case['case_id']]
            self.assertEqual(row['status'], 'timeout')
            self.assertIs(row['prediction_attempted'], False)
            self.assertEqual(row['timeout_basis'], dict(rule='five_consecutive_prediction_timeouts',
                                                       trigger_case_ids=trigger))
            self.assertEqual(row['case_timeout_seconds'], 300)
            for key, value in row.items():
                if key in ('elapsed_seconds', 'wall_seconds', 'solve_lower_bound_seconds', 'ts_db',
                           'delta_ts_db', 'amplitude_real_m', 'amplitude_imag_m') or key.startswith('memory_'):
                    self.assertIsNone(value)
        parallel.validate_packet(plan, task, packet)

    def test_success_breaks_streak_and_setup_timeout_stops_without_inference(self):
        def successes(index, case):
            return dict(status='ok', elapsed_seconds=180, ts_db=-20) if index % 5 == 0 else self.timeout(index, case)
        plan, task, packet, calls = self.execute(successes)
        self.assertEqual(len(calls), len(task['cases']))
        self.assertTrue(all(row['prediction_attempted'] for row in packet['records'].values()))
        def setup(index, case):
            return dict(status='setup_timeout') if index == 5 else self.timeout(index, case)
        plan, task, packet, calls = self.execute(setup)
        self.assertEqual(len(calls), 5)
        self.assertEqual(packet['state'], 'setup_failed')
        self.assertEqual(len(packet['records']), 4)
        self.assertTrue(all(row['prediction_attempted'] for row in packet['records'].values()))

    def test_trigger_requires_adjacent_actual_records_at_300_seconds_within_one_repetition(self):
        plan, runtime = fixtures.long_plan(repetitions=2)
        task, other = plan['tasks']
        valid = fixtures.actual_packet(plan, task, runtime, range(5))
        for row in valid['records'].values():
            row['case_timeout_seconds'] = 300
        expected = [case['case_id'] for case in task['cases'][:5]]
        self.assertEqual(parallel.timeout_streak(dict(task, cases=list(reversed(task['cases']))),
                                               valid['records'])['trigger_case_ids'], expected)
        for mutation in ('gap', 'setup', 'inferred', 'old_cap'):
            with self.subTest(mutation=mutation):
                records = copy.deepcopy(valid['records'])
                identity = expected[2]
                if mutation == 'gap':
                    del records[identity]
                elif mutation == 'setup':
                    records[identity]['status'] = 'setup_timeout'
                elif mutation == 'inferred':
                    records[identity]['prediction_attempted'] = False
                else:
                    records[identity]['case_timeout_seconds'] = 120
                self.assertIsNone(parallel.timeout_streak(task, records))
        across = fixtures.actual_packet(plan, task, runtime, range(4))['records']
        across.update(fixtures.actual_packet(plan, other, runtime, [4])['records'])
        self.assertIsNone(parallel.timeout_streak(task, across))
        for row in valid['records'].values():
            row['case_timeout_seconds'] = 600
        self.assertIsNotNone(parallel.timeout_streak(task, valid['records']))

    def test_resumed_sweep_fills_lower_gap_and_preserves_higher_actual_result(self):
        plan, runtime = fixtures.long_plan()
        task = plan['tasks'][0]
        current = fixtures.actual_packet(plan, task, runtime, [0, 1, 3, 4, 5, 6, 7, 12])
        current['slow_sweep'] = True
        for row in current['records'].values():
            row['case_timeout_seconds'] = 300
        for index in (0, 1):
            current['records'][task['cases'][index]['case_id']]['case_timeout_seconds'] = 120
        current['records'][task['cases'][12]['case_id']].update(status='ok', elapsed_seconds=5, ts_db=-20)
        original = copy.deepcopy(current['records'])
        plan, task, packet, calls = self.execute(lambda index, case: dict(status='ok', elapsed_seconds=2, ts_db=-20), current)
        self.assertEqual(calls, [task['cases'][2]['case_id']])
        self.assertEqual(packet['state'], 'complete')
        self.assertTrue(all(packet['records'][identity] == row for identity, row in original.items()))
        self.assertEqual(len(packet['records']), len(task['cases']))

    def test_historical_ten_timeout_proof_remains_valid_at_old_budget(self):
        plan, runtime = fixtures.long_plan()
        task = plan['tasks'][0]
        packet = fixtures.actual_packet(plan, task, runtime, range(10))
        for row in packet['records'].values():
            row['case_timeout_seconds'] = 120
        historical = dict(rule='ten_consecutive_prediction_timeouts',
                          trigger_case_ids=[case['case_id'] for case in task['cases'][:10]])
        self.assertEqual(parallel.timeout_basis_frequency(task, packet['records'], historical), 19000)
        self.assertIsNone(parallel.timeout_streak(task, packet['records']))
        new_basis = dict(rule='five_consecutive_prediction_timeouts',
                         trigger_case_ids=historical['trigger_case_ids'][:5])
        with self.assertRaises(ValueError):
            parallel.timeout_basis_frequency(task, packet['records'], new_basis)


# Runtime-pool plan storage regressions.

class PlanRuntimeStorageTests(unittest.TestCase):
    def sample(self):
        plan, task, runtime = pending_plan()
        plan['datasets']['0.07']['environments'] = [
            dict(runtime=copy.deepcopy(runtime), started_at='first', github_job='worker-1'),
            dict(runtime=copy.deepcopy(runtime), started_at='second', github_job='worker-2'),
            dict(started_at='missing'), dict(runtime=None, started_at='null'),
            dict(runtime=dict(runtime, compiler='distinct'), started_at='third')]
        plan['datasets']['0.1'] = dict(rows=[], environments=[])
        plan['datasets']['0.001'] = dict(rows=[], configuration={})
        plan['datasets']['0.07']['archived_rows'] = [dict(case_id='retained', status='error')]
        return plan

    def wire(self, payload, encoding=None):
        raw = json.dumps(payload, allow_nan=False, separators=(',', ':')).encode()
        return dict(encoding=encoding or parallel.PLAN_RUNTIME_ENCODING,
                    sha256=parallel.hashlib.sha256(raw).hexdigest(),
                    data=parallel.base64.b64encode(parallel.gzip.compress(raw, mtime=0)).decode())

    def payload(self, packed):
        return json.loads(parallel.gzip.decompress(parallel.base64.b64decode(packed['data'])))

    def test_exact_roundtrip_retains_rows_tasks_archives_and_environment_order(self):
        plan = self.sample()
        before = copy.deepcopy(plan)
        packed = parallel.pack(plan)
        self.assertEqual(packed['encoding'], parallel.PLAN_RUNTIME_ENCODING)
        self.assertEqual(plan, before)
        restored = parallel.unpack(packed)
        self.assertEqual(restored, before)
        payload = self.payload(packed)
        self.assertEqual(len(payload['runtimes']), 3)
        self.assertEqual(payload['environment_runtimes']['0.07'], [0, 0, -1, 1, 2])
        self.assertEqual(restored['tasks'], plan['tasks'])
        self.assertEqual(restored['datasets']['0.07']['rows'], plan['datasets']['0.07']['rows'])

    def test_reconstructed_runtime_objects_remain_independent(self):
        plan = self.sample()
        restored = parallel.unpack(parallel.pack(plan))
        environments = restored['datasets']['0.07']['environments']
        environments[0]['runtime']['source_shas']['acousticts'] = 'changed'
        self.assertEqual(environments[1]['runtime'], plan['datasets']['0.07']['environments'][1]['runtime'])
        self.assertNotEqual(environments[0]['runtime'], environments[1]['runtime'])

    def test_legacy_plan_and_nonplan_documents_remain_supported(self):
        plan = self.sample()
        self.assertEqual(parallel.unpack(self.wire(plan, 'gzip-base64')), plan)
        for value in [dict(campaign_id='100', status='active'), dict(records={}, environments={}),
                      dict(datasets={}, tasks=[], source_ref='c'*40, campaign_id='100')]:
            with self.subTest(value=value):
                packed = parallel.pack(value)
                self.assertEqual(packed['encoding'], 'gzip-base64')
                self.assertEqual(parallel.unpack(packed), value)

    def test_pooling_handles_plan_above_old_size_limit(self):
        plan = self.sample()
        plan['datasets']['0.07']['environments'] = [dict(runtime=dict(log='x'*4000), started_at=str(index))
                                                   for index in range(8)]
        original_size = len(json.dumps(plan, separators=(',', ':')).encode())
        compact_size = len(json.dumps(parallel._plan_runtime_payload(plan), separators=(',', ':')).encode())
        self.assertLess(compact_size, original_size)
        with patch.object(parallel, 'MAX_BYTES', compact_size), \
             patch.object(parallel, 'MAX_EXPANDED_PLAN_BYTES', original_size):
            packed = parallel.pack(plan)
            self.assertEqual(parallel.unpack(packed), plan)
            with self.assertRaises(ValueError):
                parallel.unpack(self.wire(plan, 'gzip-base64'))
        with patch.object(parallel, 'MAX_BYTES', compact_size-1):
            with self.assertRaisesRegex(ValueError, 'size limit'):
                parallel.pack(plan)

    def test_expanded_limit_uses_exact_json_length_before_copying(self):
        plan = self.sample()
        plan['datasets']['0.07']['environments'][0]['note'] = '?'
        plan['datasets']['0.07']['environments'].append(dict(runtime=dict(unicode='?')))
        exact = len(json.dumps(plan, separators=(',', ':')).encode())
        packed = parallel.pack(plan)
        with patch.object(parallel, 'MAX_EXPANDED_PLAN_BYTES', exact):
            self.assertEqual(parallel.unpack(packed), plan)
        with patch.object(parallel, 'MAX_EXPANDED_PLAN_BYTES', exact-1):
            with self.assertRaisesRegex(ValueError, 'Expanded spectrum plan'):
                parallel.pack(plan)
            with patch.object(parallel.copy, 'deepcopy', side_effect=AssertionError('copied too early')):
                with self.assertRaisesRegex(ValueError, 'Expanded spectrum plan'):
                    parallel.unpack(packed)

    def test_digest_tampering_is_rejected(self):
        packed = parallel.pack(self.sample())
        packed['sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'digest'):
            parallel.unpack(packed)

    def test_invalid_pool_indexes_and_metadata_are_rejected(self):
        original = self.payload(parallel.pack(self.sample()))
        mutations = [
            lambda value: value['environment_runtimes']['0.07'].__setitem__(0, 100),
            lambda value: value['environment_runtimes']['0.07'].__setitem__(0, -2),
            lambda value: value['environment_runtimes']['0.07'].__setitem__(0, True),
            lambda value: value['environment_runtimes']['0.07'].pop(),
            lambda value: value['environment_runtimes'].pop('0.07'),
            lambda value: value['environment_runtimes'].__setitem__('extra', []),
            lambda value: value['runtimes'].append(copy.deepcopy(value['runtimes'][0])),
            lambda value: value['runtimes'].append(dict(unused=True)),
            lambda value: value['plan']['datasets']['0.07']['environments'][0].__setitem__('runtime', {}),
            lambda value: value.__setitem__('schema_version', True),
            lambda value: value.__setitem__('extra', None),
            lambda value: value.__setitem__('runtimes', {}),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=index):
                changed = copy.deepcopy(original)
                mutation(changed)
                with self.assertRaises(ValueError):
                    parallel.unpack(self.wire(changed))

    def test_document_reader_supports_pooled_plan(self):
        plan = self.sample()
        raw = json.dumps(parallel.pack(plan), separators=(',', ':')).encode()
        self.assertEqual(parallel._unpack_document(Mock(), raw), plan)




class ArtifactOnlyControllerTests(unittest.TestCase):
    def test_only_exact_checkpoint_path_change_is_accepted(self):
        prefix = 'worker: unchanged\n        path: |-\n'
        suffix = '          ${{ runner.temp }}/benchmark-env/runtime.json\ntimeout: unchanged\n'
        old = '          ${{ runner.temp }}/spectrum\n'
        excluded = '          !${{ runner.temp }}/spectrum/**/fort.*\n'
        new = ''.join('          ${{ runner.temp }}/spectrum/**/' + name + '\n' for name in
                      ('checkpoint.json', 'manifest.json', 'results.json', 'environments/*.json'))
        files = [dict(filename='.github/workflows/parallel-spectra.yml', status='modified')]
        for exclusion in ('', excluded):
            before = prefix + old + exclusion + suffix
            after = prefix + new + suffix
            for candidate, accepted in ((after, True), (after.replace('timeout: unchanged', 'timeout: 999'), False),
                                        (after.replace('worker: unchanged', 'worker: new solver'), False),
                                        (after.replace('checkpoint.json', '*.json'), False), (before, False)):
                api = Mock()
                api.request.side_effect = [dict(encoding='base64', size=len(text),
                    content=base64.b64encode(text.encode()).decode()) for text in (before, candidate)]
                self.assertEqual(parallel.controller_files_allowed(api, files, 'a'*40, 'b'*40), accepted)

    def test_protected_files_and_unbounded_workflow_reads_are_rejected(self):
        for filename in ('sources.lock.json', 'benchmark.json', 'src/worker_python.py'):
            self.assertFalse(parallel.controller_files_allowed(Mock(), [dict(filename=filename)], 'a', 'b'))
        api = Mock()
        api.request.return_value = dict(encoding='base64', size=1024*1024, content='')
        self.assertFalse(parallel.controller_files_allowed(api,
            [dict(filename='.github/workflows/parallel-spectra.yml', status='modified')], 'a', 'b'))



class CheckpointBatchTests(unittest.TestCase):
    def test_chunked_batch_roundtrip_uses_one_atomic_commit(self):
        api, files = DocumentStorageTests().api()
        values = [(f'task-{i}.json', dict(values=list(range(40)), identity=i), None) for i in range(32)]
        with patch.object(parallel, 'DOCUMENT_PART_BYTES', 60):
            shas = parallel.put_documents(api, values)
        self.assertEqual(sum(c.args[:2] == ('POST', '/git/commits') for c in api.request.call_args_list), 1)
        for path, value, _ in values:
            self.assertEqual(parallel.get_document(api, path), (value, shas[path]))
        before = len(api.request.call_args_list)
        with patch.object(parallel, 'DOCUMENT_PART_BYTES', 60):
            parallel.put_documents(api, [(path, value, shas[path]) for path, value, _ in values])
        self.assertFalse(any(c.args[:2] == ('POST', '/git/commits') for c in api.request.call_args_list[before:]))

    def test_failed_upload_and_stale_checkpoint_leave_entire_batch_unpublished(self):
        for mode in ('upload', 'stale'):
            api, files = DocumentStorageTests().api()
            old = parallel.put_document(api, 'one.json', dict(value='old'))
            original = api.request.side_effect
            if mode == 'stale':
                parallel.put_document(api, 'one.json', dict(value='other writer'), old)
            baseline = dict(files)
            uploads = []
            def failure(method, path, body=None):
                if mode == 'upload' and method == 'POST' and path == '/git/blobs':
                    uploads.append(1)
                    if len(uploads) == 2:
                        raise parallel.APIError(503, 'interrupted')
                return original(method, path, body)
            api.request.side_effect = failure
            with self.assertRaises((parallel.APIError, parallel.DocumentConflict)):
                parallel.put_documents(api, [('one.json', dict(value='new'), old), ('two.json', dict(value=2), None)])
            self.assertEqual(files, baseline)

    def test_batch_rebases_unrelated_writer_without_losing_either_document(self):
        api, files = DocumentStorageTests().api()
        original = api.request.side_effect
        conflicts = []
        def competing(method, path, body=None):
            if method == 'PATCH' and not conflicts:
                conflicts.append(1)
                other = Mock(request=Mock(side_effect=original))
                parallel.put_document(other, 'other.json', dict(saved=True))
                raise parallel.APIError(409, 'competing writer')
            return original(method, path, body)
        api.request.side_effect = competing
        with patch.object(parallel.time, 'sleep'):
            parallel.put_documents(api, [('one.json', dict(value=1), None), ('two.json', dict(value=2), None)])
        self.assertEqual(set(files), {'one.json', 'two.json', 'other.json'})
        self.assertEqual(parallel.get_document(api, 'other.json')[0], dict(saved=True))

    def test_batch_rejects_duplicate_paths_and_unbounded_batches(self):
        for updates in ([('same.json', {}, None)]*2, [(f'{i}.json', {}, None) for i in range(33)]):
            with self.assertRaises(ValueError):
                parallel.put_documents(Mock(), updates)

class ProgressRelayTests(unittest.TestCase):
    def api(self):
        api = Mock(repository='owner/repository')
        run = dict(event='workflow_dispatch', head_repository=dict(full_name=api.repository),
                   path='.github/workflows/parallel-spectra.yml', display_title='Continue spectra 100')
        return api, run

    def test_takeover_reads_original_run_without_restarting_workers(self):
        api, run = self.api()
        api.request.side_effect = [run, dict(jobs=[dict(name='progress', status='completed', conclusion='cancelled')])]
        state = dict(waves=[dict(run_id=200)])
        with patch.object(parallel, 'get_document', return_value=(state, 'sha')), \
             patch.object(parallel, 'monitor', return_value=True) as monitor:
            parallel.relay_progress(api, 200, Path('results'), campaign_id='100')
        self.assertEqual(monitor.call_args.args, (api, '100', Path('results')))
        self.assertEqual(monitor.call_args.kwargs['run_id'], 200)
        self.assertTrue(all(call.args[0] == 'GET' for call in api.request.call_args_list))

    def test_expiring_takeover_renews_only_publisher(self):
        api, run = self.api()
        api.request.side_effect = [run, dict(jobs=[dict(name='progress', status='completed', conclusion='cancelled')]), {}]
        with patch.object(parallel, 'get_document', return_value=(dict(waves=[dict(run_id=200)]), 'sha')), \
             patch.object(parallel, 'monitor', return_value=False):
            parallel.relay_progress(api, 200, Path('results'), campaign_id='100')
        self.assertEqual(api.request.call_args.args, ('POST', '/actions/workflows/publish-progress.yml/dispatches',
                         dict(ref='main', inputs=dict(run_id='200', campaign_id='100'))))

    def test_waiting_relay_renews_before_job_timeout(self):
        api, run = self.api()
        api.request.side_effect = [run, dict(jobs=[dict(name='progress', status='in_progress', conclusion=None)]), {}]
        with patch.object(parallel, 'get_document', return_value=(None, None)), \
             patch.object(parallel.time, 'monotonic', side_effect=[0, 0, 61]), \
             patch.object(parallel.time, 'sleep') as sleep, patch.object(parallel, 'monitor') as monitor:
            parallel.relay_progress(api, 200, Path('results'), budget_seconds=60)
        monitor.assert_not_called()
        sleep.assert_called_once_with(60)
        self.assertEqual(api.request.call_args.args[1], '/actions/workflows/publish-progress.yml/dispatches')

    def test_successful_primary_needs_no_replacement(self):
        api, run = self.api()
        api.request.side_effect = [run, dict(jobs=[dict(name='progress', status='completed', conclusion='success')])]
        with patch.object(parallel, 'get_document', return_value=(dict(waves=[dict(run_id=200)]), 'sha')), \
             patch.object(parallel, 'monitor') as monitor:
            parallel.relay_progress(api, 200, Path('results'))
        monitor.assert_not_called()
        self.assertEqual(api.request.call_count, 2)

    def test_newer_allocation_stops_old_relay(self):
        api, run = self.api()
        api.request.side_effect = [run, dict(jobs=[])]
        with patch.object(parallel, 'get_document', return_value=(dict(waves=[dict(run_id=200),dict(run_id=300)]), 'sha')), \
             patch.object(parallel, 'monitor') as monitor:
            parallel.relay_progress(api, 200, Path('results'))
        monitor.assert_not_called()
        self.assertEqual(api.request.call_count, 2)

    def test_status_failure_waits_and_renews(self):
        api, run = self.api()
        api.request.side_effect = [run, {}]
        with patch.object(parallel, 'get_document', side_effect=parallel.APIError(503,'transient')), \
             patch.object(parallel.time, 'monotonic', side_effect=[0,0,61]), \
             patch.object(parallel.time, 'sleep') as sleep:
            parallel.relay_progress(api, 200, Path('results'), budget_seconds=60)
        sleep.assert_called_once_with(30)
        self.assertEqual(api.request.call_args.args[1], '/actions/workflows/publish-progress.yml/dispatches')

    def test_invalid_run_and_campaign_are_rejected_before_publication(self):
        for field, value in [('event','pull_request'), ('head_repository',dict(full_name='other/repository')),
                             ('path','.github/workflows/unrelated.yml')]:
            api, run = self.api()
            run[field] = value
            api.request.return_value = run
            with self.assertRaises(ValueError):
                parallel.relay_progress(api, 200, Path('results'))
            self.assertEqual(api.request.call_count, 1)
        api, run = self.api()
        api.request.return_value = run
        with self.assertRaises(ValueError):
            parallel.relay_progress(api, 200, Path('results'), campaign_id='999')

    def test_final_publication_failure_is_retried_even_after_workers_stop(self):
        plan, task, _ = pending_plan()
        state = dict(waves=[dict(run_id=200, tasks=[task['id']])])
        api = Mock()
        api.request.return_value = dict(jobs=[dict(name=task['name'], status='completed')])
        with patch.object(parallel, 'get_document', side_effect=[(plan,'p'),(state,'s'),(state,'s'),(state,'s')]), \
             patch.object(parallel, 'read_packets', return_value={}), \
             patch.object(parallel.time, 'sleep'), \
             patch.object(parallel, 'publish_datasets', side_effect=[parallel.PublicationConflict('busy'),None]) as publish:
            self.assertTrue(parallel.monitor(api, '100', Path('results'), run_id=200))
        self.assertEqual(publish.call_count, 2)
        self.assertEqual(publish.call_args.kwargs['run_id'], 200)
        self.assertTrue(all(call.args[1] == '/actions/runs/200/jobs?per_page=100' for call in api.request.call_args_list))

    def test_checkpoint_read_failure_is_retried(self):
        plan, task, _ = pending_plan()
        state = dict(waves=[dict(run_id=200, tasks=[task['id']])])
        api = Mock()
        api.request.return_value = dict(jobs=[dict(name=task['name'], status='completed')])
        with patch.object(parallel, 'get_document', side_effect=[(plan,'p'),(state,'s'),(state,'s'),(state,'s')]), \
             patch.object(parallel, 'read_packets', side_effect=[parallel.APIError(503,'transient'),{}]), \
             patch.object(parallel.time, 'sleep'), \
             patch.object(parallel, 'publish_datasets') as publish:
            self.assertTrue(parallel.monitor(api, '100', Path('results'), run_id=200))
        publish.assert_called_once()




class RollingWorkerTests(unittest.TestCase):
    def setUp(self):
        self.plan, self.task, self.runtime = pending_plan()
        self.plan.update(max_parallel=4, budget_minutes=330, automatic_continue=True)
        for index in range(1, 7):
            self.plan['tasks'].append(dict(copy.deepcopy(self.task), id=f'task-{index}', name=f'spectrum {index}'))
        self.queue, workers = parallel.scheduling_queue(self.plan, self.plan['tasks'], {}, 100, 2)
        self.wave = dict(run_id=100, queue=parallel.queue_filename('100', 100),
                         workers=workers, worker_names=['worker 01', 'worker 02'],
                         tasks=[task['id'] for task in self.plan['tasks']], pool=True, queue_run_id=100)
        self.state = dict(campaign_id='100', status='active', attempts={}, completed=[], blocked=[],
                          max_parallel=4, waves=[self.wave],
                          worker_pool=dict(run_id=100, queue=self.wave['queue']))
        self.documents = {parallel.filename('100','state'):self.state, self.wave['queue']:self.queue}
        self.versions = {path:0 for path in self.documents}
        self.api = Mock(repository='owner/repo')
        self.api.request.side_effect = self.request
        self.jobs = [dict(name=name,status='in_progress') for name in self.wave['worker_names']]

    def request(self, method, path, body=None):
        if method == 'POST':
            return {}
        if '/jobs?' in path:
            return dict(jobs=copy.deepcopy(self.jobs))
        if '/actions/runs/' in path:
            return dict(status='in_progress')
        raise AssertionError((method,path,body))

    def get(self, api, path):
        return copy.deepcopy(self.documents.get(path)), str(self.versions.get(path,0))

    def put(self, api, path, value, sha=None, **kwargs):
        self.put_many(api,[(path,value,sha)])
        return str(self.versions[path])

    def put_many(self, api, changes):
        for path, value, sha in changes:
            if sha is not None and sha != str(self.versions.get(path,0)):
                raise parallel.DocumentConflict('stale document')
        for path, value, sha in changes:
            self.documents[path] = copy.deepcopy(value)
            self.versions[path] = self.versions.get(path,0)+1

    def store(self):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(parallel,'get_document',side_effect=self.get))
        stack.enter_context(patch.object(parallel,'put_document',side_effect=self.put))
        stack.enter_context(patch.object(parallel,'put_documents',side_effect=self.put_many))
        return stack

    def test_replacements_share_queue_and_preserve_active_claims(self):
        tasks={task['id']:task for task in self.plan['tasks']}
        worker=self.wave['workers'][0]
        work,_=parallel.claim_queued_work(self.queue,self.plan,tasks,worker)
        original=copy.deepcopy(self.queue)
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='101',GITHUB_SHA='c'*40), \
             patch.object(parallel,'write_outputs') as output:
            parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
        saved=self.documents[self.wave['queue']]
        wave=self.documents[parallel.filename('100','state')]['waves'][-1]
        self.assertEqual(saved['claims'],original['claims'])
        self.assertEqual(saved['pending'],original['pending'])
        self.assertEqual(saved['owners'][worker],work['id'])
        self.assertEqual(wave['queue_run_id'],100)
        self.assertEqual(wave['run_id'],101)
        self.assertEqual(len(wave['workers']),2)
        self.assertFalse(set(wave['workers']) & set(self.wave['workers']))
        self.assertEqual(output.call_args.args[0]['max_parallel'],4)

    def test_registration_retry_reuses_saved_worker_identities(self):
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='101',GITHUB_SHA='c'*40), \
             patch.object(parallel,'write_outputs'):
            parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
            saved=copy.deepcopy(self.documents)
            parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
        self.assertEqual(self.documents,saved)

    def test_second_allocation_counts_already_registered_slots(self):
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='101',GITHUB_SHA='c'*40), \
             patch.object(parallel,'write_outputs'):
            parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
            with patch.dict(os.environ,GITHUB_RUN_ID='102'):
                parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
        waves=self.documents[parallel.filename('100','state')]['waves']
        self.assertEqual(sum(len(wave['workers']) for wave in waves),4)
        self.assertEqual(waves[-1]['workers'],[])

    def test_missing_matrix_jobs_reserve_capacity(self):
        self.jobs=[]
        active=parallel.pool_allocations(self.api,self.state)[0]['active']
        self.assertEqual(active,set(self.wave['workers']))

    def test_legacy_finalizer_is_not_modified_concurrently(self):
        self.wave.pop('pool')
        self.jobs=[dict(name=name,status='completed') for name in self.wave['worker_names']]
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='101',GITHUB_SHA='c'*40):
            with self.assertRaisesRegex(RuntimeError,'final publication'):
                parallel.plan_pool(self.api,{},self.plan,self.state,Path(directory))

    def test_uncertain_dispatch_does_not_create_duplicate_run(self):
        with self.store(), patch.object(parallel,'pool_dispatch_waiting',return_value=True):
            parallel.replenish_pool(self.api,self.plan)
            parallel.replenish_pool(self.api,self.plan)
        dispatches=[call for call in self.api.request.call_args_list if call.args[0]=='POST']
        self.assertEqual(len(dispatches),1)
        self.assertEqual(dispatches[0].args[2]['inputs']['rolling_workers'],'true')

    def test_failed_planning_dispatch_has_bounded_retries(self):
        with self.store(), patch.object(parallel,'pool_dispatch_waiting',return_value=False):
            for _ in range(6):
                parallel.replenish_pool(self.api,self.plan)
        dispatches=[call for call in self.api.request.call_args_list if call.args[0]=='POST']
        self.assertEqual(len(dispatches),3)
        self.assertIn('pool_dispatch_blocked',self.documents[self.wave['queue']])

    def test_dispatch_reconciliation_waits_for_queued_campaign(self):
        reservation=dict(requested_at='2026-11-12T10:00:00Z')
        self.api.request.side_effect=None
        self.api.request.return_value=dict(workflow_runs=[dict(id=101,display_title='Continue spectra 100',
            created_at='2026-11-12T10:00:01Z',status='queued')])
        with patch.object(parallel.time,'time',return_value=1794481200):
            self.assertTrue(parallel.pool_dispatch_waiting(self.api,self.plan,self.state,reservation))
        self.api.request.return_value['workflow_runs'][0]['status']='completed'
        with patch.object(parallel.time,'time',return_value=1794481200):
            self.assertFalse(parallel.pool_dispatch_waiting(self.api,self.plan,self.state,reservation))

    def test_recovery_precedes_retry_and_counts_attempt_once(self):
        worker=self.wave['workers'][0]
        tasks={task['id']:task for task in self.plan['tasks']}
        work,_=parallel.claim_queued_work(self.queue,self.plan,tasks,worker)
        other=self.wave['workers'][1]
        active,_=parallel.claim_queued_work(self.queue,self.plan,tasks,other)
        allocation=dict(wave=self.wave,stopped={worker},active={other})
        def recover(*args):
            self.assertEqual(self.documents[self.wave['queue']]['owners'][worker],work['id'])
        with self.store(), patch.object(parallel,'recover_artifacts',side_effect=recover) as recovered, \
             patch.object(parallel,'read_packets',return_value={}):
            parallel.recover_pool(self.api,self.plan,self.state,[allocation])
            parallel.recover_pool(self.api,self.plan,self.state,[allocation])
        self.assertEqual(recovered.call_count,1)
        queue=self.documents[self.wave['queue']]
        self.assertIsNone(queue['owners'][worker])
        self.assertEqual(queue['owners'][other],active['id'])
        retries=[unit for unit in queue['pending'] if queue['units'][unit]['task_id']==work['task_id']]
        self.assertEqual(len(retries),1)
        self.assertNotEqual(retries[0],work['id'])
        self.assertEqual(queue['claims'][work['id']],worker)
        self.assertEqual(self.documents[parallel.filename('100','state')]['attempts'][work['task_id']]['stalled'],1)

    def test_failed_artifact_recovery_keeps_claim_and_does_not_retry(self):
        worker=self.wave['workers'][0]
        tasks={task['id']:task for task in self.plan['tasks']}
        parallel.claim_queued_work(self.queue,self.plan,tasks,worker)
        original=copy.deepcopy(self.documents)
        with self.store(), patch.object(parallel,'recover_artifacts',side_effect=TimeoutError('archive')):
            with self.assertRaises(TimeoutError):
                parallel.recover_pool(self.api,self.plan,self.state,[dict(wave=self.wave,stopped={worker})])
        self.assertEqual(self.documents,original)

    def test_complete_packet_is_never_requeued(self):
        worker=self.wave['workers'][0]
        tasks={task['id']:task for task in self.plan['tasks']}
        work,_=parallel.claim_queued_work(self.queue,self.plan,tasks,worker)
        parallel.pool_requeue(self.plan,self.queue,{work['task_id']:dict(state='complete')},{worker},[])
        self.assertFalse(any(self.queue['units'][unit]['task_id']==work['task_id'] for unit in self.queue['pending']))

    def test_lease_collision_retries_instead_of_stopping_publisher(self):
        with self.store(), patch.object(parallel,'put_document',side_effect=parallel.DocumentConflict('busy')):
            with self.assertRaises(parallel.PublicationConflict):
                parallel.pool_publisher(self.api,self.state,100)

    def test_lease_takeover_after_expiry(self):
        self.queue['pool_publisher']=dict(run_id=100,expires_epoch=500)
        with self.store(), patch.object(parallel.time,'time',return_value=499):
            self.assertFalse(parallel.pool_publisher(self.api,self.state,101))
        with self.store(), patch.object(parallel.time,'time',return_value=501):
            self.assertTrue(parallel.pool_publisher(self.api,self.state,101))

    def test_worker_uses_shared_queue_and_actual_allocation_provenance(self):
        wave=dict(self.wave,run_id=101,queue_run_id=100)
        packet=packet_for(self.plan,self.task,self.runtime,len(self.task['cases']))
        packet['_publication_complete']=True
        work=dict(id=self.task['id'],task_id=self.task['id'],kind='sweep')
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'change_work_queue',side_effect=[work,None]) as claim, \
             patch.object(parallel,'run_task',return_value=packet) as solve:
            parallel.run_frequency_worker(self.api,self.plan,self.state,wave,wave['workers'][0],
                                          Path('runtime'),Path(directory),float('inf'))
        self.assertTrue(all(call.args[2]==100 for call in claim.call_args_list))
        self.assertEqual(solve.call_args.kwargs['work']['run_id'],101)

    def test_worker_exits_without_waiting_on_slow_claimed_spectrum(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'change_work_queue',return_value=dict(kind='wait')), \
             patch.object(parallel,'run_task') as solve, patch.object(parallel.time,'sleep') as sleep:
            parallel.run_frequency_worker(self.api,self.plan,self.state,self.wave,self.wave['workers'][0],
                                          Path('runtime'),Path(directory),float('inf'))
        solve.assert_not_called()
        sleep.assert_not_called()


    def test_finalization_keeps_other_allocations_active(self):
        self.jobs[0]['status']='completed'
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'read_packets',return_value={}), \
             patch.object(parallel,'publish_datasets'), patch.object(parallel,'write_outputs'), \
             patch.object(parallel,'replenish_pool') as refill, \
             patch.object(parallel,'wake_ordered_queue') as handoff:
            parallel.finish_pool(self.api,self.plan,self.wave,Path(directory))
        self.assertEqual(self.documents[parallel.filename('100','state')]['status'],'active')
        refill.assert_called_once()
        handoff.assert_not_called()

    def test_final_handoff_occurs_once_after_all_saved_work_is_complete(self):
        self.jobs=[dict(name=name,status='completed') for name in self.wave['worker_names']]
        self.queue['pending']=[]
        packets={task['id']:dict(state='complete',records={}) for task in self.plan['tasks']}
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'recover_artifacts'), \
             patch.object(parallel,'read_packets',return_value=packets), \
             patch.object(parallel,'publish_datasets'), patch.object(parallel,'write_outputs'), \
             patch.object(parallel,'wake_ordered_queue') as handoff:
            parallel.finish_pool(self.api,self.plan,self.wave,Path(directory))
            parallel.finish_pool(self.api,self.plan,self.wave,Path(directory))
        self.assertEqual(self.documents[parallel.filename('100','state')]['status'],'complete')
        handoff.assert_called_once()

    def test_recovery_never_releases_active_worker(self):
        tasks={task['id']:task for task in self.plan['tasks']}
        worker=self.wave['workers'][0]
        work,_=parallel.claim_queued_work(self.queue,self.plan,tasks,worker)
        with self.store(), patch.object(parallel,'recover_artifacts') as recovery:
            parallel.recover_pool(self.api,self.plan,self.state,[dict(wave=self.wave,stopped=set())])
        self.assertEqual(self.documents[self.wave['queue']]['owners'][worker],work['id'])
        recovery.assert_not_called()

    def test_registration_rechecks_capacity_after_conflicting_writer(self):
        first=True
        def conflicting_put(api,changes):
            nonlocal first
            if first:
                first=False
                other=dict(self.wave,run_id=102,workers=['other-1','other-2'],worker_names=['other 1','other 2'])
                self.documents[parallel.filename('100','state')]['waves'].append(other)
                self.versions[parallel.filename('100','state')]+=1
                raise parallel.DocumentConflict('another planner registered workers')
            return self.put_many(api,changes)
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ,GITHUB_RUN_ID='101',GITHUB_SHA='c'*40), \
             patch.object(parallel,'put_documents',side_effect=conflicting_put), \
             patch.object(parallel,'write_outputs'):
            parallel.plan_pool(self.api,{},self.plan,copy.deepcopy(self.state),Path(directory))
        self.assertEqual(self.documents[parallel.filename('100','state')]['waves'][-1]['workers'],[])


    def test_pool_monitor_retries_final_publication_before_handoff(self):
        self.documents[parallel.filename('100','plan')]=self.plan
        self.jobs=[dict(name=name,status='completed') for name in self.wave['worker_names']]
        self.queue['pending']=[]
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'pool_publisher',return_value=True), \
             patch.object(parallel,'replenish_pool'), patch.object(parallel,'read_packets',return_value={}), \
             patch.object(parallel.time,'sleep'), \
             patch.object(parallel,'finish_pool',side_effect=[parallel.PublicationConflict('busy'),None]) as finish:
            self.assertTrue(parallel.monitor(self.api,'100',Path(directory),run_id=100))
        self.assertEqual(finish.call_count,2)

    def test_manual_continuation_stops_monitor_after_workers_finish(self):
        self.plan['automatic_continue']=False
        self.documents[parallel.filename('100','plan')]=self.plan
        self.jobs=[dict(name=name,status='completed') for name in self.wave['worker_names']]
        with self.store(), tempfile.TemporaryDirectory() as directory, \
             patch.object(parallel,'pool_publisher',return_value=True), \
             patch.object(parallel,'replenish_pool'), patch.object(parallel,'read_packets',return_value={}), \
             patch.object(parallel,'finish_pool') as finish:
            self.assertTrue(parallel.monitor(self.api,'100',Path(directory),run_id=100))
        finish.assert_called_once()

    def test_workflow_transformation_preserves_calculation_setup(self):
        current=(ROOT/'.github/workflows/parallel-spectra.yml').read_text(encoding='utf-8')
        self.assertEqual(parallel.rolling_workflow(current),current)
        self.assertRegex(current, r'(?m)^      rolling_workers:\n        description: [^\n]+\n        type: boolean\n        default: true\n')
        self.assertIn("inputs.rolling_workers && format('spheroid-pool-{0}-{1}', inputs.campaign_id || github.run_id, github.run_id)", current)


if __name__ == "__main__":
    unittest.main()


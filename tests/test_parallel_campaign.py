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
             patch.object(parallel,'get_document',side_effect=[(plan,'p'),(state,'s')]), \
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


class DashboardTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'),'Node.js exercises the actual dashboard aggregation')
    def test_means_ranges_complete_passes_and_failed_samples(self):
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
assert.equal(complete.total,8);assert.equal(complete.average,4);assert.deepEqual(complete.passes,[2,4,18]);
rows[5].status='timeout';
const partial=timingSummary(rows,2);
assert.equal(partial.total,5.5);assert.deepEqual(partial.passes,[2,4]);
assert.deepEqual(timingSummary(rows,3).passes,[]);
assert.equal(timingSummary([...rows,{...rows[0],source_sha:'b',elapsed_seconds:10}],2).points.length,3);
const comparisons=[];
for(const [index,errors] of [[.03,.04],[.06,.08]].entries())for(const [physical,delta_ts_db] of errors.entries())
 comparisons.push({physical_id:String(physical),status:'ok',reference_id:'reference',repetition:index+1,delta_ts_db});
const error=repetitionRMSE(comparisons);
assert.equal(error.count,2);assert.equal(error.values.length,2);
assert.ok(Math.abs(error.value-(Math.sqrt(.00125)+Math.sqrt(.005))/2)<1e-14);
"""
        subprocess.run([shutil.which('node'),'-e',functions+harness],check=True,capture_output=True,text=True)


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

    def test_deferral_threshold_is_inclusive_and_distinct_from_timeout(self):
        self.assertEqual(parallel.execution_policy(), dict(case_timeout_seconds=120, slow_frequency_seconds=60.0,
                                                         consecutive_timeouts=10))
        self.assertFalse(parallel.slow_frequency(dict(status='ok', elapsed_seconds=59.99)))
        self.assertTrue(parallel.slow_frequency(dict(status='ok', elapsed_seconds=60.0)))
        self.assertTrue(parallel.slow_frequency(dict(status='timeout', elapsed_seconds=None)))
        self.assertFalse(parallel.slow_frequency(dict(status='setup_timeout', elapsed_seconds=None)))

    def test_fresh_sweep_precedes_every_deferred_frequency(self):
        plan, tasks, runtime = self.two_sweeps()
        saved = packet_for(plan, tasks[0], runtime)
        next(iter(saved['records'].values()))['elapsed_seconds'] = 120
        original = copy.deepcopy(plan)
        queue, workers = parallel.scheduling_queue(plan, tasks, {tasks[0]['id']: saved}, 100, 16)
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

    def test_one_expensive_sweep_can_use_multiple_worker_slots(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime)
        saved['slow_sweep'] = True
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: saved}, 100, 16)
        self.assertEqual(len(workers), 2)
        self.assertEqual(len(set(workers)), 2)
        self.assertEqual(queue['pending'], [])
        self.assertEqual(set(queue['deferred']), {case['case_id'] for case in task['cases'][1:]})

    def test_unstarted_sweep_precedes_resumption_of_a_partial_cheap_sweep(self):
        plan, tasks, runtime = self.two_sweeps()
        partial = packet_for(plan, tasks[0], runtime)
        queue, workers = parallel.scheduling_queue(plan, tasks, {tasks[0]['id']: partial}, 100, 2)
        self.assertEqual(queue['pending'], [tasks[1]['id'], tasks[0]['id']])

    def test_worker_finishes_frequency_then_starts_brand_new_sweep(self):
        plan, tasks, runtime = self.two_sweeps()
        queue, workers = parallel.scheduling_queue(plan, tasks, {}, 100, 1)
        path = parallel.queue_filename('100', 100)
        documents, get, put = self.documents({path: queue})
        wave = dict(run_id=100, queue=path, workers=workers, tasks=[task['id'] for task in tasks])
        expected = [tasks[0]['cases'][0]] + tasks[1]['cases'] + tasks[0]['cases'][1:]
        environment = packet_for(plan, tasks[0], runtime)['environments']['inv']
        calls = []
        def measure(case, prepared, directory, timeout):
            calls.append((case['case_id'], timeout))
            return dict(status='ok', elapsed_seconds=60.0 if len(calls) == 1 else .25, ts_db=-20.)
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
        self.assertEqual(calls, [(case['case_id'], 120) for case in expected])
        self.assertEqual(len({identity for identity, timeout in calls}), 6)
        for task in tasks:
            packet = documents[parallel.worker_packet_filename('100', 100, workers[0], task['id'])]
            parallel.validate_packet(plan, task, packet)
            self.assertEqual(packet['state'], 'complete')
            self.assertEqual(len(packet['records']), 3)
            self.assertTrue(all(row['case_timeout_seconds'] == 120 for row in packet['records'].values()))
            self.assertTrue(all(env['execution_policy'] == parallel.execution_policy()
                                for env in packet['environments'].values()))
        self.assertEqual(plan['datasets']['0.07']['configuration']['case_timeout_seconds'], 600)
        self.assertTrue(documents[parallel.worker_packet_filename('100', 100, workers[0], tasks[0]['id'])]['slow_sweep'])

    def test_idle_worker_waits_for_active_sweep_and_then_takes_new_tail(self):
        plan, tasks, runtime = self.two_sweeps()
        queue, workers = parallel.scheduling_queue(plan, tasks, {}, 100, 2)
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

    def test_restart_retains_timeout_and_splits_only_unmeasured_frequencies(self):
        plan, task, runtime = pending_plan()
        packet = packet_for(plan, task, runtime)
        row = next(iter(packet['records'].values()))
        row.update(status='timeout', elapsed_seconds=None, wall_seconds=600, solve_lower_bound_seconds=599)
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: packet}, 101, 16)
        self.assertEqual(queue['pending'], [])
        self.assertNotIn(row['case_id'], queue['units'])
        self.assertEqual(len(queue['deferred']), 2)
        self.assertEqual(plan['datasets']['0.07']['configuration']['case_timeout_seconds'], 600)

    def test_numerical_timeout_yields_sweep_after_saving_actual_timeout(self):
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
                          wall_seconds=120, solve_lower_bound_seconds=119)) as measure:
            root = Path(temporary)
            core.atomic_json(root / 'runtime.json', runtime)
            result = parallel.run_task(Mock(), '100', task['id'], root / 'runtime.json', root / 'worker',
                                       float('inf'), context=(plan, {}),
                                       work=dict(kind='sweep', run_id=100, worker_id=task['id']))
        self.assertEqual(measure.call_count, 1)
        self.assertEqual(measure.call_args.args[-1], 120)
        self.assertEqual(result['state'], 'split_pending')
        saved = documents[parallel.worker_packet_filename('100', 100, task['id'], task['id'])]
        self.assertTrue(saved['slow_sweep'])
        self.assertEqual(len(saved['records']), 1)
        self.assertEqual(next(iter(saved['records'].values()))['status'], 'timeout')

    def test_failed_publication_never_releases_a_slow_sweep(self):
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
        self.assertEqual(measure.call_count, 1)
        self.assertEqual(len(local['records']), 1)
        self.assertEqual(documents[path]['owners'][workers[0]], tasks[0]['id'])
        self.assertEqual(documents[path]['pending'], [tasks[1]['id']])

    def test_artifacts_merge_disjoint_frequency_owners_without_losing_shared_rows(self):
        plan, task, runtime = pending_plan()
        saved = packet_for(plan, task, runtime, 3)
        saved['records'] = {task['cases'][2]['case_id']: saved['records'][task['cases'][2]['case_id']]}
        saved.update(state='fragment_complete', slow_sweep=True)
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: saved}, 100, 2)
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
        queue, workers = parallel.scheduling_queue(plan, [task], {task['id']: saved}, 100, 2)
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
        queue, workers = parallel.scheduling_queue(plan, [task], {}, 100, 1)
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


if __name__ == '__main__': unittest.main()





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
                       wall_seconds=120, solve_lower_bound_seconds=119)
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


class TimeoutTailRegressionTests(unittest.TestCase):
    def test_streak_follows_planned_frequency_order_and_accepts_historical_actual_timeouts(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        packet = actual_packet(plan, task, runtime, range(10))
        ordered = [case["case_id"] for case in task["cases"][:10]]
        del packet["records"][ordered[0]]["prediction_attempted"]
        packet["records"] = dict(reversed(list(packet["records"].items())))
        shuffled = dict(task, cases=list(reversed(task["cases"])))
        self.assertEqual(parallel.timeout_streak(shuffled, packet["records"]),
                         dict(after_frequency_hz=19000, rule="ten_consecutive_prediction_timeouts",
                              trigger_case_ids=ordered))

    def test_gaps_setup_inferred_and_other_results_reset_the_actual_timeout_streak(self):
        plan, runtime = long_plan()
        task = plan["tasks"][0]
        original = actual_packet(plan, task, runtime, range(19))
        reset = task["cases"][9]["case_id"]
        for outcome in ("missing", "setup_timeout", "inferred", "ok", "numerical_error", "pending"):
            with self.subTest(outcome=outcome):
                packet = copy.deepcopy(original)
                if outcome == "missing":
                    del packet["records"][reset]
                elif outcome == "inferred":
                    packet["records"][reset]["prediction_attempted"] = False
                else:
                    packet["records"][reset]["status"] = outcome
                self.assertIsNone(parallel.timeout_streak(task, packet["records"]))
        later = actual_packet(plan, task, runtime, range(20))
        later["records"][reset]["status"] = "setup_timeout"
        self.assertEqual(parallel.timeout_streak(task, later["records"])["trigger_case_ids"],
                         [case["case_id"] for case in task["cases"][10:20]])

    def test_timeout_streaks_never_combine_repetitions(self):
        plan, runtime = long_plan(repetitions=2)
        first, second = plan["tasks"]
        self.assertEqual(len({case["repetition"] for case in first["cases"]}), 1)
        self.assertNotEqual(first["cases"][0]["repetition"], second["cases"][0]["repetition"])
        left = actual_packet(plan, first, runtime, range(9))
        right = actual_packet(plan, second, runtime, [0])
        records = dict(left["records"], **right["records"])
        self.assertIsNone(parallel.timeout_streak(first, records))
        self.assertIsNone(parallel.timeout_streak(second, records))
        records.update(actual_packet(plan, first, runtime, [9])["records"])
        self.assertEqual(parallel.timeout_streak(first, records)["trigger_case_ids"],
                         [case["case_id"] for case in first["cases"][:10]])
        self.assertIsNone(parallel.timeout_streak(second, records))

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
        queue, workers = parallel.scheduling_queue(plan, [task], {task["id"]: early}, 100, 3)
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
        queue, workers = parallel.scheduling_queue(plan, [task], {task["id"]: packet}, 100, 2)
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
                work = dict(run_id=100 + index, worker_id=str(index) * 24)
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


if __name__ == "__main__":
    unittest.main()



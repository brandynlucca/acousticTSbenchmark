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
            parallel.plan_campaign(api,dict(campaign_id='100'),directory)
            self.assertFalse({item['id'] for item in first}&{item['id'] for item in outputs[-1]['matrix']})

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


if __name__ == '__main__': unittest.main()

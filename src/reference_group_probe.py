"""Bounded Fourier-group distribution audit, isolated from reference publication."""
import argparse
import base64
import datetime
import hashlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import zipfile

WORK = Path(__file__).resolve().parent
PIN = '3f3129935e20a28b466c306b58d4354e301eba39'
BRANCH = 'reference-group-parallel-probe-20261105'
REPO = 'brandynlucca/acousticTSbenchmark'

JULIA = r'''
using AcousticScattering, LinearAlgebra, JSON3, SHA
const AS = AcousticScattering
BLAS.set_num_threads(1)
Threads.nthreads()==4 || error("Expected four Julia threads")
const group_role, output_path = ARGS
const body = AS.Spheroid(.07,.07/1.01)
const boundary = AS.FluidFilled(1.24/1026.8,345/1477.3)
const k = 2pi*299000/1477.3
const specs = [
    (;name="early_low",nr=53,nt=160,nb=72,modes=collect(0:15),span=0:15),
    (;name="early_high",nr=53,nt=160,nb=72,modes=collect(240:251),span=240:251),
    (;name="late_24",nr=227,nt=680,nb=308,modes=[24],span=24:27),
    (;name="late_35",nr=227,nt=680,nb=308,modes=[35],span=32:35)]

function calculate(spec)
    reports=AS._SolveReports()
    fields=AS._spheroid_fembem_level(body,boundary,k,spec.nr,spec.nt,spec.nb,6,5;
        half=true,incidence_angle=pi/2,m_max=259,mode_threads=4,rtol=1e-6,
        chief_points=8,reports,quadrature_extra=0,modes=spec.modes,
        operator_spans=[spec.span],interior_field=:scattered)
    result=map(spec.modes) do m
        f=fields[m+1]
        amplitude=AS._spheroid_curved_amplitude(f,k,m,pi/2,pi)
        (;mode=m,pressure=[[real(z),imag(z)] for z in f.pressure],
          derivative=[[real(z),imag(z)] for z in f.derivative],
          amplitude=[real(amplitude),imag(amplitude)])
    end
    (;fields=result,systems=length(reports.systems))
end

function save_output(rows, warmup, calibration)
    source=pathof(AS)
    root=dirname(source)
    hashes=Dict(p=>bytes2hex(sha256(read(joinpath(root,p)))) for p in
        ("solvers/coupled/coupled.jl","solvers/bem/axisymmetric.jl","solvers/bem/ring_kernel.jl"))
    open(output_path*".tmp","w") do io
        JSON3.write(io,(;role=group_role,julia_version=string(VERSION),julia_threads=Threads.nthreads(),
            blas_threads=BLAS.get_num_threads(),source_hashes=hashes,warmup_seconds=warmup,
            calibration_seconds=calibration,rows))
    end
    mv(output_path*".tmp",output_path;force=true)
end

warm_start=time()
for spec in specs
    calculate(merge(spec,(;nr=4,nt=8,nb=8)))
end
warmup=time()-warm_start
println("GROUP_WARMUP role=",group_role," elapsed_seconds=",round(warmup;digits=2))
flush(stdout)
GC.gc()
calibration=Float64[]
for repeat in 1:2
    timed=@timed calculate(first(specs))
    push!(calibration,timed.time)
end
selected=group_role=="baseline" ? collect(eachindex(specs)) :
         group_role=="worker1" ? [1,3] : group_role=="worker2" ? [2,4] : error("Invalid role")
rows=Any[]
save_output(rows,warmup,calibration)
for repeat in 1:2
    for index in (repeat==1 ? selected : reverse(selected))
        spec=specs[index]
        println("GROUP_START role=",group_role," name=",spec.name," repeat=",repeat,
            " modes=",join(spec.modes,",")," span=",spec.span)
        flush(stdout)
        started=time()
        timed=@timed calculate(spec)
        push!(rows,(;name=spec.name,repeat,started_epoch=started,finished_epoch=time(),
            wall_seconds=timed.time,allocated_bytes=timed.bytes,gc_seconds=timed.gctime,
            compile_seconds=timed.compile_time,output=timed.value))
        save_output(rows,warmup,calibration)
        println("GROUP_DONE role=",group_role," name=",spec.name," repeat=",repeat,
            " elapsed_seconds=",round(timed.time;digits=2),
            " allocated_MiB=",round(timed.bytes/2.0^20;digits=2))
        flush(stdout)
    end
end
'''


def emit_workflow():
    import yaml
    checkout = {'uses': 'actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1'}
    upload = 'actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9'
    wf = {'name': 'Reference Fourier-group pilot', 'run-name': 'Reference group distribution: bounded accuracy and elapsed-time pilot',
          'on': {'workflow_dispatch': {}}, 'permissions': {'contents': 'read'},
          'env': {'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1',
                  'JULIA_NUM_THREADS': '4', 'PYTHONDONTWRITEBYTECODE': '1'}, 'jobs': {}}
    wf['jobs']['group'] = {'name': '${{ matrix.role }}: 299 kHz coupled FEM-BEM',
        'runs-on': 'ubuntu-24.04', 'timeout-minutes': 28,
        'strategy': {'fail-fast': False, 'max-parallel': 3, 'matrix': {'role': ['baseline','worker1','worker2']}},
        'steps': [checkout,
            {'uses':'actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1','with':{'python-version':'3.12'}},
            {'name':'Record runner start and pin active queue source',
             'run': 'python src/reference_group_probe.py pin'},
            {'uses':'julia-actions/setup-julia@da908477b5db94c10ffce8cfea9f2b3b9d5db687','with':{'version':'1.12.1'}},
            {'uses':'actions/cache/restore@caa296126883cff596d87d8935842f9db880ef25', 'with':{
                'path':'~/.julia/artifacts\n~/.julia/packages\n~/.julia/registries',
                'key':"reference-deps-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('sources.lock.json', 'src/setup_julia.jl') }}",
                'restore-keys':'reference-deps-Linux-X64-'}},
            {'name':'Prepare pinned numerical environment','timeout-minutes':10,
             'run':'python -m pip install psutil==7.2.2\npython src/prepare.py --directory "$RUNNER_TEMP/reference-env" --software acousticscattering'},
            {'name':'Measure unchanged Fourier refinement requests','timeout-minutes':15,
             'run':'python src/reference_group_probe.py compute --role "${{ matrix.role }}"'},
            {'name':'Retain numerical traces and timing evidence','if':'always()','uses':upload,
             'with':{'name':'reference-groups-${{ matrix.role }}','path':'${{ runner.temp }}/reference-group-*.json\n${{ runner.temp }}/reference-group-*.log',
                     'if-no-files-found':'error','retention-days':14}}]}
    wf['jobs']['compare'] = {'name':'Combined accuracy, elapsed time and memory report',
        'runs-on':'ubuntu-24.04','needs':'group','if':'always()','timeout-minutes':5,
        'steps':[checkout,
            {'uses':'actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093','with':{
                'pattern':'reference-groups-*','path':'${{ runner.temp }}/reference-group-results','merge-multiple':True}},
            {'name':'Compare every trace and account for runner overhead',
             'run':'python src/reference_group_probe.py compare --directory "$RUNNER_TEMP/reference-group-results"'},
            {'if':'always()','uses':upload,'with':{'name':'reference-group-comparison',
                'path':'${{ runner.temp }}/reference-group-results/reference-group-comparison.json\n${{ runner.temp }}/reference-group-results/reference-group-comparison.md',
                'if-no-files-found':'warn','retention-days':14}}]}
    target=WORK/'reference_group_parallel.yml'
    target.write_text(yaml.safe_dump(wf,sort_keys=False),encoding='utf-8')
    (WORK/'reference_group_parallel.jl').write_text(JULIA,encoding='utf-8')
    return target


def pin():
    temporary=Path(os.environ['RUNNER_TEMP'])
    lock=Path('sources.lock.json')
    value=json.loads(lock.read_text())
    value['sources']['acousticscattering']['sha']=PIN
    lock.write_text(json.dumps(value,indent=2)+'\n')
    (temporary/'reference-group-start.json').write_text(json.dumps({'epoch':time.time(),'pin':PIN}))


def compute(role):
    import psutil
    temporary=Path(os.environ['RUNNER_TEMP'])
    runtime=json.loads((temporary/'reference-env/runtime.json').read_text())
    assert runtime['source_shas']['acousticscattering']==PIN
    problem=runtime.get('availability',{}).get('acousticscattering')
    if problem and problem.get('status')!='ready':
        raise RuntimeError(f'Numerical environment unavailable: {problem}')
    source=Path(runtime['source_paths']['acousticscattering'])
    command=[x for x in runtime['executables']['acousticscattering'] if not x.startswith('--threads=')]
    command += ['--threads=4','--heap-size-hint=3072M']
    script=temporary/'reference-group-probe.jl'
    script.write_text(JULIA)
    output=temporary/f'reference-group-{role}.json'
    command += [str(script),role,str(output)]
    env=os.environ.copy()
    env.update(runtime.get('env',{}))
    before=time.time()
    peak=cpu=0.0
    with (temporary/f'reference-group-{role}.log').open('w') as log:
        process=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
        observer=psutil.Process(process.pid)
        heartbeat=0.0
        while process.poll() is None:
            now=time.time()
            if now-before>840:
                process.terminate()
                try: process.wait(10)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
                raise RuntimeError('Bounded pilot reached 14-minute compute limit')
            try:
                peak=max(peak,observer.memory_info().rss)
                times=observer.cpu_times()
                cpu=times.user+times.system
            except psutil.NoSuchProcess:
                pass
            if now-heartbeat>=30:
                print(f'GROUP_HEARTBEAT role={role} elapsed_seconds={now-before:.1f} cpu_seconds={cpu:.1f} peak_MiB={peak/2**20:.1f}',flush=True)
                if output.exists():
                    saved=json.loads(output.read_text())
                    print('GROUP_PROGRESS role='+role+' completed_blocks='+str(len(saved['rows'])),flush=True)
                heartbeat=now
            time.sleep(1)
        process_seconds=time.time()-before
    print((temporary/f'reference-group-{role}.log').read_text(),flush=True)
    if process.returncode:
        raise RuntimeError(f'Numerical pilot failed with exit {process.returncode}')
    value=json.loads(output.read_text())
    value.update(pin=PIN,process_seconds=process_seconds,cpu_seconds=cpu,peak_rss_MiB=peak/2**20,
        setup_seconds=before-json.loads((temporary/'reference-group-start.json').read_text())['epoch'],
        measured_finished_epoch=time.time(),source_path=str(source),
        cpu_model=next((line.partition(':')[2].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),'unknown'))
    output.write_text(json.dumps(value,separators=(',',':'))+'\n')


def relative_vectors(left,right):
    if len(left)!=len(right): raise AssertionError('Trace dimensions differ')
    a=[complex(*z) for z in left]
    b=[complex(*z) for z in right]
    if not all(math.isfinite(z.real) and math.isfinite(z.imag) for z in a+b):
        raise AssertionError('Nonfinite numerical trace')
    scale=max(max(map(abs,a),default=0),max(map(abs,b),default=0),1e-300)
    return max((abs(x-y) for x,y in zip(a,b)),default=0)/scale


def compare(directory):
    directory=Path(directory)
    data={role:json.loads((directory/f'reference-group-{role}.json').read_text()) for role in ['baseline','worker1','worker2']}
    baseline=data['baseline']
    assert all(d['pin']==PIN and d['source_hashes']==baseline['source_hashes'] for d in data.values())
    maps={role:{(r['name'],r['repeat']):r for r in d['rows']} for role,d in data.items()}
    expected={'early_low','early_high','late_24','late_35'}
    assert set(maps['baseline'])=={(name,rep) for name in expected for rep in (1,2)}
    merged={}
    for role in ['worker1','worker2']:
        for key,row in maps[role].items():
            assert key not in merged
            merged[key]=row
    assert set(merged)==set(maps['baseline'])
    errors={'pressure':0.,'derivative':0.,'amplitude':0.}
    for key,row in merged.items():
        original=maps['baseline'][key]
        assert row['output']['systems']==original['output']['systems']
        assert len(row['output']['fields'])==len(original['output']['fields'])
        for a,b in zip(original['output']['fields'],row['output']['fields']):
            assert a['mode']==b['mode']
            for field in errors:
                left=[a[field]] if field=='amplitude' else a[field]
                right=[b[field]] if field=='amplitude' else b[field]
                errors[field]=max(errors[field],relative_vectors(left,right))
    rows=[]
    for rep in (1,2):
        serial=sum(r['wall_seconds'] for r in baseline['rows'] if r['repeat']==rep)
        workers={role:sum(r['wall_seconds'] for r in d['rows'] if r['repeat']==rep)
                 for role,d in data.items() if role!='baseline'}
        rows.append({'repeat':rep,'serial_compute_seconds':serial,'parallel_compute_seconds':max(workers.values()),
            'worker_seconds':workers,'compute_speedup':serial/max(workers.values())})
    # Complete job envelope includes environment preparation and all warmups/calibration.
    envelope={role:d['setup_seconds']+d['process_seconds'] for role,d in data.items()}
    # Actual worker start offsets and artifact download/reduction wait remain visible separately.
    first_start=min(min(r['started_epoch'] for r in data[role]['rows']) for role in ['worker1','worker2'])
    last_finish=max(data[role]['measured_finished_epoch'] for role in ['worker1','worker2'])
    result={'kind':'fixed-refinement-request feasibility probe, not a qualified complete frequency',
        'source_sha':PIN,'relative_complex_errors':errors,'trials':rows,'job_envelope_seconds':envelope,
        'parallel_observed_span_seconds':last_finish-first_start,
        'artifact_and_reducer_delay_seconds':time.time()-last_finish,
        'runners':{role:{key:d[key] for key in ['cpu_model','peak_rss_MiB','setup_seconds','process_seconds','warmup_seconds','calibration_seconds']} for role,d in data.items()},
        'accuracy_passed':max(errors.values())<=1e-11,
        'limitation':'Actual global adaptive barriers, incremental transfer and reuse require a complete frequency comparison before rollout.'}
    (directory/'reference-group-comparison.json').write_text(json.dumps(result,indent=2)+'\n')
    lines=['Reference Fourier-group pilot','',f'Numerical source: `{PIN}`','',
        'Fixed refinement requests from gas, a=0.07 m, aspect ratio 1.01, 299 kHz, 90 degrees.',
        'Full-frequency spatial/Fourier qualification remains outside the pilot. No references are published.','',
        '| Repeat | Single job compute (s) | Two worker compute (s) | Speedup |',
        '| --- | ---: | ---: | ---: |']
    for r in rows:
        lines.append(f"| {r['repeat']} | {r['serial_compute_seconds']:.2f} | {r['parallel_compute_seconds']:.2f} | {r['compute_speedup']:.2f} |")
    lines += ['',f'Maximum dimensionless relative complex trace difference: {max(errors.values()):.3e}.','',
        '| Role | CPU | Peak RAM (MiB) | Setup (s) | Process (s) |',
        '| --- | --- | ---: | ---: | ---: |']
    for role,d in data.items():
        lines.append(f"| {role} | {d['cpu_model']} | {d['peak_rss_MiB']:.2f} | {d['setup_seconds']:.2f} | {d['process_seconds']:.2f} |")
    lines += ['',result['limitation'],'']
    report='\n'.join(lines)
    (directory/'reference-group-comparison.md').write_text(report)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as output: output.write(report)
    print(report)
    if not result['accuracy_passed']: raise RuntimeError('Distributed trace comparison failed')
    return result


def api_client():
    spec=importlib.util.spec_from_file_location('reference_queue_driver',WORK/'reference_queue_migration.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.api


def deploy():
    api=api_client()
    workflow=emit_workflow()
    receipt_path=WORK/'reference_group_parallel_deployment.json'
    previous=json.loads(receipt_path.read_text()) if receipt_path.exists() else None
    head=api('git/ref/heads/'+(BRANCH if previous else 'main'))['object']['sha']
    if previous:
        assert head==previous['commit'], 'Diagnostic branch changed unexpectedly'
    base=api('git/commits/'+head)
    files={'.github/workflows/references.yml':workflow,'src/reference_group_probe.py':Path(__file__)}
    entries=[]
    for name,path in files.items():
        content=path.read_text(encoding='utf-8').replace('\r\n','\n')
        blob=api('git/blobs',{'content':content,'encoding':'utf-8'},method='POST')
        entries.append({'path':name,'mode':'100644','type':'blob','sha':blob['sha']})
    tree=api('git/trees',{'base_tree':base['tree']['sha'],'tree':entries},method='POST')
    commit=api('git/commits',{'message':'Probe independent reference Fourier groups without publishing data','tree':tree['sha'],'parents':[head]},method='POST')
    diff=api('compare/'+head+'...'+commit['sha'])
    changed={f['filename'] for f in diff['files']}
    assert changed and changed<=set(files)
    if previous:
        api('git/refs/heads/'+BRANCH,{'sha':commit['sha'],'force':False},method='PATCH')
    else:
        api('git/refs',{'ref':'refs/heads/'+BRANCH,'sha':commit['sha']},method='POST')
    receipt={'branch':BRANCH,'commit':commit['sha'],'base':head,'source_pin':PIN,'files':list(files),'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    (WORK/'reference_group_parallel_deployment.json').write_text(json.dumps(receipt,indent=2)+'\n')
    api('actions/workflows/references.yml/dispatches',{'ref':BRANCH},method='POST')
    print(json.dumps(receipt,indent=2))


def status():
    api=api_client()
    runs=api('actions/workflows/references.yml/runs?branch='+BRANCH+'&event=workflow_dispatch&per_page=3')['workflow_runs']
    for run in runs:
        print(json.dumps({key:run[key] for key in ['id','html_url','status','conclusion','created_at']}))
        jobs=api(f"actions/runs/{run['id']}/jobs?per_page=20")['jobs']
        print(json.dumps([{'id':j['id'],'name':j['name'],'status':j['status'],'conclusion':j['conclusion'],
            'steps':[{'name':s['name'],'status':s['status'],'conclusion':s['conclusion']} for s in j.get('steps',[]) if s['status']=='in_progress' or s.get('conclusion')=='failure']} for j in jobs],indent=2))
    (WORK/'reference_group_parallel_runs.json').write_text(json.dumps(runs,indent=2)+'\n')


def collect():
    api=api_client()
    runs=api('actions/workflows/references.yml/runs?branch='+BRANCH+'&event=workflow_dispatch&per_page=1')['workflow_runs']
    run=only_run=runs[0]
    artifacts=api(f"actions/runs/{run['id']}/artifacts?per_page=100")['artifacts']
    saved=[]
    for artifact in artifacts:
        if not artifact['name'].startswith('reference-group'):
            continue
        raw=api(f"actions/artifacts/{artifact['id']}/zip",raw=True)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            for member in archive.infolist():
                name=Path(member.filename).name
                if name!=member.filename or not name.startswith('reference-group-') or not name.endswith(('.json','.md','.log')):
                    continue
                (WORK/name).write_bytes(archive.read(member))
                saved.append(name)
    print(json.dumps({'run':run['html_url'],'status':run['status'],'saved':saved},indent=2))
    if all((WORK/f'reference-group-{role}.json').exists() for role in ['baseline','worker1','worker2']):
        result=compare(WORK)
        jobs=api(f"actions/runs/{run['id']}/jobs?per_page=20")['jobs']
        (WORK/'reference_group_parallel_job_times.json').write_text(json.dumps({'created_at':run['created_at'],'jobs':[
            {key:j.get(key) for key in ['name','started_at','completed_at','status','conclusion','steps']} for j in jobs]},indent=2)+'\n')


def validate():
    emit_workflow()
    compile(Path(__file__).read_text(),str(__file__),'exec')
    assert relative_vectors([[1.,0.]],[[1.,0.]])==0
    assert relative_vectors([[1.,0.]],[[0.,1.]])>1
    for a,b in [([[float('nan'),0]],[[1,0]]),([[1,0]],[]),([[1,0]],[[float('inf'),0]])]:
        try: relative_vectors(a,b)
        except AssertionError: pass
        else: raise AssertionError('Invalid trace was accepted')
    subprocess.run(['julia','--startup-file=no','-e',
        'e=Meta.parseall(read(ARGS[1],String)); @assert e.head==:toplevel; @assert all(x->!(x isa Expr && x.head in (:error,:incomplete)),e.args); println(:passed)',
        str(WORK/'reference_group_parallel.jl')],check=True)
    print('Python, workflow generation, Julia syntax and complex trace comparisons passed')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['generate','pin','compute','compare','deploy','status','collect','validate'])
    parser.add_argument('--role',choices=['baseline','worker1','worker2'])
    parser.add_argument('--directory')
    args=parser.parse_args()
    if args.action=='generate': print(emit_workflow())
    elif args.action=='pin': pin()
    elif args.action=='compute': compute(args.role)
    elif args.action=='compare': compare(args.directory)
    elif args.action=='deploy': deploy()
    elif args.action=='collect': collect()
    elif args.action=='validate': validate()
    else: status()

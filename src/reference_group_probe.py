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
import re
import statistics
import subprocess
import sys
import time
import zipfile

WORK = Path(__file__).resolve().parent
PIN = '3f3129935e20a28b466c306b58d4354e301eba39'
BRANCH = 'reference-group-parallel-probe-20261105'
REPO = 'brandynlucca/acousticTSbenchmark'
QUEUE_WORKFLOW_COMMIT = 'f1131ea8788a15b26d52e6ec65608dbe9dbfa446'


def combined_workflow(production, pilot):
    """Retain queue behavior and isolate optional numerical pilot jobs."""
    import copy
    import yaml
    original = yaml.safe_load(production)
    prefix, jobs = production.split('\njobs:\n', 1)
    input_marker = '  workflow_dispatch:\n    inputs:\n'
    assert prefix.count(input_marker) == 1
    prefix = prefix.replace(input_marker, input_marker +
        '      fourier_group_pilot:\n'
        '        description: Run the bounded Fourier-group diagnostic instead of the queue\n'
        '        type: boolean\n'
        '        default: false\n', 1)
    run_name = original['run-name'].strip()[3:-2].strip()
    replacement = ("run-name: ${{ inputs.fourier_group_pilot && "
        "'Reference Fourier-group diagnostic' || (" + run_name + ') }}')
    prefix, count = re.subn(r'^run-name:.*$', replacement, prefix, count=1, flags=re.M)
    assert count == 1
    for name, job in original['jobs'].items():
        condition = job.get('if')
        assert condition, 'Every production job requires an explicit gate'
        inner = condition.strip()
        if inner.startswith('${{'):
            inner = inner[3:-2].strip()
        gated = '${{ !inputs.fourier_group_pilot && (' + inner + ') }}'
        pattern = r'(^  ' + re.escape(name) + r':\n.*?^    if: )[^\n]*'
        jobs, count = re.subn(pattern, lambda match: match.group(1) + gated,
                            jobs, count=1, flags=re.M | re.S)
        assert count == 1, name
    pilot_jobs = copy.deepcopy(pilot['jobs'])
    for name, job in pilot_jobs.items():
        assert name not in original['jobs']
        condition = "inputs.fourier_group_pilot && inputs.queue_id == '' && inputs.timing_run_id == ''"
        if name == 'compare':
            condition = "always() && " + condition + " && needs.group.result != 'skipped'"
        job['if'] = '${{ ' + condition + ' }}'
        job['env'] = {**pilot.get('env', {}), **job.get('env', {})}
        job['permissions'] = {'contents': 'read'}
    appended = yaml.safe_dump({'jobs': pilot_jobs}, sort_keys=False).split('jobs:\n', 1)[1]
    candidate = prefix + '\njobs:\n' + jobs.rstrip() + '\n' + appended
    parsed = yaml.safe_load(candidate)
    assert set(parsed['jobs']) == set(original['jobs']) | set(pilot_jobs)
    for name, job in original['jobs'].items():
        preserved = dict(parsed['jobs'][name])
        preserved['if'] = job['if']
        assert preserved == job, name
    assert "<<'REFERENCE_QUEUE'\n" in candidate
    return candidate

CACHE_JS = r'''
const path = require('path');
const cache = require(path.join(process.env.RUNNER_TEMP, 'node_modules', '@actions/cache'));
const [action, key, filename] = process.argv.slice(2);
(async () => {
  if (action === 'save') {
    await cache.saveCache([filename], key);
  } else {
    const hit = await cache.restoreCache([filename], key, []);
    if (hit !== key) process.exitCode = 3;
  }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
'''

FULL_JULIA = r'''
using AcousticScattering, LinearAlgebra, JSON3, Serialization, SHA
const AS=AcousticScattering
BLAS.set_num_threads(1)
Threads.nthreads()==4 || error("Expected four Julia threads")
include(joinpath(ENV["GITHUB_WORKSPACE"],"src","worker_reference.jl"))
const role, output_path = ARGS
const worker_count=parse(Int,get(ENV,"GROUP_WORKERS","2"))
2<=worker_count<=4 || error("Expected two to four computational workers")
const work=ENV["RUNNER_TEMP"]
const cache_helper=joinpath(work,"reference-group-cache.js")
const prefix="reference-group-"*ENV["GITHUB_RUN_ID"]*"-"*ENV["GITHUB_RUN_ATTEMPT"]
const cutoff=time()+2100
const wave=Ref(0)
const events=Any[]
const source_hashes=Dict(p=>bytes2hex(sha256(read(joinpath(dirname(pathof(AS)),p)))) for p in
    ("solvers/coupled/coupled.jl","solvers/bem/axisymmetric.jl","solvers/bem/ring_kernel.jl"))

function transfer(action,key,filename)
    sink=IOBuffer()
    process=run(pipeline(ignorestatus(`node $cache_helper $action $key $filename`),stdout=sink,stderr=sink))
    process.exitcode==0 && return true
    action=="restore" && process.exitcode==3 && return false
    error("Reference transfer failed: "*String(take!(sink)))
end
function await_transfer(key,filename)
    while time()<cutoff
        transfer("restore",key,filename) && return
        sleep(2)
    end
    error("Reference transfer deadline reached")
end
function checkpoint(extra=(;))
    open(output_path*".tmp","w") do io
        JSON3.write(io,json_safe((;role,worker_count,source_hashes,events,extra...)))
    end
    mv(output_path*".tmp",output_path;force=true)
end
function request_at(frequency)
    Dict{String,Any}("boundary"=>"gas","semimajor_m"=>.07,"semiminor_m"=>.07/1.01,
        "frequency_hz"=>frequency,"incidence_deg"=>90.,"medium_density_kg_m3"=>1026.8,
        "medium_sound_speed_m_s"=>1477.3,"target_density_kg_m3"=>1.24,
        "target_sound_speed_m_s"=>345.,
        "controls"=>Dict("adaptive"=>true,"target_tol"=>.005,"max_refinements"=>8,
            "rtol"=>1e-6,"quadrature_extra"=>0,"chief_points"=>8,
            "boundary_order"=>5,"order"=>6,"mode_threads"=>4),
        "policy"=>Dict("spatial_tolerance_db"=>.005,"mode_increment"=>8))
end

function distributed_level(request)
    wave[]+=1
    id=wave[]
    id<=64 || error("Unexpected refinement request count")
    modes=get(request,:modes,0:request.m_max)
    groups=UnitRange{Int}[]
    for m in modes
        if isempty(groups) || m-first(last(groups))>=16
            push!(groups,m:m)
        else
            groups[end]=first(last(groups)):m
        end
    end
    get(request,:operator_spans,nothing)===nothing || (groups=request.operator_spans)
    fields=get(request,:fields,nothing)
    fields===nothing && (fields=Vector{AS._SpheroidSurfaceMode}(undef,request.m_max+1))
    bare=(; (k=>v for (k,v) in pairs(request) if !(k in (:reports,:fields,:modes,:operator_spans)))...)
    started=time()
    counts=Int[]
    for worker in 1:worker_count
        assigned=groups[worker:worker_count:end]
        selected=[m for m in modes if any(m in span for span in assigned)]
        push!(counts,length(selected))
        part=merge(bare,(;modes=selected,operator_spans=assigned))
        filename=joinpath(work,"reference-group-request-$worker.bin")
        serialize(filename,(;wave=id,source_hashes,stop=false,request=part))
        transfer("save","$prefix-request-$worker-$id",filename)
    end
    println("FREQUENCY_GROUPS_START f_khz=",request.k*1477.3/(2pi*1000),
        " wave=",id," nr=",request.nr," nt=",request.nt," nb=",request.nb,
        " modes=",counts," groups=",length(groups))
    flush(stdout)
    received=Int[]
    worker_seconds=Float64[]
    for worker in 1:worker_count
        filename=joinpath(work,"reference-group-response-$worker.bin")
        await_transfer("$prefix-response-$worker-$id",filename)
        response=deserialize(filename)
        response.wave==id && response.source_hashes==source_hashes || error("Invalid worker response identity")
        for (m,field) in response.fields
            m in received && error("Duplicate mode response")
            push!(received,m)
            fields[m+1]=field
        end
        request.reports===nothing || append!(request.reports.systems,response.systems)
        push!(worker_seconds,response.seconds)
    end
    sort(received)==sort(collect(modes)) || error("Missing distributed Fourier modes")
    push!(events,(;wave=id,frequency_hz=request.k*1477.3/(2pi),nr=request.nr,nt=request.nt,
        nb=request.nb,modes=collect(modes),groups=[collect(s) for s in groups],
        wall_seconds=time()-started,worker_seconds))
    checkpoint()
    println("FREQUENCY_GROUPS_DONE wave=",id," elapsed_seconds=",round(time()-started;digits=2))
    flush(stdout)
    fields
end

function install_dispatcher()
    filename=joinpath(dirname(pathof(AS)),"solvers","coupled","coupled.jl")
    source=read(filename,String)
    a=first(findfirst("function _spheroid_fembem_level(",source))
    b=first(findnext("function _spheroid_fembem_level_setup(",source,a+1))-1
    code=source[a:b]
    marker="    batch=get(task_local_storage(), :acousticscattering_fembem_batch, nothing)"
    count(marker,code)==1 || error("Pinned dispatcher boundary changed")
    code=replace(code,marker=>"    dispatcher=get(task_local_storage(), :reference_group_dispatcher, nothing)\n    dispatcher===nothing || return dispatcher(request)\n"*marker)
    Base.include_string(AS,code,"reference_group_dispatcher.jl")
end

function finish_workers()
    id=wave[]+1
    for worker in 1:worker_count
        filename=joinpath(work,"reference-group-request-$worker.bin")
        serialize(filename,(;wave=id,source_hashes,stop=true))
        transfer("save","$prefix-request-$worker-$id",filename)
    end
end

function worker_loop()
    worker=parse(Int,string(last(role)))
    cache=AS._ModeSolutionReuse(16*1024^2)
    task_local_storage(:acousticscattering_mode_solution_reuse,cache) do
        for id in 1:65
            input=joinpath(work,"reference-group-request-$worker.bin")
            await_transfer("$prefix-request-$worker-$id",input)
            packet=deserialize(input)
            packet.wave==id && packet.source_hashes==source_hashes || error("Invalid coordinator request identity")
            packet.stop && break
            request=packet.request
            (;body,boundary,k,nr,nt,nb,order,boundary_order)=request
            options=Base.structdiff(request,(;body,boundary,k,nr,nt,nb,order,boundary_order))
            reports=AS._SolveReports()
            started=time()
            selected=Pair{Int,AS._SpheroidSurfaceMode}[]
            println("FREQUENCY_GROUP_WORKER_START worker=",worker," wave=",id,
                " f_khz=",k*1477.3/(2pi*1000)," modes=",length(request.modes))
            flush(stdout)
            if !isempty(request.modes)
                result=AS._spheroid_fembem_level(body,boundary,k,nr,nt,nb,order,boundary_order;
                    options...,reports)
                append!(selected,[m=>result[m+1] for m in request.modes])
            end
            seconds=time()-started
            output=joinpath(work,"reference-group-response-$worker.bin")
            serialize(output,(;wave=id,source_hashes,fields=selected,systems=reports.systems,seconds))
            transfer("save","$prefix-response-$worker-$id",output)
            push!(events,(;wave=id,frequency_hz=k*1477.3/(2pi),seconds,modes=collect(request.modes),
                cache_bytes=cache.bytes,cache_hits=cache.hits,modes_reused=cache.modes_reused))
            checkpoint()
            println("FREQUENCY_GROUP_WORKER_DONE worker=",worker," wave=",id,
                " elapsed_seconds=",round(seconds;digits=2))
            flush(stdout)
        end
    end
end

function assert_qualified(evidence)
    evidence["status"]=="ok" || error("Baseline failed spatial qualification")
    baseline,expanded=evidence["baseline"],evidence["expanded"]
    expanded===nothing && error("Missing independent Fourier extension")
    expanded.status=="ok" || error("Expanded solution failed spatial qualification")
    delta=abs(baseline.ts_db-expanded.ts_db)
    isfinite(delta) && delta<=.005 || error("Fourier qualification failed")
    relative=abs(baseline.amplitude-expanded.amplitude)/max(abs(baseline.amplitude),abs(expanded.amplitude),1e-10)
    isfinite(relative) && relative<=.001 || error("Complex Fourier qualification failed")
    baseline.diagnostics["solver_report"].solver_options.m_max+8==
        expanded.diagnostics["solver_report"].solver_options.m_max || error("Incorrect Fourier extension")
    delta
end

if role=="coordinator"
    install_dispatcher()
    controls=Dict(Symbol(k)=>v for (k,v) in request_at(20000.)["controls"])
    original=Base.invokelatest(reference_calculation,request_at(20000.))
    assert_qualified(original)
    smoke=task_local_storage(:reference_group_dispatcher,distributed_level) do
        Base.invokelatest(reference_calculation,request_at(20000.))
    end
    assert_qualified(smoke)
    relative=abs(smoke["baseline"].amplitude-original["baseline"].amplitude)/abs(original["baseline"].amplitude)
    relative<=1e-11 || error("Full adaptive smoke comparison failed")
    println("FULL_ADAPTIVE_SMOKE_PASSED relative_complex=",relative)
    flush(stdout)
    started=time()
    evidence=task_local_storage(:reference_group_dispatcher,distributed_level) do
        Base.invokelatest(reference_calculation,request_at(299000.))
    end
    elapsed=time()-started
    delta=assert_qualified(evidence)
    checkpoint((;evidence,elapsed_seconds=elapsed,fourier_delta_db=delta,smoke_relative_complex=relative))
    finish_workers()
    println("FULL_FREQUENCY_QUALIFIED f_khz=299 elapsed_seconds=",round(elapsed;digits=2)," fourier_delta_db=",delta)
    flush(stdout)
else
    worker_loop()
end
'''

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


def emit_workflow(full=False, workers=2):
    assert 2 <= workers <= 4
    assert full or workers == 2, 'Worker-count comparison requires the full adaptive solve'
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
    if full:
        wf['run-name']='Reference full-frequency distribution: adaptive qualification'
        wf['env']['ACTIONS_CACHE_SERVICE_V2']='true'
        wf['env']['SEGMENT_DOWNLOAD_TIMEOUT_MINS']='2'
        group=wf['jobs']['group']
        group['strategy']['matrix']['role']=['coordinator']+[f'worker{i}' for i in range(1,workers+1)]
        group['strategy']['max-parallel']=workers+1
        group['timeout-minutes']=48
        group['env']={'GROUP_ROLE':'${{ matrix.role }}','GROUP_WORKERS':str(workers)}
        group['name']=f'${{{{ matrix.role }}}}: {workers} x 4 threads, 299 kHz coupled FEM-BEM'
        for step in group['steps']:
            if step.get('name')=='Prepare pinned numerical environment':
                step['run'] += '\nnpm install --prefix "$RUNNER_TEMP" --no-audit --no-fund @actions/cache@4.0.0'
            if step.get('name')=='Measure unchanged Fourier refinement requests':
                step.clear()
                step.update(name='Complete adaptive frequency through shared worker jobs',
                    uses='actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea',
                    **{'timeout-minutes':37,'with':{'script':
                        "await exec.exec('python', ['src/reference_group_probe.py', 'compute', '--role', process.env.GROUP_ROLE, '--full']);"}})
            if step.get('name')=='Retain numerical traces and timing evidence':
                step['with'].update(name='reference-full-${{ matrix.role }}',
                    path='${{ runner.temp }}/reference-full-*.json\n${{ runner.temp }}/reference-full-*.log')
        report=wf['jobs']['compare']
        report['name']='Combined full-frequency qualification and elapsed time'
        report['steps'][1]['with']['pattern']='reference-full-*'
        report['steps'][2]['run']='python src/reference_group_probe.py compare-full --directory "$RUNNER_TEMP/reference-group-results"'
        report['steps'][3]['with'].update(name='reference-full-comparison',
            path='${{ runner.temp }}/reference-group-results/reference-full-comparison.json\n${{ runner.temp }}/reference-group-results/reference-full-comparison.md')
        if workers != 2:
            report['steps'][2]['run'] += ' --previous-directory "$RUNNER_TEMP/reference-prior"'
            report['steps'].insert(2, {'name':'Read completed two-worker evidence',
                'uses':'actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093',
                'with':{'pattern':'reference-full-*','run-id':'42138971436',
                    'github-token':'${{ github.token }}','path':'${{ runner.temp }}/reference-prior',
                    'merge-multiple':True}})
    target=WORK/'reference_group_parallel.yml'
    production = (WORK/'reference_group_parallel_production.yml').read_text(encoding='utf-8')
    combined=combined_workflow(production, wf)
    if full and workers != 2:
        # Cross-run comparison reads retained evidence without rerunning the baseline.
        position=combined.index('  compare:\n')
        head,tail=combined[:position],combined[position:]
        assert tail.count('    permissions:\n      contents: read\n')==1
        tail=tail.replace('    permissions:\n      contents: read\n',
            '    permissions:\n      contents: read\n      actions: read\n',1)
        combined=head+tail
    target.write_text(combined, encoding='utf-8')
    (WORK/'reference_group_parallel.jl').write_text(JULIA,encoding='utf-8')
    (WORK/'reference_group_parallel_full.jl').write_text(FULL_JULIA,encoding='utf-8')
    (WORK/'reference_group_parallel_cache.js').write_text(CACHE_JS,encoding='utf-8')
    return target


def pin():
    temporary=Path(os.environ['RUNNER_TEMP'])
    lock=Path('sources.lock.json')
    value=json.loads(lock.read_text())
    value['sources']['acousticscattering']['sha']=PIN
    lock.write_text(json.dumps(value,indent=2)+'\n')
    (temporary/'reference-group-start.json').write_text(json.dumps({'epoch':time.time(),'pin':PIN}))


def compute(role,full=False):
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
    script.write_text(FULL_JULIA if full else JULIA)
    if full:
        (temporary/'reference-group-cache.js').write_text(CACHE_JS)
        assert os.environ.get('ACTIONS_RUNTIME_TOKEN'), 'Actions runtime credentials unavailable'
    stem='reference-full' if full else 'reference-group'
    output=temporary/f'{stem}-{role}.json'
    command += [str(script),role,str(output)]
    env=os.environ.copy()
    env.update(runtime.get('env',{}))
    before=time.time()
    peak=cpu=0.0
    seen=0
    log_path=temporary/f'{stem}-{role}.log'
    with log_path.open('w') as log:
        process=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
        observer=psutil.Process(process.pid)
        heartbeat=0.0
        while process.poll() is None:
            now=time.time()
            if now-before>(2160 if full else 840):
                process.terminate()
                try: process.wait(10)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
                raise RuntimeError('Bounded pilot reached compute limit')
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
                    print('GROUP_PROGRESS role='+role+' completed_blocks='+str(len(saved.get('rows',saved.get('events',[])))),flush=True)
                if full:
                    contents=log_path.read_text(errors='replace')
                    print(contents[seen:][-8000:],flush=True)
                    seen=len(contents)
                heartbeat=now
            time.sleep(1)
        process_seconds=time.time()-before
    print(log_path.read_text()[seen:],flush=True)
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


def compare_full(directory, previous_directory=None):
    directory=Path(directory)
    coordinator=json.loads((directory/'reference-full-coordinator.json').read_text())
    workers=int(coordinator.get('worker_count',2))
    data={role:json.loads((directory/f'reference-full-{role}.json').read_text())
          for role in ['coordinator']+[f'worker{i}' for i in range(1,workers+1)]}
    assert all(int(d.get('worker_count',2))==workers for d in data.values())
    assert all(d['source_hashes']==coordinator['source_hashes'] for d in data.values())
    evidence=coordinator['evidence']
    checks=[]
    for name in ['baseline','expanded']:
        stage=evidence[name]
        report=stage['diagnostics']['solver_report']
        assert stage['status']=='ok' and report.get('converged',True) is not False
        refinement=report['refinement']
        assert refinement['converged'] and math.isfinite(refinement['change_db']) and refinement['change_db']<=.005
        checks.append({'stage':name,'ts_db':stage['ts_db'],'spatial_change_db':refinement['change_db'],
                       'm_max':report['solver_options']['m_max'],'amplitude':stage['amplitude']})
    assert coordinator['fourier_delta_db']<=.005 and coordinator['smoke_relative_complex']<=1e-11
    amplitudes=[complex(r['amplitude']['real'],r['amplitude']['imag']) for r in checks]
    relative_complex=abs(amplitudes[0]-amplitudes[1])/max(abs(amplitudes[0]),abs(amplitudes[1]),1e-10)
    assert math.isfinite(relative_complex) and relative_complex<=.001
    assert checks[1]['m_max']==checks[0]['m_max']+8
    actual=[e for e in coordinator['events'] if abs(e['frequency_hz']-299000)<.01]
    assert all(len(e['worker_seconds'])==workers for e in actual)
    result={'qualified':True,'source_sha':PIN,'case':{'a_m':.07,'aspect_ratio':1.01,'angle_deg':90.,'frequency_hz':299000.,'boundary':'gas'},
        'computational_workers':workers,'threads_per_worker':4,'computational_threads':4*workers,
        'elapsed_seconds':coordinator['elapsed_seconds'],'stages':checks,
        'fourier_delta_db':coordinator['fourier_delta_db'],
        'fourier_relative_complex':relative_complex,
        'full_adaptive_smoke_relative_complex':coordinator['smoke_relative_complex'],
        'refinement_waves':len(actual),'wave_wall_seconds':sum(e['wall_seconds'] for e in actual),
        'critical_worker_compute_seconds':sum(max(e['worker_seconds']) for e in actual),
        'runners':{role:{key:d[key] for key in ['cpu_model','peak_rss_MiB','setup_seconds','process_seconds']} for role,d in data.items()},
        'publication':'Diagnostic evidence only. No dashboard/reference record changed by the pilot.'}
    if previous_directory:
        previous_directory=Path(previous_directory)
        previous_prefix=('reference-full-2workers-' if
            (previous_directory/'reference-full-2workers-coordinator.json').exists() else 'reference-full-')
        previous=json.loads((previous_directory/(previous_prefix+'coordinator.json')).read_text())
        assert previous['source_hashes']==coordinator['source_hashes']
        comparisons=[]
        for name in ('baseline','expanded'):
            left=previous['evidence'][name]
            right=evidence[name]
            a=complex(left['amplitude']['real'],left['amplitude']['imag'])
            b=complex(right['amplitude']['real'],right['amplitude']['imag'])
            relative=abs(a-b)/max(abs(a),abs(b),1e-10)
            delta=right['ts_db']-left['ts_db']
            old_refinement=left['diagnostics']['solver_report']['refinement']
            new_refinement=right['diagnostics']['solver_report']['refinement']
            meshes_equal=all(old_refinement.get(key)==new_refinement.get(key)
                for key in ('mode_n_r','mode_n_theta','mode_n_boundary','mode_refinements'))
            assert meshes_equal, 'Adaptive meshes changed during worker-count comparison'
            assert math.isfinite(relative) and relative<=.001 and abs(delta)<=.005
            comparisons.append(dict(stage=name,delta_ts_db=delta,relative_complex_difference=relative,
                                    adaptive_meshes_equal=meshes_equal))
        old_workers=int(previous.get('worker_count',2))
        old_runners={f'worker{i}':json.loads((previous_directory/(previous_prefix+f'worker{i}.json')).read_text())
                     for i in range(1,old_workers+1)}
        old_seconds=previous['elapsed_seconds']
        result['previous_worker_comparison']=dict(
            previous_workers=old_workers,previous_elapsed_seconds=old_seconds,
            current_elapsed_seconds=result['elapsed_seconds'],
            elapsed_reduction_percent=100*(1-result['elapsed_seconds']/old_seconds),
            numerical_comparisons=comparisons,
            previous_worker_cpu_models={role:d['cpu_model'] for role,d in old_runners.items()},
            current_worker_cpu_models={role:d['cpu_model'] for role,d in data.items() if role!='coordinator'},
            limitation='Different runner allocations prevent attribution of the elapsed-time change solely to worker count.')
    (directory/'reference-full-comparison.json').write_text(json.dumps(result,indent=2)+'\n')
    lines=['Complete adaptive reference distribution','',
        'Gas, a=0.07 m, aspect ratio 1.01, 90 degrees, 299 kHz.','',
        f'Computational workers: {workers}, with four threads per worker and {workers*4} computational threads.',
        f"Qualified frequency elapsed time: {result['elapsed_seconds']:.2f} s.",
        f"Fourier TS change: {result['fourier_delta_db']:.9g} dB.",
        f"Complete 20 kHz serial/distributed relative complex difference: {result['full_adaptive_smoke_relative_complex']:.3e}.",'',
        '| Stage | Spatial TS change (dB) | Maximum Fourier index |','| --- | ---: | ---: |']
    lines += [f"| {r['stage']} | {r['spatial_change_db']:.9g} | {r['m_max']} |" for r in checks]
    lines += ['',f"Adaptive waves: {len(actual)}. Critical worker computation: {result['critical_worker_compute_seconds']:.2f} s.",
        'Full-frequency speedup requires a matched serial timing. Existing 30-minute timeout evidence provides a separate historical comparison.','',result['publication'],'']
    if previous_directory:
        difference=result['previous_worker_comparison']
        lines += [f"Previous {old_workers}-worker elapsed time: {old_seconds:.2f} s.",
            f"Observed elapsed-time reduction: {difference['elapsed_reduction_percent']:.2f}%.",
            difference['limitation'],'']
    report='\n'.join(lines)
    (directory/'reference-full-comparison.md').write_text(report)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as stream: stream.write(report)
    print(report)
    return result


def prepare_record():
    report=compare_full(WORK)
    coordinator=json.loads((WORK/'reference-full-coordinator.json').read_text())
    plan=json.loads((WORK/'reference_queue_plan.json').read_text())
    matches=[j for task in plan['tasks'] for j in task['jobs']
             if j['semimajor_m']==.07 and abs(j['aspect_ratio']-1.01)<1e-12
             and j['incidence_deg']==90 and j['boundary']=='gas' and j['frequency_hz']==299000]
    assert len(matches)==1
    job=json.loads(json.dumps(matches[0]))
    assert job['source_sha']==PIN
    api=api_client()
    runs=api('actions/workflows/references.yml/runs?branch='+BRANCH+'&event=workflow_dispatch&per_page=1')['workflow_runs']
    run=runs[0]
    assert run['status']=='completed'
    jobs=api(f"actions/runs/{run['id']}/jobs?per_page=30")['jobs']
    numerical=[j for j in jobs if j['name'].split(':')[0] in ('coordinator','worker1','worker2')]
    assert len(numerical)==3 and all(j['conclusion']=='success' for j in numerical)
    source=api('contents/src/reference_group_probe.py?ref='+run['head_sha'])
    adapter_hash=hashlib.sha256(base64.b64decode(source['content'])).hexdigest()
    sys.path.insert(0,r'C:\Users\Brandyn\GitHub\acousticTSbenchmark\src')
    import references
    from benchmark import fingerprint
    adapters={}
    for filename in ['references.py','worker_reference.jl','worker_julia.jl']:
        content=api('contents/src/'+filename+'?ref='+run['head_sha'])
        adapters[filename]=hashlib.sha256(base64.b64decode(content['content'])).hexdigest()
    adapters['reference_group_probe.py']=adapter_hash
    job['signature']['adapters']=adapters
    job['reference_id']=fingerprint(job['signature'])
    evidence=coordinator['evidence']
    evidence.update(elapsed_seconds=coordinator['elapsed_seconds'],runtime_version='1.12.1',
        julia_threads=4,blas_threads=1,distributed_worker_jobs=2)
    provenance={'kind':'distributed_reference_pilot','run_url':run['html_url'],
        'execution_source_sha':run['head_sha'],'execution_adapter_sha256':adapter_hash,
        'numerical_source_sha256':coordinator['source_hashes'],
        'serial_distributed_smoke_relative_complex':coordinator['smoke_relative_complex'],
        'worker_jobs':report['runners'],'standard_reference_policy':True}
    record=references.reference_record(job,evidence,provenance)
    assert record['qualified'] and references.valid_cached(record,job)
    old=json.loads((WORK/'reference_performance_full_accelerated2_299000_pass2verified.json').read_text())
    previous=old['result']['values']['expanded']
    amplitude=complex(record['amplitude_real_m'],record['amplitude_imag_m'])
    previous_amplitude=complex(*previous['amplitude'])
    comparison={'ts_difference_db':record['ts_db']-previous['ts_db'],
        'relative_complex_difference':abs(amplitude-previous_amplitude)/abs(previous_amplitude)}
    # Cross-implementation agreement uses the existing physical reference policy.
    # The earlier Windows result used different numerical sources, so an extra
    # machine-level identity threshold does not characterize distribution error.
    for stage in ('baseline','expanded'):
        fresh=coordinator['evidence'][stage]['diagnostics']['solver_report']['refinement']
        prior=old['result']['values'][stage]['refinement']
        for key in ('mode_n_r','mode_n_theta','mode_n_boundary','mode_refinements'):
            assert fresh.get(key)==prior.get(key),(stage,key)
    comparison['per_mode_meshes_equal']=True
    assert abs(comparison['ts_difference_db'])<=job['policy']['spatial_tolerance_db'],comparison
    assert comparison['relative_complex_difference']<=job['policy']['relative_complex_tolerance'],comparison
    record['provenance']['earlier_converged_comparison']=comparison
    (WORK/(job['reference_id']+'.json')).write_text(json.dumps(record,separators=(',',':'),allow_nan=False)+'\n')
    (WORK/'reference_group_parallel_qualified_job.json').write_text(json.dumps(job,indent=2)+'\n')
    receipt={'reference_id':job['reference_id'],'physical_id':job['physical_id'],
        'qualified':True,'qualification':record['qualification'],'earlier_comparison':comparison,
        'run_url':run['html_url'],'elapsed_seconds':coordinator['elapsed_seconds']}
    (WORK/'reference_group_parallel_qualification.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2))


def publish_record():
    sys.path.insert(0,r'C:\Users\Brandyn\GitHub\acousticTSbenchmark\src')
    import reference_curves as curves
    import reference_store
    import references
    from publish_progress import GitHub, APIError
    job=json.loads((WORK/'reference_group_parallel_qualified_job.json').read_text())
    record=json.loads((WORK/(job['reference_id']+'.json')).read_text())
    assert references.valid_cached(record,job)
    token=subprocess.run(['gh','auth','token'],capture_output=True,check=True,text=True).stdout.strip()
    api=GitHub(REPO,token)
    def shared():
        entry=api.request('GET','/contents/qualified-reference-curves.json?ref=benchmark-data')
        if entry.get('encoding')!='base64' or not entry.get('content'):
            entry=api.request('GET','/git/blobs/'+entry['sha'])
        payload=json.loads(base64.b64decode(entry['content']))
        return curves._validated(payload['reference_curves'])
    before=shared()
    physical=job['physical_id']
    receipt={'reference_id':job['reference_id'],'physical_id':physical,
        'count_before':len(before),'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    if physical in before:
        receipt.update(status='existing_qualified_reference_preserved',selected_reference_id=before[physical]['reference_id'])
    else:
        receipt['full_records_saved']=reference_store.publish_cache(api,[job],WORK,branch='reference-data')
        addition=curves.curves_from_records([job],{job['reference_id']:record})
        assert len(addition)==1
        try:
            receipt['new_curves']=curves.publish_shared_curves(api,addition)
        except ValueError:
            fresh=shared()
            if physical not in fresh: raise
            receipt['concurrent_qualified_reference_preserved']=True
        after=shared()
        assert physical in after and set(before)<=set(after)
        receipt.update(status='qualified_reference_visible',count_after=len(after),selected_reference_id=after[physical]['reference_id'])
    (WORK/'reference_group_parallel_publication.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2))


def fix_remote_report():
    api=api_client()
    head=api('git/ref/heads/'+BRANCH)['object']['sha']
    original=api('contents/src/reference_group_probe.py?ref='+head)
    source=base64.b64decode(original['content']).decode()
    before="assert stage['status']=='ok' and report.get('converged',True)"
    assert source.count(before)==1
    source=source.replace(before,before+' is not False',1)
    marker="    assert coordinator['fourier_delta_db']<=.005 and coordinator['smoke_relative_complex']<=1e-11\n"
    assert source.count(marker)==1
    extra="""    amplitudes=[complex(r['amplitude']['real'],r['amplitude']['imag']) for r in checks]
    relative_complex=abs(amplitudes[0]-amplitudes[1])/max(abs(amplitudes[0]),abs(amplitudes[1]),1e-10)
    assert math.isfinite(relative_complex) and relative_complex<=.001
    assert checks[1]['m_max']==checks[0]['m_max']+8
"""
    source=source.replace(marker,marker+extra,1)
    namespace={'__name__':'reference_report_check','__file__':str(WORK/'reference_group_parallel.py')}
    exec(compile(source,'reference_group_report_candidate.py','exec'),namespace)
    namespace['compare_full'](WORK)
    result=api('contents/src/reference_group_probe.py',dict(branch=BRANCH,sha=original['sha'],
        message='Match the reference report to production convergence validation',
        content=base64.b64encode(source.encode()).decode()),method='PUT')
    receipt={'before':head,'commit':result['commit']['sha'],'file':'src/reference_group_probe.py',
        'validation':'Saved complete frequency passed, no numerical calculation repeated'}
    (WORK/'reference_group_parallel_report_fix.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2))


def api_client():
    spec=importlib.util.spec_from_file_location('reference_queue_driver',WORK/'reference_queue_migration.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.api


def repair_workflow(publish=False):
    import yaml
    api = api_client()
    head = api('git/ref/heads/' + BRANCH)['object']['sha']
    assert head == '4dd2ad1a886dde15fa9c4f47ddbf57c299bb2594', 'Diagnostic branch changed'
    path = '.github/workflows/references.yml'
    production_blob = api('contents/' + path + '?ref=' + QUEUE_WORKFLOW_COMMIT)
    pilot_blob = api('contents/' + path + '?ref=' + head)
    production = base64.b64decode(production_blob['content']).decode()
    pilot = base64.b64decode(pilot_blob['content']).decode()
    candidate = combined_workflow(production, yaml.safe_load(pilot))
    (WORK/'reference_group_parallel_production.yml').write_text(production, encoding='utf-8')
    (WORK/'reference_group_parallel_workflow_repair.yml').write_text(candidate, encoding='utf-8')

    # Exercise the original cache-identity test against the proposed workflow.
    import unittest
    from unittest.mock import patch
    tests = Path(r'C:\Users\Brandyn\GitHub\acousticTSbenchmark\tests')
    sys.path.insert(0, str(tests))
    import test_references
    read_text = Path.read_text
    workflow_path = tests.parent / path
    def proposed_text(target, *args, **kwargs):
        if target == workflow_path:
            return candidate
        return read_text(target, *args, **kwargs)
    suite = unittest.TestSuite([test_references.ReferenceTests(
        'test_workflow_reference_queue_completes_sweeps_without_changing_cache_keys')])
    with patch.object(Path, 'read_text', proposed_text):
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    assert result.wasSuccessful()
    receipt = {'before': head, 'restored_from': QUEUE_WORKFLOW_COMMIT,
        'file': path, 'validation': 'Original scheduling and cache-identity regression passed',
        'production_jobs_preserved': list(yaml.safe_load(production)['jobs']),
        'pilot_default': False, 'numerical_jobs_dispatched': 0}
    if publish:
        changed = api('contents/' + path, dict(branch=BRANCH, sha=pilot_blob['sha'],
            message='Preserve reference queue behavior alongside the opt-in Fourier pilot',
            content=base64.b64encode(candidate.encode()).decode()), method='PUT')
        receipt['commit'] = changed['commit']['sha']
        diff = api('compare/' + head + '...' + receipt['commit'])
        assert [entry['filename'] for entry in diff['files']] == [path]
    (WORK/'reference_group_parallel_workflow_repair.json').write_text(
        json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(receipt, indent=2))


def deploy(full=False,workers=2):
    api=api_client()
    workflow=emit_workflow(full,workers)
    receipt_path=WORK/'reference_group_parallel_deployment.json'
    previous=json.loads(receipt_path.read_text()) if receipt_path.exists() else None
    head=api('git/ref/heads/'+(BRANCH if previous else 'main'))['object']['sha']
    if previous:
        expected=previous['commit']
        if 'workers' not in previous:
            expected=json.loads((WORK/'reference_group_parallel_workflow_repair.json').read_text())['commit']
        assert head==expected, 'Diagnostic branch changed unexpectedly'
    base=api('git/commits/'+head)
    files={'.github/workflows/references.yml':workflow,'src/reference_group_probe.py':Path(__file__)}
    entries=[]
    for name,path in files.items():
        content=path.read_text(encoding='utf-8').replace('\r\n','\n')
        blob=api('git/blobs',{'content':content,'encoding':'utf-8'},method='POST')
        entries.append({'path':name,'mode':'100644','type':'blob','sha':blob['sha']})
    tree=api('git/trees',{'base_tree':base['tree']['sha'],'tree':entries},method='POST')
    commit=api('git/commits',{'message':f'Compare {workers} four-thread reference workers with retained two-worker evidence','tree':tree['sha'],'parents':[head]},method='POST')
    diff=api('compare/'+head+'...'+commit['sha'])
    changed={f['filename'] for f in diff['files']}
    assert changed and changed<=set(files)
    if previous:
        api('git/refs/heads/'+BRANCH,{'sha':commit['sha'],'force':False},method='PATCH')
    else:
        api('git/refs',{'ref':'refs/heads/'+BRANCH,'sha':commit['sha']},method='POST')
    receipt={'branch':BRANCH,'commit':commit['sha'],'base':head,'source_pin':PIN,'files':list(files),
        'full':full,'workers':workers,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    (WORK/'reference_group_parallel_deployment.json').write_text(json.dumps(receipt,indent=2)+'\n')
    api('actions/workflows/references.yml/dispatches',
        {'ref':BRANCH,'inputs':{'fourier_group_pilot':True}},method='POST')
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
    existing=WORK/'reference-full-coordinator.json'
    if existing.exists() and int(json.loads(existing.read_text()).get('worker_count',2))==2:
        for role in ('coordinator','worker1','worker2','comparison'):
            for suffix in ('.json','.log','.md'):
                original=WORK/('reference-full-'+role+suffix)
                saved=WORK/('reference-full-2workers-'+role+suffix)
                if original.exists() and not saved.exists():
                    saved.write_bytes(original.read_bytes())
    saved=[]
    for artifact in artifacts:
        if not artifact['name'].startswith(('reference-group','reference-full')):
            continue
        raw=api(f"actions/artifacts/{artifact['id']}/zip",raw=True)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            for member in archive.infolist():
                name=Path(member.filename).name
                if name!=member.filename or not name.startswith(('reference-group-','reference-full-')) or not name.endswith(('.json','.md','.log')):
                    continue
                (WORK/name).write_bytes(archive.read(member))
                saved.append(name)
    print(json.dumps({'run':run['html_url'],'status':run['status'],'saved':saved},indent=2))
    if 'reference-full-coordinator.json' in saved:
        compare_full(WORK,WORK if (WORK/'reference-full-2workers-coordinator.json').exists() else None)
    elif all((WORK/f'reference-group-{role}.json').exists() for role in ['baseline','worker1','worker2']):
        result=compare(WORK)
        jobs=api(f"actions/runs/{run['id']}/jobs?per_page=20")['jobs']
        (WORK/'reference_group_parallel_job_times.json').write_text(json.dumps({'created_at':run['created_at'],'jobs':[
            {key:j.get(key) for key in ['name','started_at','completed_at','status','conclusion','steps']} for j in jobs]},indent=2)+'\n')


def validate(full=False,workers=2):
    emit_workflow(full,workers)
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
    subprocess.run(['julia','--startup-file=no','-e',
        'e=Meta.parseall(read(ARGS[1],String)); @assert e.head==:toplevel; @assert all(x->!(x isa Expr && x.head in (:error,:incomplete)),e.args); println(:passed)',
        str(WORK/'reference_group_parallel_full.jl')],check=True)
    subprocess.run(['node','--check',str(WORK/'reference_group_parallel_cache.js')],check=True)
    print('Python, workflow generation, Julia syntax and complex trace comparisons passed')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('action',choices=['generate','pin','compute','compare','compare-full','deploy','status','collect','validate','prepare-record','publish-record','fix-report','repair-workflow'])
    parser.add_argument('--role',choices=['baseline','worker1','worker2','worker3','worker4','coordinator'])
    parser.add_argument('--directory')
    parser.add_argument('--previous-directory')
    parser.add_argument('--workers',type=int,choices=[2,3,4],default=2)
    parser.add_argument('--full',action='store_true')
    parser.add_argument('--publish',action='store_true')
    args=parser.parse_args()
    if args.action=='generate': print(emit_workflow(args.full,args.workers))
    elif args.action=='pin': pin()
    elif args.action=='compute': compute(args.role,args.full)
    elif args.action=='compare': compare(args.directory)
    elif args.action=='compare-full': compare_full(args.directory,args.previous_directory)
    elif args.action=='deploy': deploy(args.full,args.workers)
    elif args.action=='collect': collect()
    elif args.action=='validate': validate(args.full,args.workers)
    elif args.action=='prepare-record': prepare_record()
    elif args.action=='publish-record': publish_record()
    elif args.action=='fix-report': fix_remote_report()
    elif args.action=='repair-workflow': repair_workflow(args.publish)
    else: status()

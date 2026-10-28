"""Render a self-contained progress view without work during solver timing."""

import json
import os
from pathlib import Path


def render_report(path, rows, status, repository=None):
    from benchmark import CAPABILITIES
    from reference_curves import curves_from_rows

    payload = json.dumps({"rows": rows, "status": status, "repository": repository,
                          "reference_curves": curves_from_rows(rows),
                          "capabilities": CAPABILITIES}, allow_nan=False).replace("<", "\\u003c")
    document = '''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta http-equiv="refresh" content="60"><title>Spheroid timing progress</title>
<style>
body{font:16px system-ui,sans-serif;color:#20252b;background:#fafaf8;margin:28px auto;max-width:1300px;padding:0 20px}
h1{font-size:25px}label{margin-right:24px}select{font:inherit;padding:5px}a{color:#005b88}
#panels{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(430px,100%),1fr));gap:16px}
#panels.paired{grid-template-columns:1fr}.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(430px,100%),1fr));gap:14px}h3{font-size:14px;font-weight:500;margin:12px 0 0}.empty{fill:#626a70}
section{background:white;border:1px solid #d6dadb;padding:14px}h2{font-size:17px;margin:0}
svg{width:100%;height:auto}text{font:12px system-ui}table{border-collapse:collapse;margin-top:20px;width:100%;font-size:14px}
th,td{text-align:left;padding:7px;border-bottom:1px solid #ddd}#legend span{display:inline-block;margin:10px 16px 12px 0}
#angle-legend{display:flex;align-items:center;flex-wrap:wrap;gap:8px 20px;margin-bottom:12px}#angle-legend span{display:inline-flex;align-items:center;gap:5px}#angle-legend svg{width:58px;height:22px}
.coverage{overflow-x:auto}label{display:inline-block;margin-bottom:10px}select{max-width:100%}
.note{color:#505962;font-size:14px;line-height:1.5}
#connection{padding:12px 16px;background:#eaf1f4;border-left:3px solid #0072b2;line-height:1.5}
#data-run-warning{padding:12px 16px;background:#fff2d7;border-left:4px solid #946000;line-height:1.5}
[hidden]{display:none!important}#livebar{display:flex;align-items:baseline;gap:20px;flex-wrap:wrap}
.completion-heading{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap}.completion-heading label{margin:0;font-size:14px}
#benchmark-progress{margin:8px 0 18px}#software-progress{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(240px,100%),1fr));gap:16px}
.progress-item{min-width:0}.progress-title{display:flex;justify-content:space-between;gap:12px;font-size:14px;margin-bottom:6px}.progress-title strong{font-weight:600}
.progress-track{display:flex;height:10px;background:#e9edef;border-radius:3px;overflow:hidden}.progress-track span{background:var(--software-color);flex-shrink:0}
.progress-track .progress-other{background:repeating-linear-gradient(135deg,var(--software-color) 0,var(--software-color) 3px,#ffffffc9 3px,#ffffffc9 6px)}
.progress-counts{font-size:13px;line-height:1.5;margin-top:6px;color:#505962;font-variant-numeric:tabular-nums}.progress-unknown{background:repeating-linear-gradient(135deg,#e9edef 0,#e9edef 5px,#fafaf8 5px,#fafaf8 10px)}
.coverage .progress-item{min-width:170px}.coverage .progress-track{height:7px}.coverage .progress-counts{font-size:12px}.coverage .progress-title{font-size:13px}
</style>
<h1>Spheroid timing progress</h1>
<div id="livebar" hidden><label>Platform <select id="platform"><option>ubuntu-latest</option><option>windows-latest</option><option>macos-latest</option></select></label><a id="run-link" target="_blank" rel="noopener">Browse benchmark runs</a><a id="dataset-link" target="_blank" rel="noopener">Consolidated dataset</a><a id="local-reference-link" target="_blank" rel="noopener">Local BEM / FEM-BEM results</a><span id="run-state"></span></div>
<p id="setup-status" class="note" hidden></p>
<p id="connection" role="status" hidden></p>
<p id="data-run-warning" role="status" hidden></p>
<p id="data-run" hidden>Displayed data: <a id="data-run-link" target="_blank" rel="noopener"></a><span id="data-run-detail"></span></p>
<p id="status"></p>
<p id="latest-prediction" class="note" hidden></p>
<p id="reference-status" class="note"></p>
<p><a id="csv-link" href="results.csv">Download CSV</a> · <a id="json-link" href="results.json">Full records</a> <span id="manifest-link">· <a href="manifest.json">Campaign manifest</a></span></p>
<p><label>Semimajor axis a (m) <select id="semimajor" aria-describedby="coverage-note"></select></label>
<label>Aspect ratio <select id="aspect"></select></label>
<label>Incidence <select id="incidence" aria-describedby="coverage-note"><option value="all">All angles</option></select></label>
<label>View <select id="layout"><option value="paired">Measure + ΔTS</option><option value="single">Single measure</option><option value="tradeoff">Cost vs |ΔTS|</option></select></label>
<label>Horizontal axis <select id="axis"><option value="frequency_hz">Frequency (kHz)</option><option value="h0">h₀</option><option value="k0a">k₀a</option></select></label>
<label>Measure <select id="metric"><option value="elapsed_seconds">Solve time (s, log scale)</option><option value="memory_delta_peak_bytes">Additional RAM per prediction (MiB)</option><option value="ts_db">TS (dB)</option><option value="delta_ts_db">ΔTS (dB, software minus reference)</option></select></label></p>
<section id="benchmark-progress" aria-labelledby="completion-title"><div class="completion-heading"><h2 id="completion-title">Benchmark completion</h2><label>Scope <select id="progress-scope"><option value="all">Entire benchmark</option><option value="selected">Selected aspect ratio and incidence</option></select></label></div><p id="progress-context" class="note"></p><div id="software-progress"></div><p class="note">Only boundaries supported by each software count toward completion. Solid color = successful, stripes = failed or skipped, gray = pending. Each planned repetition is one case. Percentages count recorded cases, not elapsed time.</p></section>
<div id="legend"></div><div id="angle-legend" aria-label="Incidence line and marker styles"></div><p id="plot-status" class="note" role="status"></p><div id="panels"></div>
<p class="note" id="failure-note">X = failed prediction, in the software's color, at its recorded time to failure. Each failed repetition is shown separately. Failures without a positive recorded prediction duration, including timeouts, remain in the coverage counts.</p>
<p class="note">Lines connect adjacent sampled frequencies only. Gaps include pending, failed and unsupported cases. Timings show medians of successful repetitions. A single repetition has no variability estimate. Timeout durations are not successful solve times. ΔTS uses references matched to the same physical inputs. Local references retain their manuscript spatial convergence checks. Independently qualified references also require Fourier checks. Convergence evidence remains in the downloadable records. <span id="refresh-note">This file refreshes every minute. Downloaded artifacts are snapshots.</span></p>
<p class="note" id="memory-note" hidden>Memory is the sampled peak additional resident RAM during one prediction, above its warmed-up baseline, including child processes. Setup and warmup are excluded. Reusing allocated memory can produce zero additional RAM. Short-lived peaks between samples may be missed. Values are medians across successful repetitions, not cumulative allocated bytes.</p>
<h2>Coverage and comparison error by software and boundary</h2><p class="note" id="coverage-note"></p><div class="coverage"><table><thead><tr><th>Software</th><th>Boundary</th><th>Completion</th><th>Success</th><th>Other recorded</th><th>Pending</th><th>Total solve time (s)</th><th>Average solve time (s)</th><th>RMSE ΔTS (dB)</th><th>Predictions with reference</th></tr></thead><tbody id="coverage"></tbody></table></div>
<p class="note">Total and average solve time include successful timed predictions only.</p>
<p class="note" id="rmse-note">RMSE uses the median ΔTS across successful repetitions of each prediction, then √mean(ΔTS²) across predictions with valid comparisons. Predictions with reference counts the frequency and incidence combinations that have a matching BEM/FEM-BEM reference. Repetitions count once. No matched reference means successful predictions exist but their exact reference cases are missing. No successful prediction means there is no completed successful result in this selection.</p>
<script id="data" type="application/json">PAYLOAD</script>
<script>
let data=JSON.parse(document.getElementById('data').textContent), rows=data.rows;
let externalReferenceCurves=[],localReferenceCurves=[],sharedReferenceCurves=[];
const repository=data.repository;
if(repository)document.getElementById('dataset-link').href=`https://github.com/${repository}/releases/tag/benchmark-dataset`;
if(repository)document.getElementById('local-reference-link').href=`https://github.com/${repository}/blob/benchmark-data/local-reference-records.json`;
const capabilities=data.capabilities;
function supportsBoundary(software,boundary){return !capabilities?.[software]||capabilities[software].includes(boundary);}
function applicable(row){return supportsBoundary(row.software,row.boundary);}
const referenceColor='#228B22';
const colors={acousticts:'#0072B2',prol_spheroid:'#D55E00',echosms:'#CC79A7',liquid_spheroid:'#6b5b00'};
const labels={acousticts:'acousticTS',prol_spheroid:'Prol_Spheroid',acousticscattering:'AcousticScattering.jl',echosms:'echoSMs',liquid_spheroid:'liquid_spheroid'};
const boundaryLabels={rigid:'Fixed rigid',pressure_release:'Pressure release',liquid:'Liquid-filled',gas:'Gas-filled'};
const semimajor=document.getElementById('semimajor'),aspect=document.getElementById('aspect'),incidence=document.getElementById('incidence'),metric=document.getElementById('metric'),axis=document.getElementById('axis'),layout=document.getElementById('layout');
const sizeOptions=[.001,.01,.07,.1,.5];
function referenceCurves(){
 const unique=new Map();
 for(const row of [...localReferenceCurves,...sharedReferenceCurves,...(data.reference_curves||[]),...externalReferenceCurves]){
  if(!row||typeof row.physical_id!=='string'||typeof row.reference_id!=='string'||!Number.isFinite(row.ts_db)||!Number.isFinite(row.semimajor_m)||row.semimajor_m<=0||!Number.isFinite(row.aspect_ratio)||!Number.isFinite(row.incidence_deg)||!Number.isFinite(row.frequency_hz))continue;
  unique.set(row.physical_id,row);
 }
 return [...unique.values()];
}
function matchedReferenceRows(input){
 const references=new Map(referenceCurves().map(reference=>[reference.physical_id,reference]));
 return input.map(row=>{
  if(row.reference_id&&Number.isFinite(row.reference_ts_db)&&Number.isFinite(row.delta_ts_db))return row;
  const reference=references.get(row.physical_id);
  if(!reference)return row;
  return {...row,reference_id:reference.reference_id,reference_ts_db:reference.ts_db,
   reference_uncertainty_db:reference.uncertainty_db,
   delta_ts_db:row.status==='ok'&&Number.isFinite(row.ts_db)?row.ts_db-reference.ts_db:null};
 });
}
let preferredSize='';
try{const saved=Number(localStorage.getItem('benchmark-semimajor'));if(Number.isFinite(saved)&&saved>0)preferredSize=String(saved);}catch(e){}
function snapshotSize(snapshot){
 if(Number.isFinite(snapshot.semimajor_m)&&snapshot.semimajor_m>0)return snapshot.semimajor_m;
 const values=[...new Set((snapshot.rows||[]).map(r=>r.semimajor_m).filter(a=>Number.isFinite(a)&&a>0))];
 return values.length===1?values[0]:null;
}
function updateSizes(){
 const known=[snapshotSize(data),...rows.map(r=>r.semimajor_m),...referenceCurves().map(r=>r.semimajor_m)].filter(a=>Number.isFinite(a)&&a>0);
 const values=[...new Set([...sizeOptions,...known,...(preferredSize?[Number(preferredSize)]:[])])].sort((a,b)=>a-b);
 semimajor.replaceChildren(...values.map(a=>new Option(String(a),String(a))));
 if(!preferredSize&&known.length)preferredSize=String(known[0]);
 semimajor.value=preferredSize||'0.07';
}
function matchesSize(row){return (Number.isFinite(row.semimajor_m)?row.semimajor_m:snapshotSize(data))===Number(semimajor.value);}
let availableIncidences=[],preferredIncidence='all',preferredAspect='';
try{preferredIncidence=localStorage.getItem('benchmark-incidence')||'all';preferredAspect=localStorage.getItem('benchmark-aspect')||'';}catch(e){}
function updateIncidences(){
 availableIncidences=[...new Set([...(data.incidences_deg||rows.filter(matchesSize).map(r=>r.incidence_deg)),...referenceCurves().filter(matchesSize).map(r=>r.incidence_deg)])].filter(Number.isFinite).sort((a,b)=>a-b);
 incidence.replaceChildren(new Option('All angles','all'));
 for(const angle of availableIncidences)incidence.add(new Option(`${angle}°`,String(angle)));
 incidence.disabled=!availableIncidences.length;
 incidence.value=[...incidence.options].some(o=>o.value===preferredIncidence)?preferredIncidence:'all';
}
function updateAspects(){
 const previous=preferredAspect||aspect.value;aspect.replaceChildren();
 const ratios=[...new Set([...(data.aspect_ratios||rows.filter(matchesSize).map(r=>r.aspect_ratio)),...referenceCurves().filter(matchesSize).map(r=>r.aspect_ratio)])].sort((a,b)=>a-b);
 aspect.disabled=!ratios.length;
 if(!ratios.length){aspect.add(new Option('Waiting for run configuration',''));return;}
 for(const ratio of ratios)aspect.add(new Option(ratio,ratio));
 if([...aspect.options].some(o=>o.value===previous))aspect.value=previous;
}
updateSizes();
updateAspects();
updateIncidences();
try {aspect.value=localStorage.getItem('benchmark-aspect')||aspect.value; if(!aspect.value)aspect.selectedIndex=0;metric.value=localStorage.getItem('benchmark-metric')||metric.value;axis.value=localStorage.getItem('benchmark-axis')||axis.value;layout.value=localStorage.getItem('benchmark-layout')||'paired';}catch(e){}
function updateStatus(){
 const s=data.status,active=benchmarkProgress().filter(summary=>!summary.retained),known=active.length&&active.every(summary=>summary.planned!==null);
 const observations=rows.filter(r=>matchesSize(r)&&applicable(r)&&r.measurement_role!=='retained'&&r.status!=='pending');
 const planned=known?active.reduce((sum,g)=>sum+g.planned,0):null,recorded=known?active.reduce((sum,g)=>sum+g.recorded,0):observations.length;
 const success=known?active.reduce((sum,g)=>sum+g.success,0):observations.filter(r=>r.status==='ok').length,pending=known?planned-recorded:null;
 const other=Math.max(0,(recorded||0)-success),finished=data.state==='completed'||s.state==='complete';
 const phase=finished?(pending?(s.state==='budget_paused'?'Paused with pending cases. Resume to continue':'Run stopped with pending cases'):other?'Run finished with failures or skips':'Run finished'):data.state==='setup'?'Waiting for first timing checkpoint':'Benchmarking';
 document.getElementById('status').textContent=planned?`${recorded} / ${planned} recorded · ${success} successful · ${other} failed or skipped · ${pending} pending · ${phase} · Data updated ${s.updated_at||data.published_at||'pending'}`:known?'No supported benchmark cases in this run.':recorded?`${recorded} recorded · ${success} successful · ${other} failed or skipped · Planned total unavailable`:'Waiting for the benchmark plan.';
 const latest=rows.filter(r=>applicable(r)&&r.measurement_role!=='retained'&&r.status!=='pending').at(-1),latestNote=document.getElementById('latest-prediction');
 latestNote.hidden=!latest;
 if(latest)latestNote.textContent=`Latest recorded prediction: ${labels[latest.software]||latest.software}, ${boundaryLabels[latest.boundary]||latest.boundary}, a = ${latest.semimajor_m??snapshotSize(data)} m, aspect ratio ${latest.aspect_ratio}, ${latest.incidence_deg}°, ${latest.frequency_hz/1000} kHz. Other selections update when their predictions run.`;
}
updateStatus();
const legend=document.getElementById('legend');
for(const key of Object.keys(colors)){const s=document.createElement('span');s.dataset.software=key;s.style.color=colors[key];s.textContent='● '+labels[key];legend.append(s);}
const referenceLegend=document.createElement('span');referenceLegend.id='reference-legend';referenceLegend.style.color=referenceColor;referenceLegend.textContent='━ BEM / FEM-BEM reference';referenceLegend.hidden=true;legend.append(referenceLegend);
const angles=document.getElementById('angle-legend');
function svgElement(name,attrs={},text){const e=document.createElementNS('http://www.w3.org/2000/svg',name);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
function median(values){values.sort((a,b)=>a-b);const n=values.length;return n%2?values[(n-1)/2]:(values[n/2-1]+values[n/2])/2;}
function comparisonSummary(observations){
 const predictions=new Map();
 for(const row of observations){
  if(!validValue(row,'delta_ts_db')||typeof row.reference_id!=='string'||!row.reference_id.length)continue;
  const key=row.physical_id||JSON.stringify([row.incidence_deg,row.frequency_hz]);
  if(!predictions.has(key))predictions.set(key,[]);predictions.get(key).push(row.delta_ts_db);
 }
 const differences=[...predictions.values()].map(median),count=differences.length;
 if(!count)return {count,rmse:null};
 const scale=differences.reduce((largest,value)=>Math.max(largest,Math.abs(value)),0);
 const rmse=scale?scale*Math.sqrt(differences.reduce((sum,value)=>sum+(value/scale)**2,0)/count):0;
 return {count,rmse};
}
const measures={elapsed_seconds:'Solve time (s, log scale)',memory_delta_peak_bytes:'Additional RAM per prediction (MiB)',ts_db:'TS (dB)',delta_ts_db:'ΔTS (dB, software minus reference)'};
function valueFor(row,field){return field==='memory_delta_peak_bytes'?row[field]/1048576:row[field];}
function validValue(row,field){return row.status==='ok'&&Number.isFinite(row[field])&&(field!=='elapsed_seconds'||row[field]>0)&&(field!=='memory_delta_peak_bytes'||row[field]>=0);}
function timedFailure(row){return ['numerical_error','nonconverged','worker_error'].includes(row.status)&&Number.isFinite(row.elapsed_seconds)&&row.elapsed_seconds>0;}
function formatted(value){return Math.abs(value)>0&&Math.abs(value)<.01?value.toExponential(2):value.toFixed(2);}
function progressGroups(byAngle){
 const supplied=byAngle?data.incidence_coverage:data.coverage;
 if(Array.isArray(supplied))return supplied;
 if(repository)return null;
 const groups=new Map();
 for(const row of rows.filter(matchesSize)){
  const key=JSON.stringify([row.software,row.aspect_ratio,row.boundary,...(byAngle?[row.incidence_deg]:[])]);
  if(!groups.has(key))groups.set(key,{software:row.software,aspect_ratio:row.aspect_ratio,boundary:row.boundary,incidence_deg:row.incidence_deg,planned:0,success:0,other_recorded:0});
  const group=groups.get(key);group.planned++;
  if(row.status==='ok')group.success++;else if(row.status!=='pending')group.other_recorded++;
 }
 return [...groups.values()];
}
function benchmarkProgress(selected=false,boundary=null){
 const byAngle=selected&&incidence.value!=='all',groups=progressGroups(byAngle);
 const retained=new Set(rows.filter(r=>matchesSize(r)&&r.measurement_role==='retained').map(r=>r.software));
 const programs=Object.keys(colors).filter(name=>groups?.some(g=>g.software===name)||data.coverage?.some(g=>g.software===name)||rows.some(r=>matchesSize(r)&&r.software===name));
 const inScope=g=>(!selected||g.aspect_ratio===Number(aspect.value)&&(!byAngle||g.incidence_deg===Number(incidence.value)))&&(!boundary||g.boundary===boundary);
 return programs.map(software=>{
  const supported=g=>supportsBoundary(software,g.boundary);
  const scoped=(groups||[]).filter(g=>g.software===software&&inScope(g)&&supported(g));
  const observed=rows.filter(r=>matchesSize(r)&&r.software===software&&inScope(r)&&supported(r)&&r.status!=='pending');
  const success=groups?scoped.reduce((sum,g)=>sum+g.success,0):observed.filter(r=>r.status==='ok').length;
  const other=groups?scoped.reduce((sum,g)=>sum+g.other_recorded,0):observed.filter(r=>r.status!=='ok').length;
  const recorded=success+other;
  const total=groups&&scoped.every(g=>Number.isFinite(g.planned)&&g.planned>=g.success+g.other_recorded)?scoped.reduce((sum,g)=>sum+g.planned,0):null;
  const planned=total!==null&&total>=recorded?total:null;
  return {software,retained:retained.has(software),success,other,recorded,planned,pending:planned===null?null:planned-recorded};
 }).filter(summary=>!boundary||supportsBoundary(summary.software,boundary));
}
function progressBar(summary,showName=true){
 const {software,success,other,recorded,planned,pending}=summary,node=document.createElement('div');node.className='progress-item';node.dataset.software=software;node.style.setProperty('--software-color',colors[software]);
 const title=document.createElement('div');title.className='progress-title';
 if(showName){const name=document.createElement('strong');name.textContent=labels[software];title.append(name);}
 const percent=planned===null?null:(recorded===planned?100:Math.min(99.99,100*recorded/planned));
 const number=document.createElement('span');number.textContent=percent===null?'Total unavailable':`${percent.toFixed(2)}%`;title.append(number);node.append(title);
 const track=document.createElement('div');track.className='progress-track';track.setAttribute('role','progressbar');track.setAttribute('aria-label',`${labels[software]} recorded cases`);track.setAttribute('aria-valuemin','0');
 const countText=planned===null?`${recorded.toLocaleString()} recorded · planned total unavailable`:`${recorded.toLocaleString()} / ${planned.toLocaleString()} recorded · ${pending.toLocaleString()} pending`;
 track.setAttribute('aria-valuetext',countText);
 if(planned!==null){track.setAttribute('aria-valuemax',String(planned));track.setAttribute('aria-valuenow',String(recorded));for(const [kind,count] of [['success',success],['other',other]]){const segment=document.createElement('span');segment.className=`progress-${kind}`;segment.style.width=`${100*count/planned}%`;track.append(segment);}}
 else track.classList.add('progress-unknown');
 node.append(track);const counts=document.createElement('div');counts.className='progress-counts';counts.textContent=countText;
 if(showName){const detail=document.createElement('div');detail.textContent=`${success.toLocaleString()} successful · ${other.toLocaleString()} failed or skipped`;counts.append(detail);}node.append(counts);return node;
}
function renderProgress(){
 const selected=document.getElementById('progress-scope').value==='selected',container=document.getElementById('software-progress'),all=benchmarkProgress(selected),summaries=all.filter(summary=>summary.planned!==0);
 container.replaceChildren(...summaries.map(summary=>progressBar(summary)));
 document.getElementById('progress-context').textContent=!summaries.length?(all.length?'No supported cases in this selection.':'Waiting for the benchmark plan.'):`${data.platform?data.platform+' · ':''}a = ${semimajor.value} m · ${selected?'aspect ratio '+aspect.value+' · '+(incidence.value==='all'?'all incidences':incidence.value+'° incidence'):'all aspect ratios and incidences'} · supported boundaries only`;
}
function extentFor(values,zero=false){let lo=values.length?values.reduce((a,b)=>Math.min(a,b)):0,hi=values.length?values.reduce((a,b)=>Math.max(a,b)):1;if(zero){lo=Math.min(lo,0);hi=Math.max(hi,0);}if(lo===hi){lo-=.5;hi+=.5;}const padding=(hi-lo)*.08;return [lo-padding,hi+padding];}
function angleStyle(angle){
 if(angle===0)return {dash:'',shape:'circle'};
 if(angle===37)return {dash:'9 5',shape:'triangle'};
 if(angle===90)return {dash:'2 5',shape:'square'};
 const index=availableIncidences.filter(a=>![0,37,90].includes(a)).indexOf(angle);
 return [{dash:'9 4 2 4',shape:'diamond'},{dash:'12 4',shape:'plus'},{dash:'3 3',shape:'hexagon'}][Math.max(0,index)%3];
}
function marker(x,y,angle,color,size=3.5){
 const shape=angleStyle(angle).shape,attrs={fill:color,stroke:color,'stroke-width':1};
 if(shape==='triangle')return svgElement('path',{...attrs,d:`M ${x} ${y-size-1} L ${x-size-1} ${y+size} L ${x+size+1} ${y+size} Z`});
 if(shape==='square')return svgElement('rect',{...attrs,x:x-size,y:y-size,width:2*size,height:2*size});
 if(shape==='diamond')return svgElement('path',{...attrs,d:`M ${x} ${y-size-1} L ${x+size+1} ${y} L ${x} ${y+size+1} L ${x-size-1} ${y} Z`});
 if(shape==='plus')return svgElement('path',{...attrs,fill:'none','stroke-width':2,d:`M ${x-size} ${y} L ${x+size} ${y} M ${x} ${y-size} L ${x} ${y+size}`});
 if(shape==='hexagon')return svgElement('path',{...attrs,d:`M ${x-size} ${y} L ${x-size/2} ${y-size} L ${x+size/2} ${y-size} L ${x+size} ${y} L ${x+size/2} ${y+size} L ${x-size/2} ${y+size} Z`});
 return svgElement('circle',{...attrs,cx:x,cy:y,r:size});
}
function renderAngles(selectedAngles){
 angles.replaceChildren();
 for(const angle of selectedAngles){
  const item=document.createElement('span'),style=angleStyle(angle),sample=svgElement('svg',{viewBox:'0 0 58 22',role:'img','aria-label':`${angle} degrees, ${style.shape} markers`});
  item.dataset.incidence=angle;
  sample.append(svgElement('line',{x1:3,x2:55,y1:11,y2:11,stroke:'#303942','stroke-width':2,'stroke-dasharray':style.dash}));
  sample.append(marker(29,11,angle,'#303942',4));item.append(sample,document.createTextNode(`${angle}°`));angles.append(item);
 }
}
function point(svg,x,y,angle,color,title,scatter=false){
 const node=marker(x,y,angle,color,scatter?3.7:3.1);node.dataset.incidence=angle;
 node.classList.add('observation');node.append(svgElement('title',{},title));svg.append(node);
}
function failurePoint(svg,x,y,row){
 const size=5,title=`${labels[row.software]}, ${row.incidence_deg}°, ${row.frequency_hz/1000} kHz: ${row.status.replaceAll('_',' ')}, time to failure ${formatted(row.elapsed_seconds)} s${Number.isFinite(row.repetition)?`, repetition ${row.repetition}`:''}`;
 const node=svgElement('path',{class:'failure-observation',fill:'none',stroke:colors[row.software],'stroke-width':2.3,'stroke-linecap':'round',role:'img','aria-label':title,d:`M ${x-size} ${y-size} L ${x+size} ${y+size} M ${x-size} ${y+size} L ${x+size} ${y-size}`});
 for(const key of ['software','status','repetition'])if(row[key]!==undefined)node.dataset[key]=row[key];
 node.dataset.incidence=row.incidence_deg;node.dataset.frequency=row.frequency_hz;node.dataset.elapsed=row.elapsed_seconds;
 node.append(svgElement('title',{},title));svg.append(node);
}
function chartFrame(parent,title,label,xlabel,xrange,yrange,xlog=false,ylog=false){
 const chart=document.createElement('div'),heading=document.createElement('h3');heading.textContent=title;chart.append(heading);parent.append(chart);
 const svg=svgElement('svg',{viewBox:'0 0 560 300',role:'img','aria-label':label});chart.append(svg);
 const x=v=>70+450*(v-xrange[0])/(xrange[1]-xrange[0]),y=v=>250-220*(v-yrange[0])/(yrange[1]-yrange[0]);
 for(let tick=0;tick<=4;tick++){
  const xx=70+450*tick/4,yy=250-220*tick/4,xv=xrange[0]+(xrange[1]-xrange[0])*tick/4,yv=yrange[0]+(yrange[1]-yrange[0])*tick/4;
  svg.append(svgElement('line',{x1:70,x2:520,y1:yy,y2:yy,stroke:'#e3e5e5'}));
  svg.append(svgElement('text',{x:xx,y:272,'text-anchor':'middle'},formatted(xlog?10**xv:xv)));
  svg.append(svgElement('text',{x:63,y:yy+4,'text-anchor':'end'},formatted(ylog?10**yv:yv)));
 }
 svg.append(svgElement('text',{x:295,y:295,'text-anchor':'middle'},xlabel));
 return {svg,x,y};
}
function benchmarkFrequencyWindow(selected,references,axes={}){
 const frequencies=[...new Set([...(axes.frequencies_hz||[]),...selected.map(r=>r.frequency_hz)])].filter(Number.isFinite).sort((a,b)=>a-b);
 const endpoints=[...frequencies,...(axes.frequency_hz||[])].filter(Number.isFinite);
 const bounds=endpoints.length?[Math.min(...endpoints),Math.max(...endpoints)]:null;
 return {frequencies,bounds,references:bounds?references.filter(r=>r.frequency_hz>=bounds[0]&&r.frequency_hz<=bounds[1]):[]};
}
function render(){
 rows=matchedReferenceRows(data.rows||rows);
 renderProgress();
 if(layout.value!=='single'&&!['elapsed_seconds','memory_delta_peak_bytes'].includes(metric.value))metric.value='elapsed_seconds';
 axis.disabled=layout.value==='tradeoff';
 try{localStorage.setItem('benchmark-metric',metric.value);localStorage.setItem('benchmark-axis',axis.value);localStorage.setItem('benchmark-layout',layout.value);}catch(e){}
 const matchesAngle=r=>incidence.value==='all'||r.incidence_deg===Number(incidence.value);
 const selected=rows.filter(r=>applicable(r)&&matchesSize(r)&&r.aspect_ratio===Number(aspect.value)&&matchesAngle(r)), field=metric.value;
 const benchmarkAxes=snapshotSize(data)===Number(semimajor.value)?(data.axes?.[aspect.value]||{}):{};
 const spectrum=benchmarkFrequencyWindow(selected,referenceCurves().filter(r=>matchesSize(r)&&r.aspect_ratio===Number(aspect.value)&&matchesAngle(r)),benchmarkAxes);
 const selectedReferences=spectrum.references,benchmarkFrequencies=new Set(spectrum.frequencies);
 const localCount=selectedReferences.filter(r=>['local_manuscript','local_qualified'].includes(r.origin)).length;
 const showReferences=layout.value==='single'&&field==='ts_db';
 referenceLegend.hidden=!showReferences||!selectedReferences.length;
 const coverageGroups=(incidence.value==='all'?data.coverage:data.incidence_coverage)?.filter(r=>applicable(r)&&r.aspect_ratio===Number(aspect.value)&&matchesAngle(r));
 const plannedGroups=coverageGroups||(data.coverage||[]).filter(r=>applicable(r)&&r.aspect_ratio===Number(aspect.value));
 const measured=selected.filter(r=>r.status==='ok'),compared=measured.filter(r=>Number.isFinite(r.delta_ts_db)),failures=selected.filter(timedFailure);
 const selectedSoftware=new Set([...selected.map(r=>r.software),...plannedGroups.map(r=>r.software)]);
 for(const item of legend.querySelectorAll('[data-software]'))item.hidden=!selectedSoftware.has(item.dataset.software);
 document.getElementById('memory-note').hidden=field!=='memory_delta_peak_bytes';
 document.getElementById('failure-note').hidden=field!=='elapsed_seconds'||layout.value==='tradeoff';
 document.getElementById('reference-status').textContent=`BEM / FEM-BEM references: ${selectedReferences.length} verified predictions within the benchmark frequency range (${localCount} from local runs). ${compared.length} of ${measured.length} successful measurements have a matched reference for ΔTS. Thin green reference curves appear within that range in the TS view, including pending benchmark cases. Stored references beyond the range become visible when the benchmark is extended. Saved convergence checks distinguish the manuscript references from independently qualified references. References are reused across platforms.`;
 const allFreq=[...new Set([...spectrum.frequencies,...selectedReferences.map(r=>r.frequency_hz)])].sort((a,b)=>a-b);
 const panels=document.getElementById('panels');panels.replaceChildren();panels.classList.toggle('paired',layout.value==='paired');
 const selectedAngles=incidence.value==='all'?availableIncidences:[Number(incidence.value)];
 renderAngles(selectedAngles);
 document.getElementById('coverage-note').textContent=aspect.value?`Every plot and coverage row uses a = ${semimajor.value} m (total length 2a = ${Number(semimajor.value)*2} m), aspect ratio ${aspect.value} and ${incidence.value==='all'?'all incidences':incidence.value+'° incidence'}. Only boundaries supported by each software are included.`:'Size and incidence selections apply to every plot and coverage row once results are available.';
 if(repository&&incidence.value!=='all'&&!data.incidence_coverage)document.getElementById('coverage-note').textContent+=' This snapshot does not report pending counts by incidence.';
 const plotStatus=document.getElementById('plot-status');
 plotStatus.textContent=!allFreq.length?`No benchmark frequency range is available for this selection yet.`:!rows.length?'The run configuration is available. Timing points will appear after the first benchmark checkpoint.':!selected.some(r=>r.status!=='pending')?'No cases have been recorded for the selected size, incidence and aspect ratio yet.':(layout.value!=='single'||field==='delta_ts_db')&&!compared.length?'No matching references have been attached yet. The reference jobs save results independently, then update comparisons without repeating the timings.':layout.value==='tradeoff'?'Each point is one frequency and incidence. Cost and absolute ΔTS use medians across matched successful repetitions.':'';
 if(!measured.length&&failures.length)plotStatus.textContent='Only failed predictions have been recorded for this selection. Recorded times to failure appear as X markers in the solve-time view. Failed predictions have no valid ΔTS.';
 if(showReferences&&selectedReferences.length&&!measured.length)plotStatus.textContent='Qualified BEM / FEM-BEM reference predictions are shown in green. Software predictions are pending or unavailable for this selection.';
 if(field==='memory_delta_peak_bytes'&&!measured.some(r=>validValue(r,field)))plotStatus.textContent+=' No per-prediction memory measurements are available in this snapshot.';
 if(!allFreq.length){document.getElementById('coverage').replaceChildren();return;}
 const axisValues=new Map([...selectedReferences,...selected].map(r=>[r.frequency_hz,axis.value==='frequency_hz'?r.frequency_hz/1000:r[axis.value]]));
 const coordinates=selected.map(r=>axis.value==='frequency_hz'?r.frequency_hz/1000:r[axis.value]).filter(Number.isFinite);
 const extent=axis.value==='frequency_hz'?spectrum.bounds:benchmarkAxes[axis.value];
 if(extent)coordinates.push(...extent.map(value=>axis.value==='frequency_hz'?value/1000:value));
 let low=coordinates.length?Math.min(...coordinates):0,high=coordinates.length?Math.max(...coordinates):1;
 if(low===high){const margin=Math.abs(low)*.1||.5;low-=margin;high+=margin;}
 const grouped=new Map();
 for(const row of measured){const key=JSON.stringify([row.boundary,row.software,row.incidence_deg,row.frequency_hz]);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(row);}
 const visibleBoundaries=new Set([...selected.map(r=>r.boundary),...plannedGroups.map(r=>r.boundary),...(showReferences?selectedReferences.map(r=>r.boundary):[])]);
 for(const boundary of Object.keys(boundaryLabels).filter(b=>visibleBoundaries.has(b))){
  const section=document.createElement('section'),title=document.createElement('h2');title.textContent=boundaryLabels[boundary];section.append(title);
  if(showReferences){
   const available=selectedReferences.filter(r=>r.boundary===boundary),qualified=new Set(available.filter(r=>benchmarkFrequencies.has(r.frequency_hz)).map(r=>`${r.incidence_deg}:${r.frequency_hz}`)).size,planned=spectrum.frequencies.length*selectedAngles.length;
   const note=document.createElement('p');note.className='note reference-coverage';note.dataset.boundary=boundary;
   note.textContent=`Matching benchmark references: ${qualified} / ${planned} predictions · ${available.length?'highest displayed reference frequency '+formatted(Math.max(...available.map(r=>r.frequency_hz))/1000)+' kHz':'no verified frequencies yet'} · ${Math.max(0,planned-qualified)} pending or unavailable. Missing frequencies remain gaps.`;
   section.append(note);
  }
  const charts=document.createElement('div');charts.className='charts';section.append(charts);panels.append(section);
  if(layout.value==='tradeoff'){
   const matched=measured.filter(r=>validValue(r,field)&&validValue(r,'delta_ts_db'));
   const transform=v=>field==='elapsed_seconds'?Math.log10(v):v;
   const xr=extentFor(matched.map(r=>transform(valueFor(r,field)))),yr=extentFor(matched.map(r=>Math.abs(r.delta_ts_db)),true);yr[0]=0;
   const {svg,x,y}=chartFrame(charts,'Absolute ΔTS (dB)',`${title.textContent}: cost versus absolute ΔTS`,measures[field],xr,yr,field==='elapsed_seconds');
   let count=0;
   for(const observations of grouped.values()){
    const pairs=observations.filter(r=>r.boundary===boundary&&validValue(r,field)&&validValue(r,'delta_ts_db'));if(!pairs.length)continue;
    const row=pairs[0],cost=median(pairs.map(r=>valueFor(r,field))),error=median(pairs.map(r=>Math.abs(r.delta_ts_db)));
    point(svg,x(transform(cost)),y(error),row.incidence_deg,colors[row.software],`${labels[row.software]}, ${row.incidence_deg}°, ${row.frequency_hz/1000} kHz: ${formatted(cost)} ${field==='elapsed_seconds'?'s':'MiB'}, |ΔTS| ${formatted(error)} dB, n=${pairs.length}`,true);count++;
   }
   const failedOnly=failures.some(r=>r.boundary===boundary)&&!measured.some(r=>r.boundary===boundary);
   if(!count)svg.append(svgElement('text',{x:295,y:140,'text-anchor':'middle',class:'empty'},failedOnly?'Failed predictions have no valid ΔTS':'Waiting for matched cost and reference data'));
   continue;
  }
  for(const measure of layout.value==='paired'?[field,'delta_ts_db']:[field]){
   const transform=v=>measure==='elapsed_seconds'?Math.log10(v):v;
   const valid=measured.filter(r=>validValue(r,measure)),timedFailures=measure==='elapsed_seconds'?failures:[];
   const curveRows=measure==='ts_db'?selectedReferences:[];
   const yr=extentFor([...valid,...timedFailures,...curveRows].map(r=>transform(valueFor(r,measure))),measure==='delta_ts_db');
   const {svg,x,y}=chartFrame(charts,measures[measure],`${title.textContent}: ${measures[measure]} against ${axis.options[axis.selectedIndex].text}`,axis.options[axis.selectedIndex].text,[low,high],yr,false,measure==='elapsed_seconds');
   if(measure==='delta_ts_db')svg.append(svgElement('line',{x1:70,x2:520,y1:y(0),y2:y(0),stroke:'#89949a','stroke-dasharray':'4 4'}));
   let count=0;
   const referenceLayer=svgElement('g',{class:'reference-layer'});
   if(curveRows.length)for(const angle of selectedAngles){
    const byFrequency=new Map(curveRows.filter(r=>r.boundary===boundary&&r.incidence_deg===angle).map(r=>[r.frequency_hz,r]));
    let previous=null;
    for(let index=0;index<allFreq.length;index++){
     const frequency=allFreq[index],row=byFrequency.get(frequency),coordinate=axisValues.get(frequency);
     if(!row||!Number.isFinite(coordinate)){previous=null;continue;}
     const xx=x(coordinate),yy=y(row.ts_db),title=`${row.method==='bem'?'BEM':'FEM-BEM'} reference, ${angle}°, ${frequency/1000} kHz: ${formatted(row.ts_db)} dB`;
     if(previous){const segment=svgElement('path',{class:'reference-segment','data-incidence':angle,'data-boundary':boundary,'data-start-frequency':previous.frequency,'data-end-frequency':frequency,d:`M ${previous.x} ${previous.y} L ${xx} ${yy}`,fill:'none',stroke:referenceColor,'stroke-width':1,'stroke-dasharray':angleStyle(angle).dash});segment.append(svgElement('title',{},title));referenceLayer.append(segment);}
     if(!previous&&!byFrequency.has(allFreq[index+1])){const isolated=svgElement('circle',{class:'reference-point','data-incidence':angle,'data-boundary':boundary,'data-frequency':frequency,cx:xx,cy:yy,r:1.6,fill:referenceColor});isolated.append(svgElement('title',{},title));referenceLayer.append(isolated);}
     previous={x:xx,y:yy,frequency};count++;
    }
   }
   for(const software of Object.keys(colors))for(const angle of selectedAngles){
    let path='';
    const softwareFreq=data.axes?.[aspect.value]?.software_frequencies_hz?.[software]||allFreq;
    for(const frequency of softwareFreq){
     const observations=(grouped.get(JSON.stringify([boundary,software,angle,frequency]))||[]).filter(r=>validValue(r,measure));
     if(!observations.length){path='';continue;}
     const value=median(observations.map(r=>valueFor(r,measure))),xx=x(axisValues.get(frequency)),yy=y(transform(value));
     if(path)svg.append(svgElement('path',{class:'series-segment','data-incidence':angle,d:`M ${path} L ${xx} ${yy}`,fill:'none',stroke:colors[software],'stroke-width':1.7,'stroke-dasharray':angleStyle(angle).dash}));
     path=`${xx} ${yy}`;point(svg,xx,yy,angle,colors[software],`${labels[software]}, ${angle}°, ${frequency/1000} kHz: ${formatted(value)}, n=${observations.length}`);count++;
    }
   }
   for(const row of timedFailures.filter(r=>r.boundary===boundary)){
    failurePoint(svg,x(axisValues.get(row.frequency_hz)),y(transform(row.elapsed_seconds)),row);count++;
   }
   if(referenceLayer.childElementCount)svg.append(referenceLayer);
   const failedOnly=failures.some(r=>r.boundary===boundary)&&!measured.some(r=>r.boundary===boundary);
   if(!count)svg.append(svgElement('text',{x:295,y:140,'text-anchor':'middle',class:'empty'},measure==='delta_ts_db'?(failedOnly?'Failed predictions have no valid ΔTS':'Waiting for matched reference comparisons'):'Waiting for measurements'));
  }
 }
 const table=document.getElementById('coverage');table.replaceChildren();
 const completions=Object.fromEntries(Object.keys(boundaryLabels).map(boundary=>[boundary,benchmarkProgress(true,boundary)]));
 for(const software of Object.keys(colors))for(const boundary of Object.keys(boundaryLabels)){
  if(!supportsBoundary(software,boundary))continue;
  const completion=completions[boundary].find(summary=>summary.software===software);
  const coverage=coverageGroups?.find(c=>c.software===software&&c.boundary===boundary);
  const planned=plannedGroups.some(c=>c.software===software&&c.boundary===boundary);
  const group=selected.filter(r=>r.software===software&&r.boundary===boundary);if(!planned&&!group.length&&!completion?.planned)continue;
  const good=group.filter(r=>r.status==='ok'),pending=group.filter(r=>r.status==='pending');
  const timed=good.filter(r=>Number.isFinite(r.elapsed_seconds)&&r.elapsed_seconds>=0);
  const seconds=timed.reduce((sum,r)=>sum+r.elapsed_seconds,0);
  const total=coverage?.elapsed_seconds_sum??seconds,shownTotal=total>0&&total<.01?total.toExponential(2):total.toFixed(2);
  const average=timed.length?seconds/timed.length:null;
  const shownAverage=average===null?'Unavailable':average>0&&average<.01?average.toExponential(2):average.toFixed(2);
  const successful=coverage?.success??good.length,other=coverage?.other_recorded??(group.length-good.length-pending.length);
  const remaining=completion?.pending??'Unknown';
  const comparison=comparisonSummary(group),shownRMSE=comparison.count?comparison.rmse.toFixed(2):(successful?'No matched reference':'No successful prediction');
  const tr=document.createElement('tr');tr.dataset.software=software;tr.dataset.boundary=boundary;
  const softwareLabel=labels[software];
  for(const value of [softwareLabel,boundaryLabels[boundary]]){const td=document.createElement('td');td.textContent=value;tr.append(td);}
  const progress=document.createElement('td');if(completion)progress.append(progressBar(completion,false));else progress.textContent='Unavailable';tr.append(progress);
  for(const value of [successful,other,remaining,shownTotal,shownAverage,shownRMSE,comparison.count]){const td=document.createElement('td');td.textContent=value;tr.append(td);}table.append(tr);
 }
}
aspect.addEventListener('change',()=>{preferredAspect=aspect.value;try{localStorage.setItem('benchmark-aspect',preferredAspect);}catch(e){}render();});incidence.addEventListener('change',()=>{preferredIncidence=incidence.value;try{localStorage.setItem('benchmark-incidence',preferredIncidence);}catch(e){}render();});metric.addEventListener('change',()=>{if(!['elapsed_seconds','memory_delta_peak_bytes'].includes(metric.value))layout.value='single';render();});axis.addEventListener('change',render);layout.addEventListener('change',render);render();
document.getElementById('progress-scope').addEventListener('change',renderProgress);
semimajor.addEventListener('change',()=>{preferredSize=semimajor.value;try{localStorage.setItem('benchmark-semimajor',preferredSize);}catch(e){}if(!repository){updateAspects();updateIncidences();render();}});
if(repository){
 document.querySelector('meta[http-equiv="refresh"]').remove();
 document.getElementById('livebar').hidden=false;document.getElementById('connection').hidden=false;
 document.getElementById('manifest-link').hidden=true;
 document.getElementById('refresh-note').textContent='This dashboard checks for updates every minute. New runs publish between cases once per minute. Long predictions and GitHub caching can delay updates. Full CSV and JSON are available in the run artifacts.';
 const platform=document.getElementById('platform'),connection=document.getElementById('connection'),runLink=document.getElementById('run-link');
 const runState=document.getElementById('run-state');
 const workflowURL=`https://github.com/${repository}/actions/workflows/benchmark.yml`;
 runLink.href=workflowURL;document.getElementById('csv-link').href=workflowURL;
 document.getElementById('csv-link').textContent='Full data and artifacts';document.getElementById('json-link').hidden=true;
 try{platform.value=localStorage.getItem('benchmark-platform')||'ubuntu-latest';}catch(e){}
 if(!platform.value)platform.value='ubuntu-latest';
 let newestRun=null,newestSetup=null,lastRunCheck=0,lastContentsCheck=0,busy=false,revision='',generation=0;
 let lastReferenceCheck=0,referenceRun=null,lastLocalReferenceCheck=0;
 let legacySizes=null,lastLegacyCheck=0;
 function runOrder(a,b){return (Number(a.run_id??a.id)-Number(b.run_id??b.id))||(Number(a.run_attempt||1)-Number(b.run_attempt||1));}
 function showRun(){
  const current=newestRun&&(!data.run_id||runOrder(newestRun,data)>=0)?newestRun:null;
  runLink.href=current?.html_url||workflowURL;
  runLink.textContent=current?`Newest benchmark run #${current.run_number}`:'Browse benchmark runs';
  const awaiting=current?.status==='completed'&&data.run_id&&runOrder(current,data)===0&&data.state!=='completed';
  runState.textContent=awaiting?'Newest workflow finished · waiting for final data':current?`Newest workflow ${current.status.replaceAll('_',' ')}${current.conclusion?' · '+current.conclusion:''}`:'Newest run status unavailable';
  const dataRun=document.getElementById('data-run'),dataLink=document.getElementById('data-run-link');
  dataRun.hidden=!data.run_id;
  if(data.run_id){
   dataLink.href=data.run_url||`https://github.com/${repository}/actions/runs/${data.run_id}`;
   dataLink.textContent=`Run ${data.run_id}`;
   document.getElementById('data-run-detail').textContent=` · attempt ${data.run_attempt||1} · ${data.platform||platform.value} · a = ${semimajor.value} m. Progress counts above cover the active software. Each software has its own completion bar below.`;
  }
  const warning=document.getElementById('data-run-warning'),old=data.run_id&&current&&runOrder(current,data)>0;
  warning.hidden=!old;
  warning.textContent=old?`${current.id===data.run_id?'Previous-attempt':'Previous-run'} results for ${platform.value} at a = ${semimajor.value} m. These counts, failures and plots belong to run ${data.run_id}, attempt ${data.run_attempt||1}. A newer run may use a different size or platform. Selected platforms may also be queued or awaiting their first checkpoint.`:'';
  const setup=document.getElementById('setup-status');setup.hidden=!newestSetup;setup.replaceChildren();
  if(newestSetup){const link=document.createElement('a');link.href=newestSetup.html_url;link.target='_blank';link.rel='noopener';link.textContent=`Run #${newestSetup.run_number}`;setup.append('Latest setup check: ',link,` · ${newestSetup.status.replaceAll('_',' ')}${newestSetup.conclusion?' · '+newestSetup.conclusion:''}. Setup checks do not produce timing points.`);}
 }
 async function fetchJSON(url,headers={}){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
  try{const response=await fetch(url,{headers,cache:'no-store',signal:controller.signal});return {status:response.status,ok:response.ok,value:response.ok?await response.json():null};}
  finally{clearTimeout(timer);}
 }
 async function decodeSnapshot(value){
  if(value?.schema_version!==2)return value;
  const limit=64*1024*1024;
  if(value.encoding!=='gzip-base64'||typeof value.data!=='string'||value.data.length>Math.ceil(limit/3)*4||
     !Number.isInteger(value.decoded_bytes)||value.decoded_bytes<1||value.decoded_bytes>limit)throw new Error('Invalid compressed progress data');
  const binary=atob(value.data),bytes=Uint8Array.from(binary,c=>c.charCodeAt(0));
  const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();
  const chunks=[];let size=0;
  try{while(true){const chunk=await reader.read();if(chunk.done)break;size+=chunk.value.length;
    if(size>value.decoded_bytes)throw new Error('Progress data exceeds its declared size');chunks.push(chunk.value);}}
  finally{await reader.cancel();reader.releaseLock();}
  if(size!==value.decoded_bytes)throw new Error('Incomplete compressed progress data');
  const decoded=JSON.parse(await new Blob(chunks).text());
  for(const key of ['run_id','run_attempt','platform','semimajor_m','published_at','state']){
   if(decoded[key]!==value[key])throw new Error('Progress data identity mismatch');
  }
  if(JSON.stringify(decoded.status)!==JSON.stringify(value.status))throw new Error('Progress status mismatch');
  return decoded;
 }
 function clearReferenceOverlay(){externalReferenceCurves=[];lastReferenceCheck=0;referenceRun=null;}
 async function pollLocalReferences(){
  if(Date.now()-lastLocalReferenceCheck<120000)return;
  lastLocalReferenceCheck=Date.now();
  try{
   const shared=await fetchJSON(`https://raw.githubusercontent.com/${repository}/benchmark-data/qualified-reference-curves.json?t=${Date.now()}`);
   if(shared.ok&&shared.value?.schema_version===1&&Array.isArray(shared.value.reference_curves))sharedReferenceCurves=shared.value.reference_curves.map(row=>({...row,origin:'local_qualified'}));
  }catch(e){}
  try{
   const response=await fetchJSON(`https://raw.githubusercontent.com/${repository}/benchmark-data/local-reference-curves.json?t=${Date.now()}`);
   if(response.ok&&response.value?.schema_version===1&&Array.isArray(response.value.reference_curves))localReferenceCurves=response.value.reference_curves;
  }catch(e){}
 }
 async function pollReferences(requestGeneration){
  const runId=data.run_id;if(!runId)return;
  if(referenceRun!==String(runId)){clearReferenceOverlay();referenceRun=String(runId);}
  if(Date.now()-lastReferenceCheck<120000)return;
  lastReferenceCheck=Date.now();
  try{
   const response=await fetchJSON(`https://raw.githubusercontent.com/${repository}/benchmark-data/reference-curves-${runId}.json?t=${Date.now()}`);
   if(requestGeneration!==generation||String(data.run_id)!==String(runId))return;
   const overlay=response.value;
   if(!response.ok||overlay?.schema_version!==1||String(overlay.run_id)!==String(runId)||!Array.isArray(overlay.reference_curves))return;
   externalReferenceCurves=overlay.reference_curves;updateSizes();updateAspects();updateIncidences();render();
  }catch(e){}
 }
 async function poll(){
  if(busy)return;busy=true;const chosen=platform.value,requestGeneration=generation;
  try{
   await pollLocalReferences();
   if(Date.now()-lastRunCheck>300000){
    lastRunCheck=Date.now();
    try{const response=await fetchJSON(`https://api.github.com/repos/${repository}/actions/workflows/benchmark.yml/runs?event=workflow_dispatch&per_page=30`);if(response.ok){const runs=response.value.workflow_runs||[];newestRun=runs.find(run=>!run.display_title?.startsWith('Prepare environments'))||null;newestSetup=runs.find(run=>run.display_title?.startsWith('Prepare environments'))||null;}}catch(e){}
   }
   const feed=`https://raw.githubusercontent.com/${repository}/benchmark-data/`;
   let publicFeed=false;
   async function snapshot(filename,allowREST){
    let response;
    // At most one periodic REST request per poll cycle, below the anonymous hourly limit.
    if(allowREST&&Date.now()-lastContentsCheck>=150000){
     lastContentsCheck=Date.now();
     try{response=await fetchJSON(`https://api.github.com/repos/${repository}/contents/${filename}?ref=benchmark-data&t=${Date.now()}`,{Accept:'application/vnd.github.raw+json'});}catch(e){}
    }
    if(!response||(!response.ok&&response.status!==404)){publicFeed=true;response=await fetchJSON(feed+filename+'?t='+Date.now());}
    if(response.ok){
     const value=response.value=await decodeSnapshot(response.value);
     if(value.schema_version!==1||!Array.isArray(value.rows)||value.platform!==chosen)throw new Error('Unexpected progress data');
     const sizes=[value.semimajor_m,...value.rows.map(r=>r.semimajor_m)].filter(a=>a!==undefined&&a!==null);
     if(sizes.some(a=>!Number.isFinite(a)||a<=0)||new Set(sizes).size>1)throw new Error('Progress data contains inconsistent semimajor axes');
    }
    return response;
   }
   // Size snapshots are published first. The platform alias may lag if a
   // publisher reaches its deadline before updating the second file.
   let filename=preferredSize?`${chosen}-a-${Number(preferredSize)}.json`:`${chosen}.json`,response=await snapshot(filename,true);
   if(preferredSize&&response.status===404){filename=`${chosen}.json`;response=await snapshot(filename,false);}
   if(requestGeneration!==generation)return;
   // Source-pinned older runs omit a. A one-time artifact import records their exact size.
   if(response.ok&&snapshotSize(response.value)===null){
    if(legacySizes===null||Date.now()-lastLegacyCheck>300000){
     lastLegacyCheck=Date.now();
     try{const legacy=await fetchJSON(feed+'legacy-run-sizes.json?t='+Date.now());if(legacy.ok&&legacy.value.schema_version===1)legacySizes=legacy.value.runs||{};}catch(e){}
     legacySizes=legacySizes||{};
    }
    const size=legacySizes[String(response.value.run_id)];
    if(Number.isFinite(size)&&size>0)response.value.semimajor_m=size;
   }
   if(requestGeneration!==generation)return;
   const latestSize=response.ok?snapshotSize(response.value):null;
   if(!preferredSize&&latestSize!==null){preferredSize=String(latestSize);updateSizes();semimajor.value=preferredSize;}
   if(preferredSize&&filename!==`${chosen}-a-${Number(preferredSize)}.json`){
    const sizedFilename=`${chosen}-a-${Number(preferredSize)}.json`,sized=await snapshot(sizedFilename,false);
    if(sized.ok||latestSize!==Number(preferredSize)){filename=sizedFilename;response=sized;}
   }
   if(requestGeneration!==generation)return;
   if(response.status===404){connection.textContent=`No timing results have been published for ${chosen} at a = ${semimajor.value} m yet. Saved references will appear when a benchmark frequency range is available.`;updateSizes();updateAspects();updateIncidences();render();showRun();return;}
   if(!response.ok)throw new Error(`Progress request returned ${response.status}`);
   const incoming=response.value;
   if(snapshotSize(incoming)!==Number(semimajor.value)){connection.textContent='The size of this older snapshot has not been recovered from its artifact yet.';showRun();return;}
   const url=feed+filename;
   const nextRevision=`${incoming.run_id}:${incoming.run_attempt}:${incoming.published_at}`;
   const order=data.run_id?runOrder(incoming,data):1;
   if(nextRevision!==revision&&(order>0||(order===0&&incoming.published_at>=data.published_at))){if(String(incoming.run_id)!==String(data.run_id))clearReferenceOverlay();data=incoming;rows=data.rows;revision=nextRevision;updateSizes();updateAspects();updateIncidences();updateStatus();render();}
   await pollReferences(requestGeneration);
   if(requestGeneration!==generation)return;
   connection.textContent=rows.length?`Connected · ${chosen} · a = ${semimajor.value} m · Published ${data.published_at}`:`Connected · ${chosen} · a = ${semimajor.value} m · ${data.state==='completed'?'The displayed data run finished without a benchmark checkpoint. Open its data run link for details.':'Setup or the first benchmark chunk is still running. Measured points will appear here automatically.'}`;
   if(publicFeed)connection.textContent+=' Using the public feed, which GitHub may cache for several minutes.';
   document.getElementById('csv-link').href=data.artifacts_url||((data.run_url||runLink.href)+'#artifacts');
   const jsonLink=document.getElementById('json-link');jsonLink.hidden=false;jsonLink.href=url;jsonLink.textContent='Progress JSON';
   showRun();
  }catch(error){if(requestGeneration===generation){connection.textContent=`Unable to refresh: ${error.message}. Retrying in one minute. Existing points stay visible.`;showRun();}}
  finally{busy=false;if(requestGeneration!==generation)poll();}
 }
 function changeSelection(){generation++;clearReferenceOverlay();data={rows:[],status:{},repository};rows=[];revision='';updateAspects();updateIncidences();updateStatus();render();showRun();document.getElementById('csv-link').href=workflowURL;document.getElementById('json-link').hidden=true;connection.textContent='Connecting…';poll();}
 platform.addEventListener('change',()=>{try{localStorage.setItem('benchmark-platform',platform.value);}catch(e){}changeSelection();});
 semimajor.addEventListener('change',changeSelection);
 connection.textContent='Connecting to benchmark progress…';poll();setInterval(poll,60000);
}
</script></html>'''.replace("PAYLOAD", payload)
    path = Path(path)
    temporary = path.with_suffix(".html.tmp")
    temporary.write_text(document, encoding="utf-8")
    os.replace(temporary, path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build the GitHub Pages progress dashboard")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    render_report(output, [], dict(state="waiting", recorded=0, planned=0), args.repository)

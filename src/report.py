"""Render a self-contained progress view without work during solver timing."""

import json
import os
from pathlib import Path


def render_report(path, rows, status, repository=None):
    payload = json.dumps({"rows": rows, "status": status, "repository": repository}, allow_nan=False).replace("<", "\\u003c")
    document = '''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<meta http-equiv="refresh" content="30"><title>Spheroid timing progress</title>
<style>
body{font:16px system-ui,sans-serif;color:#20252b;background:#fafaf8;margin:28px auto;max-width:1300px;padding:0 20px}
h1{font-size:25px}label{margin-right:24px}select{font:inherit;padding:5px}a{color:#005b88}
#panels{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(430px,100%),1fr));gap:16px}
section{background:white;border:1px solid #d6dadb;padding:14px}h2{font-size:17px;margin:0}
svg{width:100%;height:auto}text{font:12px system-ui}table{border-collapse:collapse;margin-top:20px;width:100%;font-size:14px}
th,td{text-align:left;padding:7px;border-bottom:1px solid #ddd}#legend span{display:inline-block;margin:10px 16px 12px 0}
.coverage{overflow-x:auto}label{display:inline-block;margin-bottom:10px}select{max-width:100%}
.note{color:#505962;font-size:14px;line-height:1.5}
#connection{padding:12px 16px;background:#eaf1f4;border-left:3px solid #0072b2;line-height:1.5}
[hidden]{display:none!important}#livebar{display:flex;align-items:baseline;gap:20px;flex-wrap:wrap}
</style>
<h1>Spheroid timing progress</h1>
<div id="livebar" hidden><label>Platform <select id="platform"><option>ubuntu-latest</option><option>windows-latest</option><option>macos-latest</option></select></label><a id="run-link" target="_blank" rel="noopener">Open benchmark run</a><span id="run-state"></span></div>
<p id="setup-status" class="note" hidden></p>
<p id="connection" role="status" hidden></p><p id="status"></p>
<p><a id="csv-link" href="results.csv">Download CSV</a> · <a id="json-link" href="results.json">Full records</a> <span id="manifest-link">· <a href="manifest.json">Campaign manifest</a></span></p>
<p><label>Aspect ratio <select id="aspect"></select></label>
<label>Horizontal axis <select id="axis"><option value="frequency_hz">Frequency (kHz)</option><option value="h0">h₀</option><option value="k0a">k₀a</option></select></label>
<label>Measure <select id="metric"><option value="elapsed_seconds">Solve time (s, log scale)</option><option value="ts_db">TS (dB)</option><option value="delta_ts_db">ΔTS (dB, software minus reference)</option></select></label></p>
<div id="legend"></div><p id="plot-status" class="note" role="status"></p><div id="panels"></div>
<p class="note">Lines connect adjacent sampled frequencies only. Gaps include pending, failed and unsupported cases. Timings show medians of successful repetitions. A single repetition has no variability estimate. Timeout durations are not successful solve times. ΔTS is available only for explicitly qualified, matched references. Reference uncertainty remains in the downloadable records. <span id="refresh-note">This file refreshes every 30 seconds. Downloaded artifacts are snapshots.</span></p>
<h2>Coverage by software and boundary</h2><div class="coverage"><table><thead><tr><th>Software</th><th>Boundary</th><th>Success</th><th>Other recorded</th><th>Pending</th><th>Sum of measured solve times (s)</th></tr></thead><tbody id="coverage"></tbody></table></div>
<script id="data" type="application/json">PAYLOAD</script>
<script>
let data=JSON.parse(document.getElementById('data').textContent), rows=data.rows;
const repository=data.repository;
const colors={acousticts:'#0072B2',prol_spheroid:'#D55E00',acousticscattering:'#009E73',echosms:'#CC79A7',liquid_spheroid:'#6b5b00'};
const labels={acousticts:'acousticTS',prol_spheroid:'Prol_Spheroid',acousticscattering:'AcousticScattering.jl',echosms:'echoSMs',liquid_spheroid:'liquid_spheroid'};
const boundaryLabels={rigid:'Fixed rigid',pressure_release:'Pressure release',liquid:'Liquid-filled',gas:'Gas-filled'};
const aspect=document.getElementById('aspect'),metric=document.getElementById('metric'),axis=document.getElementById('axis');
function updateAspects(){
 const previous=aspect.value;aspect.replaceChildren();
 const ratios=data.aspect_ratios||[...new Set(rows.map(r=>r.aspect_ratio))];
 aspect.disabled=!ratios.length;
 if(!ratios.length){aspect.add(new Option('Waiting for run configuration',''));return;}
 for(const ratio of ratios)aspect.add(new Option(ratio,ratio));
 if([...aspect.options].some(o=>o.value===previous))aspect.value=previous;
}
updateAspects();
try {aspect.value=localStorage.getItem('benchmark-aspect')||aspect.value; if(!aspect.value)aspect.selectedIndex=0;metric.value=localStorage.getItem('benchmark-metric')||metric.value;axis.value=localStorage.getItem('benchmark-axis')||axis.value;}catch(e){}
function updateStatus(){const s=data.status;document.getElementById('status').textContent=s.planned?`${s.recorded} / ${s.planned} recorded · ${s.state||data.state||'running'} · Checkpoint ${s.updated_at||'pending'}`:'Waiting for the first benchmark checkpoint.';}
updateStatus();
const legend=document.getElementById('legend');
for(const key of Object.keys(colors)){const s=document.createElement('span');s.style.color=colors[key];s.textContent='● '+labels[key];legend.append(s);}
const angles=document.createElement('span');angles.textContent='0° solid · 37° dashed · 90° dotted';legend.append(angles);
function svgElement(name,attrs={},text){const e=document.createElementNS('http://www.w3.org/2000/svg',name);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
function median(values){values.sort((a,b)=>a-b);const n=values.length;return n%2?values[(n-1)/2]:(values[n/2-1]+values[n/2])/2;}
function render(){
 try{localStorage.setItem('benchmark-aspect',aspect.value);localStorage.setItem('benchmark-metric',metric.value);localStorage.setItem('benchmark-axis',axis.value);}catch(e){}
 const selected=rows.filter(r=>r.aspect_ratio===Number(aspect.value)), field=metric.value;
 const allFreq=data.axes?.[aspect.value]?.frequencies_hz||[...new Set(selected.map(r=>r.frequency_hz))].sort((a,b)=>a-b);
 const panels=document.getElementById('panels');panels.replaceChildren();
 const plotStatus=document.getElementById('plot-status');
 plotStatus.textContent=!allFreq.length?'No run configuration has been published for this platform yet.':!rows.length?'The run configuration is available. Timing points will appear after the first benchmark checkpoint.':'';
 if(!allFreq.length){document.getElementById('coverage').replaceChildren();return;}
 const axisValues=new Map(selected.map(r=>[r.frequency_hz,axis.value==='frequency_hz'?r.frequency_hz/1000:r[axis.value]]));
 let low=axisValues.get(allFreq[0]),high=axisValues.get(allFreq.at(-1));
 const extent=data.axes?.[aspect.value]?.[axis.value];if(extent){low=extent[0];high=extent[1];if(axis.value==='frequency_hz'){low/=1000;high/=1000;}}
 if(low===high){const margin=Math.abs(low)*.1||.5;low-=margin;high+=margin;}
 const transform=v=>field==='elapsed_seconds'?Math.log10(v):v;
 const valid=selected.filter(r=>r.status==='ok'&&Number.isFinite(r[field])&&(field!=='elapsed_seconds'||r[field]>0));
 const values=valid.map(r=>transform(r[field]));let ymin=values.length?values.reduce((a,b)=>Math.min(a,b)):0,ymax=values.length?values.reduce((a,b)=>Math.max(a,b)):1;
 const grouped=new Map();
 for(const row of valid){const key=JSON.stringify([row.boundary,row.software,row.incidence_deg,row.frequency_hz]);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(row);}
 if(ymin===ymax){ymin-=0.5;ymax+=0.5;}const pad=(ymax-ymin)*.08;ymin-=pad;ymax+=pad;
 for(const boundary of ['rigid','pressure_release','liquid','gas']){
  const section=document.createElement('section'),title=document.createElement('h2');title.textContent=boundaryLabels[boundary];section.append(title);
  const svg=svgElement('svg',{viewBox:'0 0 560 300',role:'img','aria-label':`${title.textContent} ${field} against frequency`});section.append(svg);panels.append(section);
  const x=f=>65+460*(axisValues.get(f)-low)/(high-low||1), y=v=>255-225*(transform(v)-ymin)/(ymax-ymin);
  for(let tick=0;tick<=4;tick++){
   const xx=65+460*tick/4,yy=255-225*tick/4,v=ymin+(ymax-ymin)*tick/4;
   svg.append(svgElement('line',{x1:65,x2:525,y1:yy,y2:yy,stroke:'#e3e5e5'}));
   const xv=low+(high-low)*tick/4;
   svg.append(svgElement('text',{x:xx,y:275,'text-anchor':'middle'},Math.abs(xv)>0&&Math.abs(xv)<.01?xv.toExponential(2):xv.toFixed(2)));
   svg.append(svgElement('text',{x:58,y:yy+4,'text-anchor':'end'},field==='elapsed_seconds'?(10**v).toPrecision(2):v.toFixed(2)));
  }
  svg.append(svgElement('text',{x:295,y:295,'text-anchor':'middle'},axis.options[axis.selectedIndex].text));
  for(const software of Object.keys(colors))for(const angle of [...new Set(selected.map(r=>r.incidence_deg))]){
   let path='';
   for(const frequency of allFreq){
    const observations=grouped.get(JSON.stringify([boundary,software,angle,frequency]))||[];
    if(!observations.length){path='';continue;}
    const value=median(observations.map(r=>r[field]));const xx=x(frequency),yy=y(value);
    if(path)svg.append(svgElement('path',{d:`M ${path} L ${xx} ${yy}`,fill:'none',stroke:colors[software],'stroke-width':1.7,'stroke-dasharray':angle===37?'8 4':angle===90?'2 4':''}));
    path=`${xx} ${yy}`;
    const dot=svgElement('circle',{cx:xx,cy:yy,r:2.5,fill:colors[software]});
    dot.append(svgElement('title',{},`${labels[software]}, ${angle}°, ${frequency/1000} kHz: ${value}, n=${observations.length}`));svg.append(dot);
   }
  }
 }
 const table=document.getElementById('coverage');table.replaceChildren();
 for(const software of Object.keys(colors))for(const boundary of Object.keys(boundaryLabels)){
  const coverage=data.coverage?.find(c=>c.aspect_ratio===Number(aspect.value)&&c.software===software&&c.boundary===boundary);
  const group=selected.filter(r=>r.software===software&&r.boundary===boundary);if(!coverage&&!group.length)continue;
  const good=group.filter(r=>r.status==='ok'),pending=group.filter(r=>r.status==='pending');
  const total=coverage?.elapsed_seconds_sum??good.reduce((sum,r)=>sum+r.elapsed_seconds,0),shownTotal=total>0&&total<.01?total.toExponential(2):total.toFixed(2);
  const successful=coverage?.success??good.length,other=coverage?.other_recorded??(group.length-good.length-pending.length);
  const remaining=coverage?.pending??(repository?'Unknown':pending.length);
  const tr=document.createElement('tr');for(const value of [labels[software],boundaryLabels[boundary],successful,other,remaining,shownTotal]){const td=document.createElement('td');td.textContent=value;tr.append(td);}table.append(tr);
 }
}
aspect.addEventListener('change',render);metric.addEventListener('change',render);axis.addEventListener('change',render);render();
if(repository){
 document.querySelector('meta[http-equiv="refresh"]').remove();
 document.getElementById('livebar').hidden=false;document.getElementById('connection').hidden=false;
 document.getElementById('manifest-link').hidden=true;
 document.getElementById('refresh-note').textContent='This dashboard checks for updates every 30 seconds. Checkpoints are published between benchmark chunks, usually every 5–15 minutes after setup. Full CSV and JSON remain available in the run artifacts.';
 const platform=document.getElementById('platform'),connection=document.getElementById('connection'),runLink=document.getElementById('run-link');
 const runState=document.getElementById('run-state');
 const workflowURL=`https://github.com/${repository}/actions/workflows/benchmark.yml`;
 runLink.href=workflowURL;document.getElementById('csv-link').href=workflowURL;
 document.getElementById('csv-link').textContent='Full data and artifacts';document.getElementById('json-link').hidden=true;
 try{platform.value=localStorage.getItem('benchmark-platform')||'ubuntu-latest';}catch(e){}
 if(!platform.value)platform.value='ubuntu-latest';
 let newestRun=null,newestSetup=null,lastRunCheck=0,busy=false,revision='';
 function runOrder(a,b){return (Number(a.run_id??a.id)-Number(b.run_id??b.id))||(Number(a.run_attempt||1)-Number(b.run_attempt||1));}
 function showRun(){
  const current=newestRun&&(!data.run_id||runOrder(newestRun,data)>=0)?newestRun:null;
  runLink.href=current?.html_url||data.run_url||workflowURL;
  runLink.textContent=current?`Benchmark run #${current.run_number}`:data.run_id?`Benchmark run ${data.run_id}`:'Open benchmark runs';
  runState.textContent=current?`${current.status.replaceAll('_',' ')}${current.conclusion?' · '+current.conclusion:''}`:`${data.run_state||data.state||''}${data.run_conclusion?' · '+data.run_conclusion:''}`;
  const setup=document.getElementById('setup-status');setup.hidden=!newestSetup;setup.replaceChildren();
  if(newestSetup){const link=document.createElement('a');link.href=newestSetup.html_url;link.target='_blank';link.rel='noopener';link.textContent=`Run #${newestSetup.run_number}`;setup.append('Latest setup check: ',link,` · ${newestSetup.status.replaceAll('_',' ')}${newestSetup.conclusion?' · '+newestSetup.conclusion:''}. Setup checks do not produce timing points.`);}
 }
 async function fetchJSON(url){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
  try{const response=await fetch(url,{cache:'no-store',signal:controller.signal});return {status:response.status,ok:response.ok,value:response.ok?await response.json():null};}
  finally{clearTimeout(timer);}
 }
 async function poll(){
  if(busy)return;busy=true;const chosen=platform.value;
  try{
   if(Date.now()-lastRunCheck>120000){
    lastRunCheck=Date.now();
    try{const response=await fetchJSON(`https://api.github.com/repos/${repository}/actions/workflows/benchmark.yml/runs?event=workflow_dispatch&per_page=30`);if(response.ok){const runs=response.value.workflow_runs||[];newestRun=runs.find(run=>!run.display_title?.startsWith('Prepare environments'))||null;newestSetup=runs.find(run=>run.display_title?.startsWith('Prepare environments'))||null;}}catch(e){}
   }
   const url=`https://raw.githubusercontent.com/${repository}/benchmark-data/${chosen}.json`;
   const response=await fetchJSON(url+'?t='+Math.floor(Date.now()/30000));
   if(chosen!==platform.value)return;
   if(response.status===404){connection.textContent=`No checkpoint has been published for ${chosen} yet. Setup must finish before measured points appear.`;showRun();return;}
   if(!response.ok)throw new Error(`Progress request returned ${response.status}`);
   const incoming=response.value;
   if(incoming.schema_version!==1||!Array.isArray(incoming.rows)||incoming.platform!==chosen)throw new Error('Unexpected progress data');
   const nextRevision=`${incoming.run_id}:${incoming.run_attempt}:${incoming.published_at}`;
   const order=data.run_id?runOrder(incoming,data):1;
   if(nextRevision!==revision&&(order>0||(order===0&&incoming.published_at>=data.published_at))){data=incoming;rows=data.rows;revision=nextRevision;updateAspects();updateStatus();render();}
   const old=newestRun&&runOrder(newestRun,data)>0;
   connection.textContent=old?`Showing the last checkpoint from run ${data.run_id}, attempt ${data.run_attempt}. A newer run or attempt is ${newestRun.status.replaceAll('_',' ')}. Open the run to check its selected platforms.`:rows.length?`Connected · ${chosen} · Published ${data.published_at}`:`Connected · ${chosen} · ${data.state==='completed'?'The run finished without a benchmark checkpoint. Open the run for details.':'Setup or the first benchmark chunk is still running. Measured points will appear here automatically.'}`;
   document.getElementById('csv-link').href=(data.run_url||runLink.href)+'#artifacts';
   const jsonLink=document.getElementById('json-link');jsonLink.hidden=false;jsonLink.href=url;jsonLink.textContent='Progress JSON';
   showRun();
  }catch(error){connection.textContent=`Unable to refresh: ${error.message}. Retrying in 30 seconds. Existing points are retained.`;showRun();}
  finally{busy=false;if(chosen!==platform.value)poll();}
 }
 platform.addEventListener('change',()=>{try{localStorage.setItem('benchmark-platform',platform.value);}catch(e){}data={rows:[],status:{},repository};rows=[];revision='';updateAspects();updateStatus();render();showRun();document.getElementById('csv-link').href=workflowURL;document.getElementById('json-link').hidden=true;connection.textContent='Connecting…';poll();});
 connection.textContent='Connecting to benchmark progress…';poll();setInterval(poll,30000);
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

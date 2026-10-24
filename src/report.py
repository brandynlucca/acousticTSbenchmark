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
</style>
<h1>Spheroid timing progress</h1>
<div id="livebar" hidden><label>Platform <select id="platform"><option>ubuntu-latest</option><option>windows-latest</option><option>macos-latest</option></select></label><a id="run-link" target="_blank" rel="noopener">Browse benchmark runs</a><span id="run-state"></span></div>
<p id="setup-status" class="note" hidden></p>
<p id="connection" role="status" hidden></p>
<p id="data-run-warning" role="status" hidden></p>
<p id="data-run" hidden>Displayed data: <a id="data-run-link" target="_blank" rel="noopener"></a><span id="data-run-detail"></span></p>
<p id="status"></p>
<p id="reference-status" class="note"></p>
<p><a id="csv-link" href="results.csv">Download CSV</a> · <a id="json-link" href="results.json">Full records</a> <span id="manifest-link">· <a href="manifest.json">Campaign manifest</a></span></p>
<p><label>Semimajor axis a (m) <select id="semimajor" aria-describedby="coverage-note"></select></label>
<label>Aspect ratio <select id="aspect"></select></label>
<label>Incidence <select id="incidence" aria-describedby="coverage-note"><option value="all">All angles</option></select></label>
<label>View <select id="layout"><option value="paired">Measure + ΔTS</option><option value="single">Single measure</option><option value="tradeoff">Cost vs |ΔTS|</option></select></label>
<label>Horizontal axis <select id="axis"><option value="frequency_hz">Frequency (kHz)</option><option value="h0">h₀</option><option value="k0a">k₀a</option></select></label>
<label>Measure <select id="metric"><option value="elapsed_seconds">Solve time (s, log scale)</option><option value="memory_delta_peak_bytes">Additional RAM per prediction (MiB)</option><option value="ts_db">TS (dB)</option><option value="delta_ts_db">ΔTS (dB, software minus reference)</option></select></label></p>
<div id="legend"></div><div id="angle-legend" aria-label="Incidence line and marker styles"></div><p id="plot-status" class="note" role="status"></p><div id="panels"></div>
<p class="note">Lines connect adjacent sampled frequencies only. Gaps include pending, failed and unsupported cases. Timings show medians of successful repetitions. A single repetition has no variability estimate. Timeout durations are not successful solve times. ΔTS is available only for explicitly qualified, matched references. Reference uncertainty remains in the downloadable records. <span id="refresh-note">This file refreshes every 30 seconds. Downloaded artifacts are snapshots.</span></p>
<p class="note" id="memory-note" hidden>Memory is the sampled peak additional resident RAM during one prediction, above its warmed-up baseline, including child processes. Setup and warmup are excluded. Reusing allocated memory can produce zero additional RAM. Short-lived peaks between samples may be missed. Values are medians across successful repetitions, not cumulative allocated bytes.</p>
<h2>Coverage and comparison error by software and boundary</h2><p class="note" id="coverage-note"></p><div class="coverage"><table><thead><tr><th>Software</th><th>Boundary</th><th>Success</th><th>Other recorded</th><th>Pending</th><th>Sum of measured solve times (s)</th><th>RMSE ΔTS (dB)</th><th>Matched predictions</th></tr></thead><tbody id="coverage"></tbody></table></div>
<p class="note" id="rmse-note">RMSE takes the median ΔTS across successful matched repetitions of each physical prediction, then √mean(ΔTS²) across those predictions, with equal weight per prediction. Only finite differences with a matched reference are included. Unavailable means no valid comparisons. The matched count includes each frequency and incidence combination once at the selected aspect ratio.</p>
<script id="data" type="application/json">PAYLOAD</script>
<script>
let data=JSON.parse(document.getElementById('data').textContent), rows=data.rows;
const repository=data.repository;
const colors={acousticts:'#0072B2',prol_spheroid:'#D55E00',echosms:'#CC79A7',liquid_spheroid:'#6b5b00'};
const labels={acousticts:'acousticTS',prol_spheroid:'Prol_Spheroid',acousticscattering:'AcousticScattering.jl',echosms:'echoSMs',liquid_spheroid:'liquid_spheroid'};
const boundaryLabels={rigid:'Fixed rigid',pressure_release:'Pressure release',liquid:'Liquid-filled',gas:'Gas-filled'};
const semimajor=document.getElementById('semimajor'),aspect=document.getElementById('aspect'),incidence=document.getElementById('incidence'),metric=document.getElementById('metric'),axis=document.getElementById('axis'),layout=document.getElementById('layout');
const sizeOptions=[.001,.01,.07,.1,.5];
let preferredSize='';
try{const saved=Number(localStorage.getItem('benchmark-semimajor'));if(Number.isFinite(saved)&&saved>0)preferredSize=String(saved);}catch(e){}
function snapshotSize(snapshot){
 if(Number.isFinite(snapshot.semimajor_m)&&snapshot.semimajor_m>0)return snapshot.semimajor_m;
 const values=[...new Set((snapshot.rows||[]).map(r=>r.semimajor_m).filter(a=>Number.isFinite(a)&&a>0))];
 return values.length===1?values[0]:null;
}
function updateSizes(){
 const known=[snapshotSize(data),...rows.map(r=>r.semimajor_m)].filter(a=>Number.isFinite(a)&&a>0);
 const values=[...new Set([...sizeOptions,...known,...(preferredSize?[Number(preferredSize)]:[])])].sort((a,b)=>a-b);
 semimajor.replaceChildren(...values.map(a=>new Option(String(a),String(a))));
 if(!preferredSize&&known.length)preferredSize=String(known[0]);
 semimajor.value=preferredSize||'0.07';
}
function matchesSize(row){return (Number.isFinite(row.semimajor_m)?row.semimajor_m:snapshotSize(data))===Number(semimajor.value);}
let availableIncidences=[],preferredIncidence='all',preferredAspect='';
try{preferredIncidence=localStorage.getItem('benchmark-incidence')||'all';preferredAspect=localStorage.getItem('benchmark-aspect')||'';}catch(e){}
function updateIncidences(){
 availableIncidences=[...new Set(data.incidences_deg||rows.filter(matchesSize).map(r=>r.incidence_deg))].filter(Number.isFinite).sort((a,b)=>a-b);
 incidence.replaceChildren(new Option('All angles','all'));
 for(const angle of availableIncidences)incidence.add(new Option(`${angle}°`,String(angle)));
 incidence.disabled=!availableIncidences.length;
 incidence.value=[...incidence.options].some(o=>o.value===preferredIncidence)?preferredIncidence:'all';
}
function updateAspects(){
 const previous=preferredAspect||aspect.value;aspect.replaceChildren();
 const ratios=data.aspect_ratios||[...new Set(rows.filter(matchesSize).map(r=>r.aspect_ratio))];
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
 const s=data.status,counts=s.outcomes||{},success=counts.ok||0,pending=counts.pending??Math.max(0,(s.planned||0)-(s.recorded||0));
 const other=Math.max(0,(s.recorded||0)-success),finished=data.state==='completed'||s.state==='complete';
 const phase=finished?(pending?(s.state==='budget_paused'?'Paused with pending cases. Resume to continue':'Run stopped with pending cases'):other?'Run finished with failures or skips':'Run finished'):data.state==='setup'?'Waiting for first timing checkpoint':'Benchmarking';
 document.getElementById('status').textContent=s.planned?`${s.recorded} / ${s.planned} recorded · ${success} successful · ${other} failed or skipped · ${pending} pending · ${phase} · Data updated ${s.updated_at||data.published_at||'pending'}`:'Waiting for the first benchmark checkpoint.';
}
updateStatus();
const legend=document.getElementById('legend');
for(const key of Object.keys(colors)){const s=document.createElement('span');s.dataset.software=key;s.style.color=colors[key];s.textContent='● '+labels[key];legend.append(s);}
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
function formatted(value){return Math.abs(value)>0&&Math.abs(value)<.01?value.toExponential(2):value.toFixed(2);}
function extentFor(values,zero=false){let lo=values.length?values.reduce((a,b)=>Math.min(a,b)):0,hi=values.length?values.reduce((a,b)=>Math.max(a,b)):1;if(zero){lo=Math.min(lo,0);hi=Math.max(hi,0);}if(lo===hi){lo-=.5;hi+=.5;}const padding=(hi-lo)*.08;return [lo-padding,hi+padding];}
function angleStyle(angle){
 if(angle===0)return {dash:'',shape:'circle'};
 if(angle===37)return {dash:'9 5',shape:'triangle'};
 if(angle===90)return {dash:'2 5',shape:'square'};
 const index=availableIncidences.filter(a=>![0,37,90].includes(a)).indexOf(angle);
 return [{dash:'9 4 2 4',shape:'diamond'},{dash:'12 4',shape:'cross'},{dash:'3 3',shape:'hexagon'}][Math.max(0,index)%3];
}
function marker(x,y,angle,color,size=3.5){
 const shape=angleStyle(angle).shape,attrs={fill:color,stroke:color,'stroke-width':1};
 if(shape==='triangle')return svgElement('path',{...attrs,d:`M ${x} ${y-size-1} L ${x-size-1} ${y+size} L ${x+size+1} ${y+size} Z`});
 if(shape==='square')return svgElement('rect',{...attrs,x:x-size,y:y-size,width:2*size,height:2*size});
 if(shape==='diamond')return svgElement('path',{...attrs,d:`M ${x} ${y-size-1} L ${x+size+1} ${y} L ${x} ${y+size+1} L ${x-size-1} ${y} Z`});
 if(shape==='cross')return svgElement('path',{...attrs,fill:'none','stroke-width':2,d:`M ${x-size} ${y-size} L ${x+size} ${y+size} M ${x-size} ${y+size} L ${x+size} ${y-size}`});
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
function render(){
 if(layout.value!=='single'&&!['elapsed_seconds','memory_delta_peak_bytes'].includes(metric.value))metric.value='elapsed_seconds';
 axis.disabled=layout.value==='tradeoff';
 try{localStorage.setItem('benchmark-metric',metric.value);localStorage.setItem('benchmark-axis',axis.value);localStorage.setItem('benchmark-layout',layout.value);}catch(e){}
 const matchesAngle=r=>incidence.value==='all'||r.incidence_deg===Number(incidence.value);
 const selected=rows.filter(r=>matchesSize(r)&&r.aspect_ratio===Number(aspect.value)&&matchesAngle(r)), field=metric.value;
 const coverageGroups=(incidence.value==='all'?data.coverage:data.incidence_coverage)?.filter(r=>r.aspect_ratio===Number(aspect.value)&&matchesAngle(r));
 const plannedGroups=coverageGroups||(data.coverage||[]).filter(r=>r.aspect_ratio===Number(aspect.value));
 const measured=selected.filter(r=>r.status==='ok'),compared=measured.filter(r=>Number.isFinite(r.delta_ts_db));
 const selectedSoftware=new Set([...selected.map(r=>r.software),...plannedGroups.map(r=>r.software)]);
 for(const item of legend.querySelectorAll('[data-software]'))item.hidden=!selectedSoftware.has(item.dataset.software);
 document.getElementById('memory-note').hidden=field!=='memory_delta_peak_bytes';
 document.getElementById('reference-status').textContent=`BEM / FEM-BEM references: ${compared.length} of ${measured.length} successful measurements have a matched, convergence-checked reference. References run separately and are reused across platforms. Missing references leave gaps in ΔTS.`;
 const allFreq=data.axes?.[aspect.value]?.frequencies_hz||[...new Set(selected.map(r=>r.frequency_hz))].sort((a,b)=>a-b);
 const panels=document.getElementById('panels');panels.replaceChildren();panels.classList.toggle('paired',layout.value==='paired');
 const selectedAngles=incidence.value==='all'?availableIncidences:[Number(incidence.value)];
 renderAngles(selectedAngles);
 document.getElementById('coverage-note').textContent=aspect.value?`Every plot and coverage row uses a = ${semimajor.value} m (total length 2a = ${Number(semimajor.value)*2} m), aspect ratio ${aspect.value} and ${incidence.value==='all'?'all incidences':incidence.value+'° incidence'}. Campaign progress above includes every planned case in the displayed run.`:'Size and incidence selections apply to every plot and coverage row once results are available.';
 if(repository&&incidence.value!=='all'&&!data.incidence_coverage)document.getElementById('coverage-note').textContent+=' This snapshot does not report pending counts by incidence.';
 const plotStatus=document.getElementById('plot-status');
 plotStatus.textContent=!allFreq.length?`No results are available for a = ${semimajor.value} m in this view yet.`:!rows.length?'The run configuration is available. Timing points will appear after the first benchmark checkpoint.':!selected.some(r=>r.status!=='pending')?'No cases have been recorded for the selected size, incidence and aspect ratio yet.':(layout.value!=='single'||field==='delta_ts_db')&&!compared.length?'No qualified references have been attached yet. The reference jobs save results independently, then update comparisons without repeating the timings.':layout.value==='tradeoff'?'Each point is one frequency and incidence. Cost and absolute ΔTS use medians across matched successful repetitions.':'';
 if(field==='memory_delta_peak_bytes'&&!measured.some(r=>validValue(r,field)))plotStatus.textContent+=' No per-prediction memory measurements are available in this snapshot.';
 if(!allFreq.length){document.getElementById('coverage').replaceChildren();return;}
 const axisValues=new Map(selected.map(r=>[r.frequency_hz,axis.value==='frequency_hz'?r.frequency_hz/1000:r[axis.value]]));
 let low=axisValues.get(allFreq[0]),high=axisValues.get(allFreq.at(-1));
 const extent=data.axes?.[aspect.value]?.[axis.value];if(extent){low=extent[0];high=extent[1];if(axis.value==='frequency_hz'){low/=1000;high/=1000;}}
 if(low===high){const margin=Math.abs(low)*.1||.5;low-=margin;high+=margin;}
 const grouped=new Map();
 for(const row of measured){const key=JSON.stringify([row.boundary,row.software,row.incidence_deg,row.frequency_hz]);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(row);}
 const visibleBoundaries=new Set([...selected.map(r=>r.boundary),...plannedGroups.map(r=>r.boundary)]);
 for(const boundary of Object.keys(boundaryLabels).filter(b=>visibleBoundaries.has(b))){
  const section=document.createElement('section'),title=document.createElement('h2');title.textContent=boundaryLabels[boundary];section.append(title);
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
   if(!count)svg.append(svgElement('text',{x:295,y:140,'text-anchor':'middle',class:'empty'},'Waiting for matched cost and reference data'));
   continue;
  }
  for(const measure of layout.value==='paired'?[field,'delta_ts_db']:[field]){
   const transform=v=>measure==='elapsed_seconds'?Math.log10(v):v;
   const valid=measured.filter(r=>validValue(r,measure)),yr=extentFor(valid.map(r=>transform(valueFor(r,measure))),measure==='delta_ts_db');
   const {svg,x,y}=chartFrame(charts,measures[measure],`${title.textContent}: ${measures[measure]} against ${axis.options[axis.selectedIndex].text}`,axis.options[axis.selectedIndex].text,[low,high],yr,false,measure==='elapsed_seconds');
   if(measure==='delta_ts_db')svg.append(svgElement('line',{x1:70,x2:520,y1:y(0),y2:y(0),stroke:'#89949a','stroke-dasharray':'4 4'}));
   let count=0;
   for(const software of Object.keys(colors))for(const angle of selectedAngles){
    let path='';
    for(const frequency of allFreq){
     const observations=(grouped.get(JSON.stringify([boundary,software,angle,frequency]))||[]).filter(r=>validValue(r,measure));
     if(!observations.length){path='';continue;}
     const value=median(observations.map(r=>valueFor(r,measure))),xx=x(axisValues.get(frequency)),yy=y(transform(value));
     if(path)svg.append(svgElement('path',{class:'series-segment','data-incidence':angle,d:`M ${path} L ${xx} ${yy}`,fill:'none',stroke:colors[software],'stroke-width':1.7,'stroke-dasharray':angleStyle(angle).dash}));
     path=`${xx} ${yy}`;point(svg,xx,yy,angle,colors[software],`${labels[software]}, ${angle}°, ${frequency/1000} kHz: ${formatted(value)}, n=${observations.length}`);count++;
    }
   }
   if(!count)svg.append(svgElement('text',{x:295,y:140,'text-anchor':'middle',class:'empty'},measure==='delta_ts_db'?'Waiting for qualified reference comparisons':'Waiting for measurements'));
  }
 }
 const table=document.getElementById('coverage');table.replaceChildren();
 for(const software of Object.keys(colors))for(const boundary of Object.keys(boundaryLabels)){
  const coverage=coverageGroups?.find(c=>c.software===software&&c.boundary===boundary);
  const planned=plannedGroups.some(c=>c.software===software&&c.boundary===boundary);
  const group=selected.filter(r=>r.software===software&&r.boundary===boundary);if(!planned&&!group.length)continue;
  const good=group.filter(r=>r.status==='ok'),pending=group.filter(r=>r.status==='pending');
  const total=coverage?.elapsed_seconds_sum??good.reduce((sum,r)=>sum+r.elapsed_seconds,0),shownTotal=total>0&&total<.01?total.toExponential(2):total.toFixed(2);
  const successful=coverage?.success??good.length,other=coverage?.other_recorded??(group.length-good.length-pending.length);
  const remaining=coverage?.pending??(repository?'Unknown':pending.length);
  const comparison=comparisonSummary(group),shownRMSE=comparison.count?comparison.rmse.toFixed(2):'Unavailable';
  const tr=document.createElement('tr');tr.dataset.software=software;tr.dataset.boundary=boundary;
  for(const value of [labels[software],boundaryLabels[boundary],successful,other,remaining,shownTotal,shownRMSE,comparison.count]){const td=document.createElement('td');td.textContent=value;tr.append(td);}table.append(tr);
 }
}
aspect.addEventListener('change',()=>{preferredAspect=aspect.value;try{localStorage.setItem('benchmark-aspect',preferredAspect);}catch(e){}render();});incidence.addEventListener('change',()=>{preferredIncidence=incidence.value;try{localStorage.setItem('benchmark-incidence',preferredIncidence);}catch(e){}render();});metric.addEventListener('change',()=>{if(!['elapsed_seconds','memory_delta_peak_bytes'].includes(metric.value))layout.value='single';render();});axis.addEventListener('change',render);layout.addEventListener('change',render);render();
semimajor.addEventListener('change',()=>{preferredSize=semimajor.value;try{localStorage.setItem('benchmark-semimajor',preferredSize);}catch(e){}if(!repository){updateAspects();updateIncidences();render();}});
if(repository){
 document.querySelector('meta[http-equiv="refresh"]').remove();
 document.getElementById('livebar').hidden=false;document.getElementById('connection').hidden=false;
 document.getElementById('manifest-link').hidden=true;
 document.getElementById('refresh-note').textContent='This dashboard checks for updates every 30 seconds. New runs publish completed results between cases at 30-second intervals. A long-running case appears when it finishes. Older runs publish only between chunks. GitHub caching can add delay. Full CSV and JSON remain available in the run artifacts.';
 const platform=document.getElementById('platform'),connection=document.getElementById('connection'),runLink=document.getElementById('run-link');
 const runState=document.getElementById('run-state');
 const workflowURL=`https://github.com/${repository}/actions/workflows/benchmark.yml`;
 runLink.href=workflowURL;document.getElementById('csv-link').href=workflowURL;
 document.getElementById('csv-link').textContent='Full data and artifacts';document.getElementById('json-link').hidden=true;
 try{platform.value=localStorage.getItem('benchmark-platform')||'ubuntu-latest';}catch(e){}
 if(!platform.value)platform.value='ubuntu-latest';
 let newestRun=null,newestSetup=null,lastRunCheck=0,lastContentsCheck=0,busy=false,revision='',generation=0;
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
   document.getElementById('data-run-detail').textContent=` · attempt ${data.run_attempt||1} · ${data.platform||platform.value} · a = ${semimajor.value} m. Counts, failures and plots below belong to this run.`;
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
 async function poll(){
  if(busy)return;busy=true;const chosen=platform.value,requestGeneration=generation;
  try{
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
     const value=response.value;
     if(value.schema_version!==1||!Array.isArray(value.rows)||value.platform!==chosen)throw new Error('Unexpected progress data');
     const sizes=[value.semimajor_m,...value.rows.map(r=>r.semimajor_m)].filter(a=>a!==undefined&&a!==null);
     if(sizes.some(a=>!Number.isFinite(a)||a<=0)||new Set(sizes).size>1)throw new Error('Progress data contains inconsistent semimajor axes');
    }
    return response;
   }
   let filename=`${chosen}.json`,response=await snapshot(filename,!preferredSize||snapshotSize(data)===Number(preferredSize));
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
   if(preferredSize&&latestSize!==Number(preferredSize)){
    filename=`${chosen}-a-${Number(preferredSize)}.json`;response=await snapshot(filename,true);
   }
   if(requestGeneration!==generation)return;
   if(response.status===404){connection.textContent=`No results have been published for ${chosen} at a = ${semimajor.value} m yet. Run a benchmark at this size to populate it.`;showRun();return;}
   if(!response.ok)throw new Error(`Progress request returned ${response.status}`);
   const incoming=response.value;
   if(snapshotSize(incoming)!==Number(semimajor.value)){connection.textContent='The size of this older snapshot has not been recovered from its artifact yet.';showRun();return;}
   const url=feed+filename;
   const nextRevision=`${incoming.run_id}:${incoming.run_attempt}:${incoming.published_at}`;
   const order=data.run_id?runOrder(incoming,data):1;
   if(nextRevision!==revision&&(order>0||(order===0&&incoming.published_at>=data.published_at))){data=incoming;rows=data.rows;revision=nextRevision;updateSizes();updateAspects();updateIncidences();updateStatus();render();}
   connection.textContent=rows.length?`Connected · ${chosen} · a = ${semimajor.value} m · Published ${data.published_at}`:`Connected · ${chosen} · a = ${semimajor.value} m · ${data.state==='completed'?'The displayed data run finished without a benchmark checkpoint. Open its data run link for details.':'Setup or the first benchmark chunk is still running. Measured points will appear here automatically.'}`;
   if(publicFeed)connection.textContent+=' Using the public feed, which GitHub may cache for several minutes.';
   document.getElementById('csv-link').href=data.artifacts_url||((data.run_url||runLink.href)+'#artifacts');
   const jsonLink=document.getElementById('json-link');jsonLink.hidden=false;jsonLink.href=url;jsonLink.textContent='Progress JSON';
   showRun();
  }catch(error){if(requestGeneration===generation){connection.textContent=`Unable to refresh: ${error.message}. Retrying in 30 seconds. Existing points are retained.`;showRun();}}
  finally{busy=false;if(requestGeneration!==generation)poll();}
 }
 function changeSelection(){generation++;data={rows:[],status:{},repository};rows=[];revision='';updateAspects();updateIncidences();updateStatus();render();showRun();document.getElementById('csv-link').href=workflowURL;document.getElementById('json-link').hidden=true;connection.textContent='Connecting…';poll();}
 platform.addEventListener('change',()=>{try{localStorage.setItem('benchmark-platform',platform.value);}catch(e){}changeSelection();});
 semimajor.addEventListener('change',changeSelection);
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

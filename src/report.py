"""Render a self-contained progress view without work during solver timing."""

import json
import os
from pathlib import Path


def render_report(path, rows, status):
    payload = json.dumps({"rows": rows, "status": status}, allow_nan=False).replace("<", "\\u003c")
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
</style>
<h1>Spheroid timing progress</h1><p id="status"></p>
<p><a href="results.csv">Download CSV</a> · <a href="results.json">Full records</a> · <a href="manifest.json">Campaign manifest</a></p>
<p><label>Aspect ratio <select id="aspect"></select></label>
<label>Horizontal axis <select id="axis"><option value="frequency_hz">Frequency (kHz)</option><option value="h0">h₀</option><option value="k0a">k₀a</option></select></label>
<label>Measure <select id="metric"><option value="elapsed_seconds">Solve time (s, log scale)</option><option value="ts_db">TS (dB)</option><option value="delta_ts_db">ΔTS (dB, software minus reference)</option></select></label></p>
<div id="legend"></div><div id="panels"></div>
<p class="note">Lines connect adjacent sampled frequencies only. Gaps include pending, failed and unsupported cases. Timings show medians of successful repetitions. A single repetition has no variability estimate. Timeout durations are not successful solve times. ΔTS is available only for explicitly qualified, matched references. Reference uncertainty remains in the downloadable records. This file refreshes every 30 seconds. Downloaded artifacts are snapshots.</p>
<h2>Coverage by software and boundary</h2><div class="coverage"><table><thead><tr><th>Software</th><th>Boundary</th><th>Success</th><th>Other recorded</th><th>Pending</th><th>Sum of measured solve times (s)</th></tr></thead><tbody id="coverage"></tbody></table></div>
<script id="data" type="application/json">PAYLOAD</script>
<script>
const data=JSON.parse(document.getElementById('data').textContent), rows=data.rows;
const colors={acousticts:'#0072B2',prol_spheroid:'#D55E00',acousticscattering:'#009E73',echosms:'#CC79A7',liquid_spheroid:'#6b5b00'};
const labels={acousticts:'acousticTS',prol_spheroid:'Prol_Spheroid',acousticscattering:'AcousticScattering.jl',echosms:'echoSMs',liquid_spheroid:'liquid_spheroid'};
const boundaryLabels={rigid:'Fixed rigid',pressure_release:'Pressure release',liquid:'Liquid-filled',gas:'Gas-filled'};
const aspect=document.getElementById('aspect'),metric=document.getElementById('metric'),axis=document.getElementById('axis');
for(const ratio of [...new Set(rows.map(r=>r.aspect_ratio))]) aspect.add(new Option(ratio,ratio));
try {aspect.value=localStorage.getItem('benchmark-aspect')||aspect.value; if(!aspect.value)aspect.selectedIndex=0;metric.value=localStorage.getItem('benchmark-metric')||metric.value;axis.value=localStorage.getItem('benchmark-axis')||axis.value;}catch(e){}
document.getElementById('status').textContent=`${data.status.state}: ${data.status.recorded} / ${data.status.planned} recorded. Updated ${data.status.updated_at}`;
const legend=document.getElementById('legend');
for(const key of Object.keys(colors)){const s=document.createElement('span');s.style.color=colors[key];s.textContent='● '+labels[key];legend.append(s);}
const angles=document.createElement('span');angles.textContent='0° solid · 37° dashed · 90° dotted';legend.append(angles);
function svgElement(name,attrs={},text){const e=document.createElementNS('http://www.w3.org/2000/svg',name);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
function median(values){values.sort((a,b)=>a-b);const n=values.length;return n%2?values[(n-1)/2]:(values[n/2-1]+values[n/2])/2;}
function render(){
 try{localStorage.setItem('benchmark-aspect',aspect.value);localStorage.setItem('benchmark-metric',metric.value);localStorage.setItem('benchmark-axis',axis.value);}catch(e){}
 const selected=rows.filter(r=>r.aspect_ratio===Number(aspect.value)), field=metric.value;
 const allFreq=[...new Set(selected.map(r=>r.frequency_hz))].sort((a,b)=>a-b);
 const axisValues=new Map(selected.map(r=>[r.frequency_hz,axis.value==='frequency_hz'?r.frequency_hz/1000:r[axis.value]]));
 let low=axisValues.get(allFreq[0]),high=axisValues.get(allFreq.at(-1));
 if(low===high){const margin=Math.abs(low)*.1||.5;low-=margin;high+=margin;}
 const transform=v=>field==='elapsed_seconds'?Math.log10(v):v;
 const valid=selected.filter(r=>r.status==='ok'&&Number.isFinite(r[field])&&(field!=='elapsed_seconds'||r[field]>0));
 const values=valid.map(r=>transform(r[field]));let ymin=values.length?values.reduce((a,b)=>Math.min(a,b)):0,ymax=values.length?values.reduce((a,b)=>Math.max(a,b)):1;
 const grouped=new Map();
 for(const row of valid){const key=JSON.stringify([row.boundary,row.software,row.incidence_deg,row.frequency_hz]);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(row);}
 if(ymin===ymax){ymin-=0.5;ymax+=0.5;}const pad=(ymax-ymin)*.08;ymin-=pad;ymax+=pad;
 const panels=document.getElementById('panels');panels.replaceChildren();
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
  const group=selected.filter(r=>r.software===software&&r.boundary===boundary);if(!group.length)continue;
  const good=group.filter(r=>r.status==='ok'),pending=group.filter(r=>r.status==='pending');
  const total=good.reduce((sum,r)=>sum+r.elapsed_seconds,0),shownTotal=total>0&&total<.01?total.toExponential(2):total.toFixed(2);
  const tr=document.createElement('tr');for(const value of [labels[software],boundaryLabels[boundary],good.length,group.length-good.length-pending.length,pending.length,shownTotal]){const td=document.createElement('td');td.textContent=value;tr.append(td);}table.append(tr);
 }
}
aspect.addEventListener('change',render);metric.addEventListener('change',render);axis.addEventListener('change',render);render();
</script></html>'''.replace("PAYLOAD", payload)
    path = Path(path)
    temporary = path.with_suffix(".html.tmp")
    temporary.write_text(document, encoding="utf-8")
    os.replace(temporary, path)

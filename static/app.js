const API='';

// ═══ Utilities ═══════════════════════════════════════════

function toast(msg,ok=true){
  const c=document.getElementById('toastContainer');
  const t=document.createElement('div');
  t.className='toast show '+(ok?'ok':'err');
  t.textContent=msg;
  c.appendChild(t);
  setTimeout(()=>{t.classList.remove('show');setTimeout(()=>t.remove(),300)},3000);
}

async function api(path,opts){
  try{const r=await fetch(API+path,opts);return r.json();}
  catch(e){return {error:e.message};}
}

function syntaxHL(obj){
  let s=JSON.stringify(obj,null,2);
  if(!s)return '';
  s=s.replace(/&/g,'&amp;').replace(/</g,'&lt;');
  s=s.replace(/"([^"]+)":/g,'<span class="k">"$1"</span>:');
  s=s.replace(/: "([^"]*)"/g,': <span class="s">"$1"</span>');
  s=s.replace(/: (-?\d+\.?\d*)/g,': <span class="n">$1</span>');
  s=s.replace(/: (true|false)/g,': <span class="bool">$1</span>');
  s=s.replace(/: (null)/g,': <span class="null">$1</span>');
  return s;
}

function timerHTML(ms){
  return '<span class="timer"><svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.2"/><path d="M8 5v3.5l2.5 1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>'+ms+'ms</span>';
}

// ═══ Tab Navigation ══════════════════════════════════════

function switchTab(name){
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.tab===name));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.toggle('active',p.id==='panel-'+name));
  if(name==='documents')refreshDocs();
  if(name==='indexes')refreshIndexes();
  if(name==='oplog')refreshOplog();
  if(name==='sync'){refreshSyncStatus();updateCounts();}
}

// ═══ Shell ═══════════════════════════════════════════════

const shellHistory=[];
let historyIdx=-1;

document.getElementById('shellInput').addEventListener('keydown',function(e){
  if(e.key==='Enter'){
    e.preventDefault();
    const cmd=this.value.trim();
    if(!cmd)return;
    shellHistory.unshift(cmd);
    historyIdx=-1;
    runShellCmd(cmd);
    this.value='';
  }
  if(e.key==='ArrowUp'){
    e.preventDefault();
    if(historyIdx<shellHistory.length-1){historyIdx++;this.value=shellHistory[historyIdx];}
  }
  if(e.key==='ArrowDown'){
    e.preventDefault();
    if(historyIdx>0){historyIdx--;this.value=shellHistory[historyIdx];}
    else{historyIdx=-1;this.value='';}
  }
});

async function runShellCmd(cmd){
  const out=document.getElementById('shellOutput');
  out.innerHTML+='<div class="line"><span class="prompt">&gt; </span><span class="cmd">'+escHTML(cmd)+'</span></div>';
  const res=await api('/api/shell',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({command:cmd})});
  if(res.error){
    out.innerHTML+='<div class="line err">'+escHTML(res.error)+'</div>';
  }else{
    const formatted=JSON.stringify(res.result,null,2);
    out.innerHTML+='<div class="line result">'+escHTML(formatted)+'</div>';
  }
  if(res.ms!==undefined){
    out.innerHTML+='<div class="line timing">\u23F1 '+res.ms+'ms</div>';
  }
  out.innerHTML+='<div class="line">&nbsp;</div>';
  out.scrollTop=out.scrollHeight;
  refreshHeaderStats();
}

function escHTML(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// ═══ Documents ═══════════════════════════════════════════

async function refreshDocs(){
  const docs=await api('/api/docs?coll=users');
  const el=document.getElementById('docsBody');
  if(!docs||docs.error||!docs.length){el.innerHTML='<div class="empty">Collection is empty.</div>';return;}
  const fields=['_id','name','age','city','dept','salary','tags'];
  const activeFields=fields.filter(f=>docs.some(d=>d[f]!==undefined));
  let h='<table><thead><tr>'+activeFields.map(f=>'<th>'+f+'</th>').join('')+'<th></th></tr></thead><tbody>';
  docs.forEach(d=>{
    h+='<tr>'+activeFields.map(f=>{
      let v=d[f]; if(v===undefined)v='';
      else if(Array.isArray(v))v='<span style="color:var(--purple)">'+v.map(x=>'<span class="chip" style="margin:0;padding:2px 6px;font-size:10px;cursor:default">'+x+'</span>').join(' ')+'</span>';
      else if(typeof v==='object'&&v!==null)v='<span style="color:var(--text2)">'+JSON.stringify(v)+'</span>';
      else if(f==='_id')v='<span style="color:var(--text3);font-family:var(--mono);font-size:10px" title="'+v+'">'+v.slice(0,8)+'...</span>';
      else if(f==='salary')v='<span style="color:var(--green);font-family:var(--mono)">$'+Number(v).toLocaleString()+'</span>';
      else v=escHTML(String(v));
      return '<td>'+v+'</td>';
    }).join('')+
    '<td><button class="btn xs danger" onclick="deleteDoc(\''+d._id+'\')">Del</button></td></tr>';
  });
  h+='</tbody></table>';
  el.innerHTML=h;
  document.getElementById('docCountBadge').textContent=docs.length;
  document.getElementById('hdrDocCount').textContent=docs.length;
  document.getElementById('localCount').textContent=docs.length;
}

async function deleteDoc(id){
  if(!confirm('Delete this document?'))return;
  const res=await api('/api/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',query:{"_id":id}})});
  if(res.error){toast(res.error,false);return;}
  toast('Deleted document');
  refreshDocs();
}

function showInsertModal(){document.getElementById('insertModal').style.display='flex';}
function hideInsertModal(){document.getElementById('insertModal').style.display='none';}

async function insertFromModal(){
  let doc;
  try{doc=JSON.parse(document.getElementById('insertDocInput').value);}catch(e){toast('Invalid JSON',false);return;}
  const res=await api('/api/insert',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',doc})});
  if(res.error){toast(res.error,false);return;}
  toast('Document inserted');
  hideInsertModal();
  refreshDocs();
}

// ═══ Query ═══════════════════════════════════════════════

function setQuery(q){document.getElementById('queryInput').value=q;}

async function runQuery(){
  const raw=document.getElementById('queryInput').value.trim();
  let query;
  try{query=JSON.parse(raw);}catch(e){toast('Invalid JSON: '+e.message,false);return;}
  const res=await api('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',query})});
  if(res.error){toast(res.error,false);return;}
  renderQueryResult(res);
}

async function explainQuery(){
  const raw=document.getElementById('queryInput').value.trim();
  let query;
  try{query=JSON.parse(raw);}catch(e){toast('Invalid JSON: '+e.message,false);return;}
  const res=await api('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',query})});
  if(res.error){toast(res.error,false);return;}
  document.getElementById('queryResults').innerHTML=syntaxHL(res.plan);
  if(res.ms) document.getElementById('queryTimer').innerHTML=timerHTML(res.ms);
  renderPlanBadge(res.plan,res.count);
}

function renderQueryResult(res){
  const plan=res.plan||{};
  renderPlanBadge(plan,res.count);
  document.getElementById('queryResults').innerHTML=syntaxHL(res.docs);
  if(res.ms) document.getElementById('queryTimer').innerHTML=timerHTML(res.ms);
}

function renderPlanBadge(plan,count){
  let badge='';
  if(plan.plan==='index_scan') badge='<span class="badge idx">INDEX SCAN</span> <span style="color:var(--text2);font-size:11px">using <strong>'+plan.index+'</strong></span>';
  else if(plan.plan==='pk_lookup') badge='<span class="badge pk">PK LOOKUP</span>';
  else badge='<span class="badge scan">COLL SCAN</span>';
  document.getElementById('queryPlan').innerHTML=badge+' <span style="color:var(--text3);font-size:11px;margin-left:8px">'+count+' result'+(count!==1?'s':'')+'</span>';
}

// ═══ Aggregation ═════════════════════════════════════════

const defaultStageValues={
  '$match':'{"dept": "engineering"}',
  '$group':'{"_id": "$city", "count": {"$sum": 1}, "avgAge": {"$avg": "$age"}}',
  '$sort':'{"count": -1}',
  '$project':'{"_id": 1, "name": 1, "salary": 1}',
  '$lookup':'{"from":"departments","localField":"dept","foreignField":"name","as":"deptInfo"}',
  '$addFields':'{"compBand":{"$cond":[{"$gte":["$salary",150000]},"high","standard"]}}',
  '$replaceRoot':'{"newRoot":"$$ROOT"}',
  '$count':'"total"',
  '$unwind':'"$tags"',
  '$limit':'5',
  '$skip':'0'
};

const examplePipelines={
  countByCity:[
    {type:'$group',value:'{"_id": "$city", "count": {"$sum": 1}}'},
    {type:'$sort',value:'{"count": -1}'}
  ],
  avgSalaryByDept:[
    {type:'$group',value:'{"_id": "$dept", "avgSalary": {"$avg": "$salary"}, "count": {"$sum": 1}}'},
    {type:'$sort',value:'{"avgSalary": -1}'}
  ],
  topEarners:[
    {type:'$sort',value:'{"salary": -1}'},
    {type:'$limit',value:'3'},
    {type:'$project',value:'{"name": 1, "salary": 1, "dept": 1}'}
  ],
  tagBreakdown:[
    {type:'$unwind',value:'"$tags"'},
    {type:'$group',value:'{"_id": "$tags", "count": {"$sum": 1}}'},
    {type:'$sort',value:'{"count": -1}'}
  ],
  nycEngineers:[
    {type:'$match',value:'{"city": "NYC", "dept": "engineering"}'},
    {type:'$project',value:'{"name": 1, "salary": 1, "tags": 1}'}
  ],
  deptCount:[
    {type:'$match',value:'{"dept":"engineering"}'},
    {type:'$count',value:'"engineers"'}
  ],
  withLookup:[
    {type:'$lookup',value:'{"from":"departments","localField":"dept","foreignField":"name","as":"deptInfo"}'},
    {type:'$limit',value:'5'}
  ]
};

let stages=[];

function addStage(type,value){
  stages.push({type,value:value||defaultStageValues[type]||'{}'});
  renderStages();
}

function removeStage(i){stages.splice(i,1);renderStages();}

function clearStages(){stages=[];renderStages();document.getElementById('aggResults').innerHTML='<span style="color:var(--text3)">Build a pipeline and click Run</span>';}

function loadPipeline(name){
  stages=examplePipelines[name].map(s=>({...s}));
  renderStages();
}

function renderStages(){
  const el=document.getElementById('stagesContainer');
  if(!stages.length){el.innerHTML='<div class="empty" style="padding:20px">Add stages using the buttons above or pick an example pipeline</div>';return;}
  el.innerHTML=stages.map((s,i)=>
    '<div class="stage-card">'+
      '<span class="stage-num">'+(i+1)+'</span>'+
      '<div class="stage-body">'+
        '<div class="stage-type">'+s.type+'</div>'+
        '<textarea rows="2" onchange="stages['+i+'].value=this.value">'+s.value+'</textarea>'+
      '</div>'+
      '<span class="remove-stage" onclick="removeStage('+i+')">&times;</span>'+
    '</div>').join('');
}

async function runAggregate(){
  let pipeline;
  try{
    pipeline=stages.map(s=>{
      const val=JSON.parse(s.value);
      return {[s.type]:val};
    });
  }catch(e){toast('Invalid JSON in pipeline: '+e.message,false);return;}
  const res=await api('/api/aggregate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',pipeline})});
  if(res.error){toast(res.error,false);document.getElementById('aggResults').innerHTML='<span style="color:var(--red)">'+escHTML(res.error)+'</span>';return;}
  document.getElementById('aggResults').innerHTML=syntaxHL(res.results);
  document.getElementById('aggTimer').innerHTML=timerHTML(res.ms)+' <span style="color:var(--text3);font-size:11px;margin-left:6px">'+res.count+' result'+(res.count!==1?'s':'')+'</span>';
}

renderStages();

// ═══ Indexes ═════════════════════════════════════════════

async function refreshIndexes(){
  const idxs=await api('/api/indexes?coll=users');
  const el=document.getElementById('idxBody');
  if(!idxs||idxs.error||!idxs.length){el.innerHTML='<tr><td colspan="5" style="color:var(--text3)">No indexes</td></tr>';return;}
  el.innerHTML=idxs.map(i=>'<tr>'+
    '<td style="font-family:var(--mono);font-size:11px">'+i.name+'</td>'+
    '<td style="font-family:var(--mono);font-size:11px;color:var(--text2)">'+JSON.stringify(i.keys)+'</td>'+
    '<td>'+(i.unique?'<span style="color:var(--green);font-weight:700">Yes</span>':'<span style="color:var(--text3)">No</span>')+'</td>'+
    '<td>'+(i.sparse?'<span style="color:var(--orange)">Yes</span>':'<span style="color:var(--text3)">No</span>')+'</td>'+
    '<td><button class="btn xs danger" onclick="dropIdx(\''+i.name+'\')">Drop</button></td>'+
  '</tr>').join('');
  document.getElementById('idxCountBadge').textContent=idxs.length;
  document.getElementById('hdrIdxCount').textContent=idxs.length;
}

function setIdxKeys(keys,unique){
  document.getElementById('newIdxKeys').value=keys;
  if(unique)document.getElementById('newIdxUnique').value='true';
  else document.getElementById('newIdxUnique').value='false';
}

async function createIdx(){
  let keys;try{keys=JSON.parse(document.getElementById('newIdxKeys').value);}catch(e){toast('Invalid JSON',false);return;}
  const unique=document.getElementById('newIdxUnique').value==='true';
  const ttlRaw=document.getElementById('newIdxTtl').value.trim();
  const expireAfterSeconds=ttlRaw===''?null:Number(ttlRaw);
  const payload={coll:'users',keys,unique};
  if(expireAfterSeconds!==null && !Number.isNaN(expireAfterSeconds)) payload.expireAfterSeconds=expireAfterSeconds;
  const res=await api('/api/indexes',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  if(res.error){toast(res.error,false);return;}
  toast('Index '+res.name+' created');
  refreshIndexes();
}

async function dropIdx(name){
  if(!confirm('Drop index '+name+'?'))return;
  await api('/api/indexes/'+encodeURIComponent(name)+'?coll=users',{method:'DELETE'});
  toast('Dropped '+name);
  refreshIndexes();
}

async function testPlan(){
  let query;try{query=JSON.parse(document.getElementById('planQuery').value);}catch(e){toast('Invalid JSON',false);return;}
  const res=await api('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',query})});
  if(res.error){toast(res.error,false);return;}
  document.getElementById('planResult').innerHTML=syntaxHL(res.plan);
}

// ═══ Sync ════════════════════════════════════════════════

let syncRunning=false;

async function syncPush(){
  toast('Pushing to MongoDB...');
  const res=await api('/api/sync/push',{method:'POST'});
  toast(res.error?res.error:'Push complete',!res.error);
  refreshSyncStatus();updateCounts();refreshDocs();
}

async function syncPull(){
  toast('Pulling from MongoDB...');
  const res=await api('/api/sync/pull',{method:'POST'});
  toast(res.error?res.error:'Pull complete',!res.error);
  refreshSyncStatus();refreshDocs();updateCounts();
}

async function toggleSync(){
  if(syncRunning){
    await api('/api/sync/stop',{method:'POST'});
    syncRunning=false;
  }else{
    await api('/api/sync/start',{method:'POST'});
    syncRunning=true;
  }
  updateSyncUI();
  refreshSyncStatus();
}

function updateSyncUI(){
  const pill=document.getElementById('syncPill');
  const btn1=document.getElementById('syncBtn');
  const btn2=document.getElementById('syncBtn2');
  const label=document.getElementById('syncLabel');
  if(syncRunning){
    btn1.textContent='Stop Sync';btn1.className='btn sm danger';
    btn2.textContent='Stop Auto-Sync';btn2.className='btn danger';
    pill.textContent='Sync ON';pill.className='pill on';
    label.textContent='CONNECTED';label.style.color='var(--green)';
  }else{
    btn1.textContent='Start Sync';btn1.className='btn sm primary';
    btn2.textContent='Start Auto-Sync';btn2.className='btn';
    pill.textContent='Sync OFF';pill.className='pill';
    label.textContent='DISCONNECTED';label.style.color='var(--text3)';
  }
}

async function refreshSyncStatus(){
  const s=await api('/api/sync/status');
  if(s.error)return;
  syncRunning=s.running;
  updateSyncUI();
  document.getElementById('syncStatusPre').innerHTML=syntaxHL(s);
  document.getElementById('syncPushCount').textContent=s.pushed||0;
  document.getElementById('syncPullCount').textContent=s.pulled||0;
  document.getElementById('syncConflicts').textContent=s.conflicts||0;
}

async function updateCounts(){
  try{const rd=await api('/api/remote/docs?coll=users');document.getElementById('remoteCount').textContent=Array.isArray(rd)?rd.length:'?';}
  catch(e){document.getElementById('remoteCount').textContent='?';}
}

async function remoteInsert(){
  let doc;try{doc=JSON.parse(document.getElementById('remoteDoc').value);}catch(e){toast('Invalid JSON',false);return;}
  const res=await api('/api/remote/insert',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({coll:'users',doc})});
  if(res.error){toast(res.error,false);return;}
  toast('Inserted into remote MongoDB');
  updateCounts();
}

async function viewRemoteDocs(){
  const docs=await api('/api/remote/docs?coll=users');
  document.getElementById('remoteDocs').innerHTML=syntaxHL(docs);
}

// ═══ Oplog ═══════════════════════════════════════════════

async function refreshOplog(){
  const entries=await api('/api/oplog?coll=users&limit=100');
  const el=document.getElementById('oplogBody');
  if(!entries||entries.error||!entries.length){el.innerHTML='<div class="empty">No oplog entries</div>';return;}
  document.getElementById('oplogCountBadge').textContent=entries.length;
  el.innerHTML=entries.reverse().map(e=>{
    const ts=new Date(e.ts*1000).toLocaleTimeString();
    return '<div class="op-entry">'+
      '<span class="op-tag '+e.op+'">'+e.op+'</span>'+
      '<span style="color:var(--text3);font-family:var(--mono);font-size:10px;min-width:70px">'+ts+'</span>'+
      '<span style="color:var(--text2);font-family:var(--mono);font-size:10px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+escHTML(e.doc_id||'')+'">'+escHTML(e.doc_id||'')+'</span>'+
      '<span style="color:var(--text3);font-size:10px">v'+(e.v||0)+'</span>'+
    '</div>';
  }).join('');
}

// ═══ Seed ════════════════════════════════════════════════

async function seedData(){
  if(!confirm('This will reset all data. Continue?'))return;
  toast('Seeding sample data...');
  const res=await api('/api/seed',{method:'POST'});
  if(res.error){toast(res.error,false);return;}
  toast('Seeded '+res.count+' documents');
  refreshAll();
}

// ═══ Header Stats ════════════════════════════════════════

async function refreshHeaderStats(){
  try{
    const s=await api('/api/stats?coll=users');
    document.getElementById('hdrDocCount').textContent=s.doc_count||0;
    document.getElementById('hdrIdxCount').textContent=s.index_count||0;
  }catch(e){}
}

// ═══ Init ════════════════════════════════════════════════

function refreshAll(){
  refreshDocs();
  refreshIndexes();
  refreshOplog();
  refreshSyncStatus();
  updateCounts();
  refreshHeaderStats();
}

refreshAll();
setInterval(()=>{refreshSyncStatus();updateCounts();refreshHeaderStats();},5000);

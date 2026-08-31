let data=[], filter='all', ws=null;
const $=id=>document.getElementById(id);
function score(p){return Math.max(0,Math.min(100,50+p*4))}
function money(n){return Number(n).toLocaleString('tr-TR',{maximumFractionDigits:8})}
function render(){
 const rows=data.filter(x=>filter==='all'||(filter==='long'?x.score>=50:x.score<50)).slice(0,30);
 $('long').textContent=data.filter(x=>x.score>=50).length;
 $('short').textContent=data.filter(x=>x.score<50).length;
 $('count').textContent=data.length;
 $('list').innerHTML=rows.length?rows.map(x=>{
   const isL=x.score>=50, pct=Math.round(x.score);
   return `<article class="coin">
    <div class="row"><div><span class="symbol">${x.s}</span><div class="muted">${isL?'LONG adayı':'SHORT adayı'} • Hacim $${compact(x.q)}</div></div>
    <div style="text-align:right"><div class="price">${money(x.c)}</div><div class="${x.P>=0?'green':'red'}">${x.P>=0?'+':''}${Number(x.P).toFixed(2)}%</div></div>
    <span class="badge ${isL?'lb':'sb'}">${isL?'LONG':'SHORT'} ${pct}%</span></div>
    <div class="bar"><i style="width:${pct}%;background:${isL?'#49e49a':'#ff6476'}"></i></div>
   </article>`}).join(''):'<div class="empty">Bu filtrede uygun coin yok.</div>';
}
function compact(n){n=Number(n);if(n>=1e9)return (n/1e9).toFixed(1)+'B';if(n>=1e6)return (n/1e6).toFixed(1)+'M';if(n>=1e3)return (n/1e3).toFixed(1)+'K';return n.toFixed(0)}
function setFilter(f){
 filter=f;['all','longTab','shortTab'].forEach(id=>$(id)?.classList.remove('on'));
 $(f==='all'?'all':f==='long'?'longTab':'shortTab').classList.add('on');render();
}
function connect(){
 try{
   ws=new WebSocket('wss://fstream.binance.com/market/stream?streams=!ticker@arr');
   ws.onopen=()=>{$('status').textContent='● CANLI';$('status').classList.add('live')};
   ws.onmessage=e=>{
     try{
       const msg=JSON.parse(e.data), arr=msg.data||[];
       data=arr.filter(x=>x.s?.endsWith('USDT') && Number(x.q)>1000000)
         .map(x=>({...x,score:score(Number(x.P))}))
         .sort((a,b)=>b.score-a.score);
       render();
     }catch(_){}
   };
   ws.onclose=()=>{ $('status').textContent='YENİDEN BAĞLANIYOR'; setTimeout(connect,2000)};
   ws.onerror=()=>ws.close();
 }catch(_){setTimeout(connect,3000)}
}
render(); connect();

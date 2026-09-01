const API='https://fapi.binance.com';
const WS='wss://fstream.binance.com/market/stream?streams=!ticker@arr';

let tickers=new Map();
let signals=[];
let filter='all';
let ws=null;
let busy=false;
let lastScan=0;
let scanTimer=null;
let currentView='scan';
let openDetailSymbol=null;

const $=id=>document.getElementById(id);
const n=v=>Number(v)||0;

const fmt=v=>Number(v).toLocaleString('tr-TR',{
 maximumFractionDigits:8
});

const compact=v=>{
 v=n(v);
 if(v>=1e9)return(v/1e9).toFixed(1)+'B';
 if(v>=1e6)return(v/1e6).toFixed(1)+'M';
 if(v>=1e3)return(v/1e3).toFixed(1)+'K';
 return v.toFixed(0);
};


/* =========================
   BINANCE API
========================= */

async function api(path,p={}){

 let u=new URL(API+path);

 Object.entries(p).forEach(([k,v])=>{
  u.searchParams.set(k,v);
 });

 let r=await fetch(u,{cache:'no-store'});

 if(!r.ok){
  throw Error(r.status+' '+path);
 }

 return r.json();
}


/* =========================
   TEKNİK GÖSTERGELER
========================= */

function ema(a,p){

 if(a.length<p)return null;

 let k=2/(p+1);

 let e=a
  .slice(0,p)
  .reduce((x,y)=>x+y,0)/p;

 for(let i=p;i<a.length;i++){
  e=a[i]*k+e*(1-k);
 }

 return e;
}


function rsi(a,p=14){

 if(a.length<p+1)return 50;

 let g=0,l=0;

 for(let i=1;i<=p;i++){

  let d=a[i]-a[i-1];

  g+=d>0?d:0;
  l+=d<0?-d:0;
 }

 let ag=g/p;
 let al=l/p;

 for(let i=p+1;i<a.length;i++){

  let d=a[i]-a[i-1];

  ag=(ag*(p-1)+(d>0?d:0))/p;
  al=(al*(p-1)+(d<0?-d:0))/p;
 }

 return al===0
  ?100
  :100-100/(1+ag/al);
}


function atr(c,p=14){

 if(c.length<p+1)return 0;

 let t=[];

 for(let i=1;i<c.length;i++){

  t.push(
   Math.max(
    c[i].h-c[i].l,
    Math.abs(c[i].h-c[i-1].c),
    Math.abs(c[i].l-c[i-1].c)
   )
  );
 }

 return t
  .slice(-p)
  .reduce((a,b)=>a+b,0)/p;
}


function macd(a){

 let vals=[];

 for(let i=26;i<=a.length;i++){

  let x=a.slice(0,i);

  vals.push(
   ema(x,12)-ema(x,26)
  );
 }

 let m=vals.at(-1)||0;
 let s=ema(vals,9)||0;

 return{h:m-s};
}


function parseK(r){

 return r.map(x=>({
  o:+x[1],
  h:+x[2],
  l:+x[3],
  c:+x[4],
  v:+x[5]
 }));
}


function trend(c){

 let a=c.map(x=>x.c);

 let e20=ema(a,20);
 let e50=ema(a,50);
 let e200=ema(a,200);

 let p=a.at(-1);

 let s=
  (e20>e50?1:-1)+
  (e50>e200?1:-1)+
  (p>e20?1:-1);

 return{s,e20,e50,e200};
}


function vr(c){

 let a=c
  .slice(-21,-1)
  .map(x=>x.v);

 let avg=
  a.reduce((x,y)=>x+y,0)/
  (a.length||1);

 return avg
  ?c.at(-1).v/avg
  :1;
}


function clamp(x){
 return Math.max(0,Math.min(100,x));
}


function side(x){

 if(x>=55)return'LONG';
 if(x<=45)return'SHORT';

 return'NÖTR';
}


function candleBias(c){

 let x=c.at(-1);

 let body=Math.abs(x.c-x.o);

 let range=Math.max(
  x.h-x.l,
  1e-12
 );

 let upper=x.h-Math.max(x.o,x.c);
 let lower=Math.min(x.o,x.c)-x.l;

 if(x.c>x.o&&body/range>.55)return 1;

 if(x.c<x.o&&body/range>.55)return -1;

 if(lower/range>.45&&x.c>=x.o)return 1;

 if(upper/range>.45&&x.c<=x.o)return -1;

 return 0;
}


/* =========================
   SİNYAL ANALİZİ
========================= */

async function analyze(symbol,t){

 const[
  r5,
  r15,
  r1,
  f,
  oi,
  ls
 ]=await Promise.all([

  api('/fapi/v1/klines',{
   symbol,
   interval:'5m',
   limit:220
  }),

  api('/fapi/v1/klines',{
   symbol,
   interval:'15m',
   limit:220
  }),

  api('/fapi/v1/klines',{
   symbol,
   interval:'1h',
   limit:220
  }),

  api('/fapi/v1/fundingRate',{
   symbol,
   limit:1
  }),

  api('/fapi/v1/openInterest',{
   symbol
  }),

  api('/futures/data/globalLongShortAccountRatio',{
   symbol,
   period:'5m',
   limit:1,
   contractType:'PERPETUAL'
  })

 ]);

 let c5=parseK(r5);
 let c15=parseK(r15);
 let c1=parseK(r1);

 let a5=c5.map(x=>x.c);
 let a15=c15.map(x=>x.c);
 let a1=c1.map(x=>x.c);

 let t5=trend(c5);
 let t15=trend(c15);
 let t1=trend(c1);

 let r5v=rsi(a5);
 let r15v=rsi(a15);
 let r1v=rsi(a1);

 let m5=macd(a5);
 let m15=macd(a15);
 let m1=macd(a1);

 let volume=vr(c5);
 let A=atr(c5);

 let fund=n(f?.[0]?.fundingRate);
 let oiNow=n(oi?.openInterest);
 let lsr=n(ls?.[0]?.longShortRatio);

 let price=n(t?.c)||a5.at(-1);

 let e1=ema(a1,20);
 let e150=ema(a1,50);

 let longPts=0;
 let shortPts=0;

 if(t1.s>0)longPts+=20;
 else shortPts+=20;

 if(t15.s>0)longPts+=15;
 else shortPts+=15;

 if(t5.s>0)longPts+=10;
 else shortPts+=10;

 if(r1v>=52&&r1v<=70)
  longPts+=10;
 else if(r1v>=30&&r1v<=48)
  shortPts+=10;

 if(r15v>=52&&r15v<=72)
  longPts+=7;
 else if(r15v>=28&&r15v<=48)
  shortPts+=7;

 if(m1.h>0)
  longPts+=8;
 else if(m1.h<0)
  shortPts+=8;

 if(m15.h>0)
  longPts+=7;
 else if(m15.h<0)
  shortPts+=7;

 if(price>e1&&e1>e150)
  longPts+=8;
 else if(price<e1&&e1<e150)
  shortPts+=8;

 if(volume>=1.25){

  if(t5.s>0)longPts+=7;
  else if(t5.s<0)shortPts+=7;

 }

 if(fund>.0005)
  shortPts+=4;
 else if(fund<-.0005)
  longPts+=4;

 if(lsr>1.25)
  shortPts+=4;
 else if(lsr&&lsr<.8)
  longPts+=4;

 let cb=candleBias(c5);

 if(cb>0)longPts+=4;
 else if(cb<0)shortPts+=4;

 let score=clamp(
  Math.round(
   50+(longPts-shortPts)*.5
  )
 );

 let s=side(score);

 let alignedLong=
  t1.s>0&&
  t15.s>0&&
  t5.s>0&&
  cb>=0&&
  m5.h>=0;

 let alignedShort=
  t1.s<0&&
  t15.s<0&&
  t5.s<0&&
  cb<=0&&
  m5.h<=0;

 let confirmation=
  s==='LONG'&&alignedLong
   ?'LONG TEYİT EDİLDİ'
   :s==='SHORT'&&alignedShort
   ?'SHORT TEYİT EDİLDİ'
   :s==='NÖTR'
   ?'İZLE'
   :'TEYİT BEKLENİYOR';

 let risk=Math.max(
  A*1.5,
  price*.004
 );

 let sl,tp1,tp2,tp3;

 if(s==='SHORT'){

  sl=price+risk;
  tp1=price-risk;
  tp2=price-risk*2;
  tp3=price-risk*3;

 }else{

  sl=price-risk;
  tp1=price+risk;
  tp2=price+risk*2;
  tp3=price+risk*3;
 }

 let atrPct=price?A/price*100:0;

 let lev=
  atrPct>3
   ?2
   :atrPct>1.5
   ?3
   :5;

 let rr1=
  Math.abs(tp1-price)/
  Math.max(Math.abs(price-sl),1e-12);

 let rr2=
  Math.abs(tp2-price)/
  Math.max(Math.abs(price-sl),1e-12);

 let rr3=
  Math.abs(tp3-price)/
  Math.max(Math.abs(price-sl),1e-12);

 return{

  symbol,
  price,
  change:n(t?.P),
  quote:n(t?.q),

  score,
  side:s,
  confidence:score,
  confirmation,

  entry:price,
  sl,
  tp1,
  tp2,
  tp3,

  rr1,
  rr2,
  rr3,

  lev,
  atrPct,

  rsi1:r1v,
  rsi15:r15v,
  rsi5:r5v,

  vr:volume,

  funding:fund,
  oi:oiNow,
  lsr,

  trend1:t1.s,
  trend15:t15.s,
  trend5:t5.s,

  updated:Date.now()
 };
}


/* =========================
   PAPER TRADING STORAGE
========================= */

function getPaperTrades(){

 try{

  return JSON.parse(
   localStorage.getItem('paperHistory')||'[]'
  );

 }catch(_){

  return[];

 }
}


function savePaperTrades(h){

 localStorage.setItem(
  'paperHistory',
  JSON.stringify(h.slice(0,100))
 );
}


function getOpenPaperTrades(){

 return getPaperTrades()
  .filter(x=>x.status==='Açık');
}


/* =========================
   PAPER PNL
========================= */

function calculatePnL(trade,price){

 let entry=n(trade.entry);
 let current=n(price);
 let capital=n(trade.capital);
 let lev=n(trade.lev)||1;

 if(!entry||!current)return{
  pnl:0,
  pnlPct:0,
  notional:capital*lev
 };

 let move=
  trade.side==='LONG'
   ?(current-entry)/entry
   :(entry-current)/entry;

 let pnlPct=move*100;
 let pnl=capital*lev*move;

 return{
  pnl,
  pnlPct,
  notional:capital*lev
 };
}


/* =========================
   AÇIK POZİSYON KONTROLÜ
========================= */

function checkPaperPositions(){

 let history=getPaperTrades();

 let changed=false;

 history.forEach(trade=>{

  if(trade.status!=='Açık')return;

  let ticker=tickers.get(trade.symbol);

  if(!ticker)return;

  let price=n(ticker.c);

  if(!price)return;

  let hit=null;

  if(trade.side==='LONG'){

   if(price<=n(trade.sl))
    hit='SL';

   else if(price>=n(trade.tp1))
    hit='TP1';

  }else if(trade.side==='SHORT'){

   if(price>=n(trade.sl))
    hit='SL';

   else if(price<=n(trade.tp1))
    hit='TP1';
  }

  if(hit){

   closePaperTradeInternal(
    trade,
    price,
    hit,
    history
   );

   changed=true;
  }

 });

 if(changed){

  savePaperTrades(history);

  if(currentView==='trade')
   populateTrade();

  if(currentView==='history')
   renderHistory();
 }

}


function closePaperTradeInternal(
 trade,
 price,
 reason,
 history
){

 let result=calculatePnL(
  trade,
  price
 );

 trade.status='Kapandı';

 trade.exit=n(price);

 trade.exitReason=reason;

 trade.pnl=result.pnl;

 trade.pnlPct=result.pnlPct;

 trade.closedAt=
  new Date().toISOString();

 trade.duration=
  Math.max(
   0,
   Date.now()-new Date(trade.time).getTime()
  );
}


/* =========================
   MANUEL POZİSYON KAPATMA
========================= */

function closePaperTrade(id){

 let history=getPaperTrades();

 let trade=
  history.find(
   x=>String(x.id)===String(id)
  );

 if(!trade||trade.status!=='Açık')
  return;

 let ticker=tickers.get(trade.symbol);

 let price=
  ticker
   ?n(ticker.c)
   :n(trade.entry);

 let result=calculatePnL(
  trade,
  price
 );

 let ok=confirm(
  trade.symbol+
  ' '+trade.side+
  '\n\n'+
  'Güncel fiyat: '+fmt(price)+
  '\nPnL: '+
  (result.pnl>=0?'+':'')+
  fmt(result.pnl)+
  ' USDT'+
  '\n\nPozisyon kapatılsın mı?'
 );

 if(!ok)return;

 closePaperTradeInternal(
  trade,
  price,
  'Manuel',
  history
 );

 savePaperTrades(history);

 renderHistory();
 populateTrade();

 alert(
  'Paper pozisyon kapatıldı.\n'+
  'PnL: '+
  (result.pnl>=0?'+':'')+
  fmt(result.pnl)+
  ' USDT'
 );
}


/* =========================
   TARAMA RENDER
========================= */

function render(){

 let rows=signals.filter(
  x=>
   filter==='all'||
   (filter==='long'&&x.side==='LONG')||
   (filter==='short'&&x.side==='SHORT')
 );

 $('long').textContent=
  signals.filter(
   x=>x.side==='LONG'&&x.score>=65
  ).length;

 $('short').textContent=
  signals.filter(
   x=>x.side==='SHORT'&&x.score<=35
  ).length;

 $('count').textContent=tickers.size;

 let min=Number(
  localStorage.getItem('minScore')||65
 );

 rows=rows.filter(
  x=>
   x.side==='NÖTR'||
   x.score>=min||
   100-x.score>=min
 );

 if(!rows.length){

  $('list').innerHTML=
   '<div class="empty">Henüz güçlü sinyal yok. Tarama devam ediyor…</div>';

  return;
 }

 $('list').innerHTML=
  rows.slice(0,12).map(x=>{

   let cls=
    x.side==='LONG'
     ?'lb'
     :x.side==='SHORT'
     ?'sb'
     :'watch';

   let bar=
    x.side==='LONG'
     ?x.score
     :x.side==='SHORT'
     ?100-x.score
     :50;

   return `

<article class="coin">

<div class="row">

<div>

<div class="symbol">${x.symbol}</div>

<div class="muted">
${x.confirmation} •
Hacim $${compact(x.quote)}
</div>

</div>

<div style="text-align:right">

<div class="price">
${fmt(x.price)}
</div>

<div class="${x.change>=0?'green':'red'}">
${x.change>=0?'+':''}${x.change.toFixed(2)}%
</div>

</div>

<span class="badge ${cls}">
${x.side} • ${x.score}/100
</span>

</div>

<div class="bar">
<i style="
width:${bar}%;
background:${
 x.side==='LONG'
 ?'#49e49a'
 :x.side==='SHORT'
 ?'#ff6678'
 :'#f4cf62'
}">
</i>
</div>

<div class="meta">

<span>RSI 1H ${x.rsi1.toFixed(0)}</span>

<span>RSI 15M ${x.rsi15.toFixed(0)}</span>

<span>Hacim x${x.vr.toFixed(1)}</span>

<span>Funding ${(x.funding*100).toFixed(3)}%</span>

</div>

<button
class="action"
onclick="toggleDetail('${x.symbol}')">
İşlem planını göster
</button>

<div
id="d-${x.symbol}"
class="detail">

<div class="muted">

1 saatlik plan •
Teknik Güç ${x.score}/100 •
<b>${x.confirmation}</b> •
${x.lev}x

</div>

<div class="plan">

<div class="box">
<span>GİRİŞ</span>
<b>${fmt(x.entry)}</b>
</div>

<div class="box">
<span>SL</span>
<b class="red">${fmt(x.sl)}</b>
</div>

<div class="box">
<span>TP1</span>
<b class="green">${fmt(x.tp1)}</b>
</div>

<div class="box">
<span>TP2</span>
<b class="green">${fmt(x.tp2)}</b>
</div>

</div>

<div class="grid">

<div class="box">
<span>TP3</span>
<b class="green">${fmt(x.tp3)}</b>
</div>

<div class="box">
<span>R/R TP1</span>
<b>${x.rr1.toFixed(2)}</b>
</div>

<div class="box">
<span>R/R TP2</span>
<b>${x.rr2.toFixed(2)}</b>
</div>

<div class="box">
<span>R/R TP3</span>
<b>${x.rr3.toFixed(2)}</b>
</div>

</div>

<div class="grid">

<div class="box">
<span>ATR</span>
<b>${x.atrPct.toFixed(2)}%</b>
</div>

<div class="box">
<span>OPEN INTEREST</span>
<b>${compact(x.oi)}</b>
</div>

<div class="box">
<span>LONG/SHORT</span>
<b>${x.lsr?x.lsr.toFixed(2):'—'}</b>
</div>

<div class="box">
<span>5M RSI</span>
<b>${x.rsi5.toFixed(0)}</b>
</div>

</div>

<div class="meta">

<span>5M ${x.trend5>0?'↑':'↓'}</span>

<span>15M ${x.trend15>0?'↑':'↓'}</span>

<span>1H ${x.trend1>0?'↑':'↓'}</span>

<span>${new Date(x.updated).toLocaleTimeString('tr-TR')}</span>

</div>

<button
class="action"
onclick="prepareTrade('${x.symbol}')">

Bu sinyalle İşlem'e git

</button>

</div>

</article>

`;

 }).join('');

}


/* =========================
   DETAY
========================= */

function toggleDetail(s){

 const el=document.getElementById('d-'+s);

 if(!el)return;

 const wasOpen=el.classList.contains('open');

 document
  .querySelectorAll('.detail.open')
  .forEach(x=>x.classList.remove('open'));

 if(wasOpen){

  openDetailSymbol=null;

 }else{

  el.classList.add('open');

  openDetailSymbol=s;
 }
}


/* =========================
   FİLTRE
========================= */

function setFilter(f){

 filter=f;

 [
  'all',
  'longTab',
  'shortTab'
 ].forEach(id=>
  $(id)?.classList.remove('on')
 );

 $(
  f==='all'
   ?'all'
   :f==='long'
   ?'longTab'
   :'shortTab'
 ).classList.add('on');

 render();
}


/* =========================
   SAYFA GEÇİŞLERİ
========================= */

function showView(v){

 currentView=v;

 [
  'scan',
  'markets',
  'trade',
  'history',
  'settings'
 ].forEach(x=>{

  let el=$(x+'View');

  if(el){

   el.classList.toggle(
    'hidden',
    x!==v
   );
  }

 });

 document
  .querySelectorAll('.navbtn')
  .forEach(b=>
   b.classList.toggle(
    'active',
    b.dataset.view===v
   )
  );

 if(v==='markets')
  renderMarkets();

 if(v==='trade')
  populateTrade();

 if(v==='history')
  renderHistory();

 if(v==='settings'){

  if($('minScore'))
   $('minScore').value=
    localStorage.getItem('minScore')||65;

  if($('scanSeconds'))
   $('scanSeconds').value=
    localStorage.getItem('scanSeconds')||90;
 }
}


/* =========================
   PİYASALAR
========================= */

function renderMarkets(){

 let a=
  [...tickers.values()]
   .filter(
    x=>
     x.s.endsWith('USDT')&&
     n(x.q)>1000000
   )
   .sort(
    (a,b)=>n(b.q)-n(a.q)
   )
   .slice(0,30);

 if(!$('marketList'))return;

 $('marketList').innerHTML=
  a.length
   ?a.map(x=>`

<div class="marketrow">

<b>${x.s}</b>

<span>${fmt(x.c)}</span>

<span class="${n(x.P)>=0?'green':'red'}">

${n(x.P)>=0?'+':''}${n(x.P).toFixed(2)}%

</span>

</div>

`).join('')

   :'<div class="empty">Canlı piyasa verisi bekleniyor…</div>';
}


/* =========================
   TRADE SAYFASI
========================= */

function populateTrade(){

 if(!$('tradeCoin'))return;

 let list=
  signals.filter(
   x=>x.side!=='NÖTR'
  );

 $('tradeCoin').innerHTML=
  list.length
   ?list.map(x=>`

<option value="${x.symbol}">
${x.symbol} • ${x.side} ${x.score}/100
</option>

`).join('')

   :'<option value="">Henüz sinyal yok</option>';

 applyTradeFromSelection();

 renderOpenPositions();
}


function applyTradeFromSelection(){

 if(!$('tradeCoin'))return;

 let x=
  signals.find(
   s=>s.symbol===$('tradeCoin').value
  );

 if(!x)return;

 if($('tradeSide'))
  $('tradeSide').value=x.side;

 if($('tradeLev'))
  $('tradeLev').value=x.lev;

 if($('tradeEntry'))
  $('tradeEntry').value=x.entry;

 if($('tradeSL'))
  $('tradeSL').value=x.sl;

 if($('tradeTP1'))
  $('tradeTP1').value=x.tp1;

 calcTrade();
}


function prepareTrade(s){

 showView('trade');

 if($('tradeCoin'))
  $('tradeCoin').value=s;

 applyTradeFromSelection();
}


/* =========================
   TRADE HESAPLAMA
========================= */

function calcTrade(){

 if(!$('tradeCapital'))return;

 let cap=n($('tradeCapital').value);
 let entry=n($('tradeEntry').value);
 let sl=n($('tradeSL').value);
 let tp=n($('tradeTP1').value);
 let lev=n($('tradeLev').value);

 if(!cap||!entry||!sl||!tp)return;

 let riskPct=
  Math.abs(entry-sl)/
  entry*100;

 let rr=
  Math.abs(tp-entry)/
  Math.max(Math.abs(entry-sl),1e-12);

 let notional=cap*lev;

 if($('calcResult')){

  $('calcResult').innerHTML=`

<div class="box">

Fiyat riski:
<b>${riskPct.toFixed(2)}%</b>

•

TP1 R/R:
<b>${rr.toFixed(2)}</b>

•

Pozisyon notional:
<b>${fmt(notional)} USDT</b>

</div>

`;
 }
}


/* =========================
   PAPER İŞLEM AÇ
========================= */

function savePaperTrade(){

 if(!$('tradeCoin'))return;

 let symbol=$('tradeCoin').value;

 if(!symbol){

  alert('Önce bir sinyal seçmelisin.');

  return;
 }

 let capital=n(
  $('tradeCapital')?.value
 );

 if(!capital){

  alert('Paper sermayesini gir.');

  return;
 }

 let rec={

  id:Date.now(),

  symbol,

  side:$('tradeSide').value,

  entry:n($('tradeEntry').value),

  sl:n($('tradeSL').value),

  tp1:n($('tradeTP1').value),

  lev:n($('tradeLev').value),

  capital,

  time:new Date().toISOString(),

  status:'Açık',

  pnl:0,

  pnlPct:0,

  exit:null,

  exitReason:null,

  closedAt:null
 };

 let h=getPaperTrades();

 h.unshift(rec);

 savePaperTrades(h);

 renderOpenPositions();
 renderHistory();

 alert(
  'Paper işlem açıldı.\n'+
  'Gerçek Binance emri gönderilmedi.'
 );
}


/* =========================
   AÇIK POZİSYONLAR
========================= */

function renderOpenPositions(){

 if(!$('openPositions'))return;

 let open=getOpenPaperTrades();

 if(!open.length){

  $('openPositions').innerHTML=
   '<div class="empty">Açık paper pozisyon bulunmuyor.</div>';

  return;
 }

 $('openPositions').innerHTML=
  open.map(trade=>{

   let ticker=tickers.get(trade.symbol);

   let current=
    ticker
     ?n(ticker.c)
     :n(trade.entry);

   let result=
    calculatePnL(
     trade,
     current
    );

   let pnlClass=
    result.pnl>=0
     ?'green'
     :'red';

   return`

<div class="coin">

<div class="row">

<div>

<div class="symbol">

${trade.symbol}

</div>

<div class="muted">

PAPER • ${trade.side} • ${trade.lev}x

</div>

</div>

<div style="text-align:right">

<div class="price">

${fmt(current)}

</div>

<div class="${pnlClass}">

${result.pnl>=0?'+':''}${fmt(result.pnl)} USDT

</div>

</div>

</div>

<div class="grid">

<div class="box">

<span>GİRİŞ</span>

<b>${fmt(trade.entry)}</b>

</div>

<div class="box">

<span>ANLIK PNL</span>

<b class="${pnlClass}">

${result.pnlPct>=0?'+':''}${result.pnlPct.toFixed(2)}%

</b>

</div>

<div class="box">

<span>SL</span>

<b class="red">${fmt(trade.sl)}</b>

</div>

<div class="box">

<span>TP1</span>

<b class="green">${fmt(trade.tp1)}</b>

</div>

</div>

<div class="meta">

<span>

Sermaye ${fmt(trade.capital)} USDT

</span>

<span>

Notional ${fmt(result.notional)} USDT

</span>

</div>

<button
class="action"
onclick="closePaperTrade('${trade.id}')">

⛔ Pozisyonu Kapat

</button>

</div>

`;

  }).join('');
}


/* =========================
   GEÇMİŞ
========================= */

function renderHistory(){

 if(!$('historyList'))return;

 let h=getPaperTrades();

 $('historyList').innerHTML=
  h.length
   ?h.map(x=>{

    let pnl=
     n(x.pnl);

    let pnlClass=
     x.status==='Açık'
      ?'yellow'
      :pnl>=0
      ?'green'
      :'red';

    let exitText=
     x.status==='Açık'
      ?'Pozisyon açık'
      :'Çıkış '+fmt(x.exit)+
       ' • '+x.exitReason;

    return`

<div class="history">

<b>
${x.symbol} ${x.side}
</b>

<div class="muted">

Giriş ${fmt(x.entry)}
•
SL ${fmt(x.sl)}
•
TP1 ${fmt(x.tp1)}
•
${x.lev}x

</div>

<div class="muted">

${new Date(x.time).toLocaleString('tr-TR')}

•
${exitText}

</div>

<div class="${pnlClass}">

${x.status==='Açık'
 ?'AÇIK'
 :(pnl>=0?'+':'')+
 fmt(pnl)+
 ' USDT ('+
 (n(x.pnlPct)>=0?'+':'')+
 n(x.pnlPct).toFixed(2)+
 '%)'
}

</div>

</div>

`;

   }).join('')

   :'<div class="empty">Henüz paper işlem yok.</div>';
}


/* =========================
   AYARLAR
========================= */

function saveSettings(){

 let m=Math.min(
  95,
  Math.max(
   50,
   n($('minScore').value)||65
  )
 );

 let s=Math.min(
  600,
  Math.max(
   30,
   n($('scanSeconds').value)||90
  )
 );

 localStorage.setItem(
  'minScore',
  m
 );

 localStorage.setItem(
  'scanSeconds',
  s
 );

 if(scanTimer)
  clearInterval(scanTimer);

 scanTimer=setInterval(
  scan,
  s*1000
 );

 render();

 alert('Ayarlar kaydedildi.');
}


function clearHistory(){

 if(
  confirm(
   'Paper işlem geçmişi silinsin mi?'
  )
 ){

  localStorage.removeItem(
   'paperHistory'
  );

  renderHistory();
  renderOpenPositions();
 }
}


/* =========================
   TARAMA
========================= */

async function scan(){

 if(
  busy||
  !tickers.size
 )return;

 busy=true;

 if($('refresh'))
  $('refresh').textContent=
   'V6 teknik motoru hesaplıyor…';

 try{

  let top=
   [...tickers.values()]
    .filter(
     x=>
      x.s.endsWith('USDT')&&
      n(x.q)>1000000&&
      n(x.c)>0
    )
    .sort(
     (a,b)=>n(b.q)-n(a.q)
    )
    .slice(0,8);

  let out=[];

  for(
   let i=0;
   i<top.length;
   i+=2
  ){

   let r=
    await Promise.all(
     top
      .slice(i,i+2)
      .map(
       x=>
        analyze(x.s,x)
         .catch(()=>null)
      )
    );

   out.push(
    ...r.filter(Boolean)
   );

   await new Promise(
    r=>setTimeout(r,250)
   );
  }

  signals=
   out.sort(
    (a,b)=>
     Math.abs(b.score-50)-
     Math.abs(a.score-50)
   );

  lastScan=Date.now();

  render();

  if(currentView==='markets')
   renderMarkets();

  if(currentView==='trade')
   populateTrade();

  if($('refresh'))
   $('refresh').textContent=
    'Son tarama '+
    new Date(lastScan)
     .toLocaleTimeString('tr-TR')+
    ' • V6 • 8 yüksek hacimli coin';

 }catch(e){

  if($('refresh'))
   $('refresh').textContent=
    'Tarama hatası: '+e.message;

 }finally{

  busy=false;
 }
}


/* =========================
   BINANCE WEBSOCKET
========================= */

function connect(){

 try{

  ws=new WebSocket(WS);

  ws.onopen=()=>{

   if($('status')){

    $('status').textContent=
     '● CANLI';

    $('status')
     .classList.add('live');
   }
  };


  ws.onmessage=e=>{

   try{

    let a=
     JSON.parse(e.data).data||[];

    a.forEach(x=>
     tickers.set(x.s,x)
    );


    /*
      ÖNEMLİ:
      Ana sinyal listesini burada render etmiyoruz.

      Böylece kullanıcı işlem planını açtığında
      her Binance tick'inde DOM yeniden oluşmuyor.
    */


    checkPaperPositions();

    if(currentView==='markets')
     renderMarkets();

    if(currentView==='trade')
     renderOpenPositions();

    if(
     Date.now()-lastScan>90000
    )
     scan();

   }catch(_){}
  };


  ws.onclose=()=>{

   if($('status'))
    $('status').textContent=
     'YENİDEN BAĞLANIYOR';

   setTimeout(
    connect,
    2000
   );
  };


  ws.onerror=()=>ws.close();

 }catch(_){

  setTimeout(
   connect,
   3000
  );
 }
}


/* =========================
   INPUT EVENTLERİ
========================= */

if($('tradeCoin')){

 $('tradeCoin')
  .addEventListener(
   'change',
   applyTradeFromSelection
  );
}


[
 'tradeEntry',
 'tradeSL',
 'tradeTP1',
 'tradeLev',
 'tradeCapital'
].forEach(id=>{

 if($(id)){

  $(id).addEventListener(
   'input',
   calcTrade
  );
 }
});


/* =========================
   BAŞLAT
========================= */

render();

connect();

scan();

scanTimer=
 setInterval(
  scan,
  (n(
   localStorage.getItem('scanSeconds')
  )||90)*1000
 );


/*
  Paper pozisyonlarını ayrıca
  düzenli kontrol ediyoruz.

  WebSocket zaten fiyatları güncelliyor;
  bu interval güvenlik yedek kontrolüdür.
*/

setInterval(
 ()=>{

  checkPaperPositions();

  if(currentView==='trade')
   renderOpenPositions();

 },
 1000
);

/* =========================================================
   FUTURES SIGNAL SCANNER V8
   Binance Futures • Live Scanner • Paper Trading Engine
   ========================================================= */

const API='https://fapi.binance.com';
const WS='wss://fstream.binance.com/market/stream?streams=!ticker@arr';

let tickers=new Map();
let signals=[];
let filter='all';

let ws=null;
let busy=false;
let lastScan=0;

let scanTimer=null;
let positionTimer=null;
let currentView='scan';
let openDetailSymbol=null;


/* =========================================================
   YARDIMCI
   ========================================================= */

const $=id=>document.getElementById(id);

const n=v=>{
    const x=Number(v);
    return Number.isFinite(x)?x:0;
};


const fmt=v=>{
    return n(v).toLocaleString('tr-TR',{
        maximumFractionDigits:8
    });
};


const compact=v=>{

    v=n(v);

    if(v>=1e12)
        return(v/1e12).toFixed(1)+'T';

    if(v>=1e9)
        return(v/1e9).toFixed(1)+'B';

    if(v>=1e6)
        return(v/1e6).toFixed(1)+'M';

    if(v>=1e3)
        return(v/1e3).toFixed(1)+'K';

    return v.toFixed(0);
};


const sleep=ms=>
    new Promise(resolve=>setTimeout(resolve,ms));


function escapeHtml(v){

    return String(v??'')
        .replaceAll('&','&amp;')
        .replaceAll('<','&lt;')
        .replaceAll('>','&gt;')
        .replaceAll('"','&quot;')
        .replaceAll("'","&#039;");
}


/* =========================================================
   API
   ========================================================= */

async function api(path,p={}){

    const u=new URL(API+path);

    Object.entries(p).forEach(([k,v])=>{
        u.searchParams.set(k,v);
    });

    const r=await fetch(u,{
        cache:'no-store'
    });

    if(!r.ok){

        throw Error(
            r.status+' '+path
        );
    }

    return r.json();
}


/* =========================================================
   TEKNİK GÖSTERGELER
   ========================================================= */

function ema(a,p){

    if(!Array.isArray(a)||a.length<p)
        return null;

    const k=2/(p+1);

    let e=
        a.slice(0,p)
        .reduce((x,y)=>x+y,0)/p;

    for(let i=p;i<a.length;i++){

        e=
            a[i]*k+
            e*(1-k);
    }

    return e;
}


function rsi(a,p=14){

    if(!Array.isArray(a)||a.length<p+1)
        return 50;

    let g=0;
    let l=0;

    for(let i=1;i<=p;i++){

        const d=
            a[i]-a[i-1];

        g+=d>0?d:0;
        l+=d<0?-d:0;
    }

    let ag=g/p;
    let al=l/p;

    for(let i=p+1;i<a.length;i++){

        const d=
            a[i]-a[i-1];

        ag=
            (ag*(p-1)+(d>0?d:0))/p;

        al=
            (al*(p-1)+(d<0?-d:0))/p;
    }

    if(al===0)
        return 100;

    return 100-100/(1+ag/al);
}


function atr(c,p=14){

    if(!Array.isArray(c)||c.length<p+1)
        return 0;

    const t=[];

    for(let i=1;i<c.length;i++){

        t.push(
            Math.max(
                c[i].h-c[i].l,
                Math.abs(
                    c[i].h-c[i-1].c
                ),
                Math.abs(
                    c[i].l-c[i-1].c
                )
            )
        );
    }

    return t
        .slice(-p)
        .reduce((a,b)=>a+b,0)/p;
}


function macd(a){

    if(a.length<35)
        return{h:0};

    const vals=[];

    for(let i=26;i<=a.length;i++){

        const x=a.slice(0,i);

        const fast=ema(x,12);
        const slow=ema(x,26);

        vals.push(
            (fast||0)-(slow||0)
        );
    }

    const m=vals.at(-1)||0;
    const s=ema(vals,9)||0;

    return{
        h:m-s
    };
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

    const a=c.map(x=>x.c);

    const e20=ema(a,20);
    const e50=ema(a,50);
    const e200=ema(a,200);

    const p=a.at(-1);

    if(
        !e20||
        !e50||
        !e200||
        !p
    ){

        return{
            s:0,
            e20,
            e50,
            e200
        };
    }

    const s=
        (e20>e50?1:-1)+
        (e50>e200?1:-1)+
        (p>e20?1:-1);

    return{
        s,
        e20,
        e50,
        e200
    };
}


function vr(c){

    const a=
        c.slice(-21,-1)
        .map(x=>x.v);

    const avg=
        a.reduce((x,y)=>x+y,0)/
        (a.length||1);

    return avg
        ?c.at(-1).v/avg
        :1;
}


function clamp(x){

    return Math.max(
        0,
        Math.min(100,x)
    );
}


function side(x){

    if(x>=55)
        return'LONG';

    if(x<=45)
        return'SHORT';

    return'NÖTR';
}


function candleBias(c){

    const x=c.at(-1);

    if(!x)
        return 0;

    const body=
        Math.abs(x.c-x.o);

    const range=
        Math.max(
            x.h-x.l,
            1e-12
        );

    const upper=
        x.h-Math.max(x.o,x.c);

    const lower=
        Math.min(x.o,x.c)-x.l;

    if(
        x.c>x.o&&
        body/range>.55
    )
        return 1;

    if(
        x.c<x.o&&
        body/range>.55
    )
        return -1;

    if(
        lower/range>.45&&
        x.c>=x.o
    )
        return 1;

    if(
        upper/range>.45&&
        x.c<=x.o
    )
        return -1;

    return 0;
}


/* =========================================================
   SİNYAL ANALİZİ
   ========================================================= */

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

        api(
            '/futures/data/globalLongShortAccountRatio',
            {
                symbol,
                period:'5m',
                limit:1,
                contractType:'PERPETUAL'
            }
        )

    ]);


    const c5=parseK(r5);
    const c15=parseK(r15);
    const c1=parseK(r1);

    const a5=c5.map(x=>x.c);
    const a15=c15.map(x=>x.c);
    const a1=c1.map(x=>x.c);

    const t5=trend(c5);
    const t15=trend(c15);
    const t1=trend(c1);

    const r5v=rsi(a5);
    const r15v=rsi(a15);
    const r1v=rsi(a1);

    const m5=macd(a5);
    const m15=macd(a15);
    const m1=macd(a1);

    const volume=vr(c5);

    const A=atr(c5);

    const fund=
        n(f?.[0]?.fundingRate);

    const oiNow=
        n(oi?.openInterest);

    const lsr=
        n(ls?.[0]?.longShortRatio);

    const price=
        n(t?.c)||
        a5.at(-1);

    const e1=ema(a1,20);
    const e150=ema(a1,50);

    let longPts=0;
    let shortPts=0;


    /* TREND */

    if(t1.s>0)
        longPts+=20;
    else
        shortPts+=20;


    if(t15.s>0)
        longPts+=15;
    else
        shortPts+=15;


    if(t5.s>0)
        longPts+=10;
    else
        shortPts+=10;


    /* RSI */

    if(
        r1v>=52&&
        r1v<=70
    )
        longPts+=10;

    else if(
        r1v>=30&&
        r1v<=48
    )
        shortPts+=10;


    if(
        r15v>=52&&
        r15v<=72
    )
        longPts+=7;

    else if(
        r15v>=28&&
        r15v<=48
    )
        shortPts+=7;


    /* MACD */

    if(m1.h>0)
        longPts+=8;

    else if(m1.h<0)
        shortPts+=8;


    if(m15.h>0)
        longPts+=7;

    else if(m15.h<0)
        shortPts+=7;


    /* EMA */

    if(
        price>e1&&
        e1>e150
    )
        longPts+=8;

    else if(
        price<e1&&
        e1<e150
    )
        shortPts+=8;


    /* VOLUME */

    if(volume>=1.25){

        if(t5.s>0)
            longPts+=7;

        else if(t5.s<0)
            shortPts+=7;
    }


    /* FUNDING */

    if(fund>.0005)
        shortPts+=4;

    else if(fund<-.0005)
        longPts+=4;


    /* LONG SHORT */

    if(lsr>1.25)
        shortPts+=4;

    else if(
        lsr&&
        lsr<.8
    )
        longPts+=4;


    /* CANDLE */

    const cb=candleBias(c5);

    if(cb>0)
        longPts+=4;

    else if(cb<0)
        shortPts+=4;


    /* SCORE */

    const score=
        clamp(
            Math.round(
                50+
                (longPts-shortPts)*.5
            )
        );


    const s=side(score);


    /* CONFIRMATION */

    const alignedLong=
        t1.s>0&&
        t15.s>0&&
        t5.s>0&&
        cb>=0&&
        m5.h>=0;


    const alignedShort=
        t1.s<0&&
        t15.s<0&&
        t5.s<0&&
        cb<=0&&
        m5.h<=0;


    const confirmation=
        s==='LONG'&&alignedLong
            ?'LONG TEYİT EDİLDİ'
            :s==='SHORT'&&alignedShort
            ?'SHORT TEYİT EDİLDİ'
            :s==='NÖTR'
            ?'İZLE'
            :'TEYİT BEKLENİYOR';


    /* RISK */

    const risk=
        Math.max(
            A*1.5,
            price*.004
        );


    let sl;
    let tp1;
    let tp2;
    let tp3;


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


    const atrPct=
        price
            ?A/price*100
            :0;


    const lev=
        atrPct>3
            ?2
            :atrPct>1.5
            ?3
            :5;


    const denominator=
        Math.max(
            Math.abs(price-sl),
            1e-12
        );


    const rr1=
        Math.abs(tp1-price)/
        denominator;


    const rr2=
        Math.abs(tp2-price)/
        denominator;


    const rr3=
        Math.abs(tp3-price)/
        denominator;


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


/* =========================================================
   SİNYAL LİSTESİ
   ========================================================= */

function render(){

    const list=$('list');

    if(!list)
        return;


    let rows=
        signals.filter(x=>
            filter==='all'||
            (
                filter==='long'&&
                x.side==='LONG'
            )||
            (
                filter==='short'&&
                x.side==='SHORT'
            )
        );


    const min=
        Number(
            localStorage.getItem(
                'minScore'
            )||65
        );


    if($('long')){

        $('long').textContent=
            signals.filter(
                x=>
                    x.side==='LONG'&&
                    x.score>=min
            ).length;
    }


    if($('short')){

        $('short').textContent=
            signals.filter(
                x=>
                    x.side==='SHORT'&&
                    100-x.score>=min
            ).length;
    }


    if($('count'))
        $('count').textContent=
            tickers.size;


    rows=
        rows.filter(
            x=>
                x.side==='NÖTR'||
                x.score>=min||
                100-x.score>=min
        );


    if(!rows.length){

        list.innerHTML=
            '<div class="empty">'+
            'Henüz güçlü sinyal yok. '+
            'Tarama devam ediyor…'+
            '</div>';

        return;
    }


    list.innerHTML=
        rows
        .slice(0,12)
        .map(x=>{

            const cls=
                x.side==='LONG'
                    ?'lb'
                    :x.side==='SHORT'
                    ?'sb'
                    :'watch';


            const bar=
                x.side==='LONG'
                    ?x.score
                    :x.side==='SHORT'
                    ?100-x.score
                    :50;


            const barColor=
                x.side==='LONG'
                    ?'#49e49a'
                    :x.side==='SHORT'
                    ?'#ff6678'
                    :'#f4cf62';


            return`

<article class="coin">

<div class="row">

<div>

<div class="symbol">
${escapeHtml(x.symbol)}
</div>

<div class="muted">
${escapeHtml(x.confirmation)}
•
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
background:${barColor}
"></i>

</div>


<div class="meta">

<span>
RSI 1H ${x.rsi1.toFixed(0)}
</span>

<span>
RSI 15M ${x.rsi15.toFixed(0)}
</span>

<span>
Hacim x${x.vr.toFixed(1)}
</span>

<span>
Funding ${(x.funding*100).toFixed(3)}%
</span>

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

<b>${escapeHtml(x.confirmation)}</b>
•
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

<span>
5M ${x.trend5>0?'↑':'↓'}
</span>

<span>
15M ${x.trend15>0?'↑':'↓'}
</span>

<span>
1H ${x.trend1>0?'↑':'↓'}
</span>

<span>
${new Date(x.updated)
    .toLocaleTimeString('tr-TR')}
</span>

</div>


<button
class="action"
onclick="prepareTrade('${x.symbol}')">

Bu sinyalle İşlem'e git

</button>


</div>

</article>

`;

        })
        .join('');


    if(openDetailSymbol){

        const d=
            document.getElementById(
                'd-'+openDetailSymbol
            );

        if(d)
            d.classList.add('open');
    }
}


function toggleDetail(symbol){

    const el=
        document.getElementById(
            'd-'+symbol
        );

    if(!el)
        return;


    const wasOpen=
        el.classList.contains('open');


    document
        .querySelectorAll('.detail.open')
        .forEach(x=>{
            x.classList.remove('open');
        });


    if(wasOpen){

        openDetailSymbol=null;

    }else{

        el.classList.add('open');

        openDetailSymbol=symbol;
    }
}


function setFilter(v){

    filter=v;

    document
        .querySelectorAll('.tabs button')
        .forEach(b=>
            b.classList.remove('on')
        );


    const id=
        v==='all'
            ?'all'
            :v==='long'
            ?'longTab'
            :'shortTab';


    $(id)?.classList.add('on');

    render();
}


/* =========================================================
   ALT MENÜ
   ========================================================= */

function showView(v){

    currentView=v;


  [
    'scan',
    'markets',
    'trade',
    'history',
    'performance',
    'settings'
].forEach(x=>{

        const el=
            $(x+'View');

        if(el){

            el.classList.toggle(
                'hidden',
                x!==v
            );
        }
    });


    document
        .querySelectorAll('.navbtn')
        .forEach(b=>{

            b.classList.toggle(
                'active',
                b.dataset.view===v
            );
        });


    if(v==='markets')
        renderMarkets();


    if(v==='trade')
        populateTrade();


    if(v==='history')
        renderHistory();

    if(v==='performance')
        renderPerformance();


    if(v==='settings'){

        if($('minScore'))
            $('minScore').value=
                localStorage.getItem(
                    'minScore'
                )||65;


        if($('scanSeconds'))
            $('scanSeconds').value=
                localStorage.getItem(
                    'scanSeconds'
                )||90;
    }
}


/* =========================================================
   PİYASALAR
   ========================================================= */

function renderMarkets(){

    const el=$('marketList');

    if(!el)
        return;


    const a=
        [...tickers.values()]
        .filter(
            x=>
                x.s?.endsWith('USDT')&&
                n(x.q)>1000000
        )
        .sort(
            (a,b)=>
                n(b.q)-n(a.q)
        )
        .slice(0,30);


    el.innerHTML=
        a.length

            ?a.map(x=>`

<div class="marketrow">

<b>${escapeHtml(x.s)}</b>

<span>
${fmt(x.c)}
</span>

<span class="${n(x.P)>=0?'green':'red'}">
${n(x.P)>=0?'+':''}${n(x.P).toFixed(2)}%
</span>

</div>

`).join('')

            :'<div class="empty">'+
             'Canlı piyasa verisi bekleniyor…'+
             '</div>';
}


/* =========================================================
   PAPER TRADING STORAGE
   ========================================================= */

function getOpenPosition(){

    try{

        const raw=
            localStorage.getItem(
                'openPaperPosition'
            );

        if(!raw)
            return null;

        const p=JSON.parse(raw);

        if(!p||!p.symbol)
            return null;

        return normalizePosition(p);

    }catch(_){

        return null;
    }
}


function normalizePosition(p){

    return{

        id:p.id||Date.now(),

        symbol:p.symbol,

        side:p.side==='SHORT'
            ?'SHORT'
            :'LONG',

        entry:n(p.entry),

        currentPrice:
            n(p.currentPrice)||n(p.entry),

        sl:n(p.sl),

        tp1:n(p.tp1),

        tp2:n(p.tp2),

        tp3:n(p.tp3),

        lev:n(p.lev)||1,

        capital:n(p.capital),

        notional:
            n(p.notional)||
            n(p.capital)*n(p.lev),

        openedAt:
            p.openedAt||
            new Date().toISOString(),

        status:'Açık',

        maxPnl:n(p.maxPnl),

        minPnl:n(p.minPnl),

        lastPnl:n(p.lastPnl),

        updatedAt:
            p.updatedAt||
            Date.now()
    };
}


function saveOpenPosition(p){

    localStorage.setItem(
        'openPaperPosition',
        JSON.stringify(p)
    );
}


function clearOpenPosition(){

    localStorage.removeItem(
        'openPaperPosition'
    );
}


function getHistory(){

    try{

        const h=
            JSON.parse(
                localStorage.getItem(
                    'paperHistory'
                )||'[]'
            );

        return Array.isArray(h)
            ?h
            :[];

    }catch(_){

        return[];
    }
}


function saveHistory(history){

    localStorage.setItem(
        'paperHistory',
        JSON.stringify(
            history.slice(0,200)
        )
    );
}


/* =========================================================
   TRADE EKRANI
   ========================================================= */

function populateTrade(){

    ensureV8TradeUI();


    const list=
        signals.filter(
            x=>x.side!=='NÖTR'
        );


    const select=
        $('tradeCoin');


    if(!select)
        return;


    const current=
        select.value;


    select.innerHTML=
        list.length

            ?list.map(x=>`

<option value="${escapeHtml(x.symbol)}">
${escapeHtml(x.symbol)}
•
${x.side}
${x.score}/100
</option>

`).join('')

            :'<option value="">Henüz sinyal yok</option>';


    const open=
        getOpenPosition();


    if(open){

        if(
            list.some(
                x=>x.symbol===open.symbol
            )
        ){

            select.value=
                open.symbol;
        }

        loadPositionIntoTradeForm(open);

    }else if(current&&
             list.some(x=>x.symbol===current)){

        select.value=current;

        applyTradeFromSelection();

    }else{

        applyTradeFromSelection();
    }


    renderOpenPosition();
    renderTradeStats();
}


function applyTradeFromSelection(){

    const open=
        getOpenPosition();

    if(open)
        return;


    const select=
        $('tradeCoin');

    if(!select)
        return;


    const x=
        signals.find(
            s=>s.symbol===select.value
        );


    if(!x)
        return;


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


    if($('tradeTP2'))
        $('tradeTP2').value=x.tp2;


    if($('tradeTP3'))
        $('tradeTP3').value=x.tp3;


    calcTrade();
}


function loadPositionIntoTradeForm(p){

    if($('tradeCoin'))
        $('tradeCoin').value=p.symbol;

    if($('tradeSide'))
        $('tradeSide').value=p.side;

    if($('tradeLev'))
        $('tradeLev').value=p.lev;

    if($('tradeEntry'))
        $('tradeEntry').value=p.entry;

    if($('tradeSL'))
        $('tradeSL').value=p.sl;

    if($('tradeTP1'))
        $('tradeTP1').value=p.tp1;

    if($('tradeTP2'))
        $('tradeTP2').value=p.tp2;

    if($('tradeTP3'))
        $('tradeTP3').value=p.tp3;

    calcTrade();
}


function prepareTrade(symbol){

    showView('trade');

    const select=
        $('tradeCoin');

    if(select)
        select.value=symbol;

    applyTradeFromSelection();
}


/* =========================================================
   V8 TRADE UI
   ========================================================= */

function ensureV8TradeUI(){

    const trade=
        $('tradeView');

    if(!trade)
        return;


    if(!$('openPosition')){

        const box=
            document.createElement('div');

        box.id='openPosition';

        trade.appendChild(box);
    }


    if(!$('tradeStats')){

        const stats=
            document.createElement('div');

        stats.id='tradeStats';

        trade.appendChild(stats);
    }
}


function ensureV8HistoryUI(){

    const history=
        $('historyView');

    if(!history)
        return;


    if(!$('historyStats')){

        const box=
            document.createElement('div');

        box.id='historyStats';

        const panel=
            history.querySelector('.panel');

        if(panel)
            panel.insertBefore(
                box,
                $('historyList')
            );
    }
}


/* =========================================================
   TRADE CALC
   ========================================================= */

function calcTrade(){

    const cap=
        n($('tradeCapital')?.value);

    const entry=
        n($('tradeEntry')?.value);

    const sl=
        n($('tradeSL')?.value);

    const tp=
        n($('tradeTP1')?.value);

    const lev=
        n($('tradeLev')?.value);


    if(
        !cap||
        !entry||
        !sl||
        !tp
    ){

        if($('calcResult'))
            $('calcResult').innerHTML='';

        return;
    }


    const riskPct=
        Math.abs(entry-sl)/
        entry*100;


    const rr=
        Math.abs(tp-entry)/
        Math.max(
            Math.abs(entry-sl),
            1e-12
        );


    const notional=
        cap*lev;


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


/* =========================================================
   PAPER POZİSYON AÇ
   ========================================================= */

function savePaperTrade(){

    const symbol=
        $('tradeCoin')?.value;
  
   const signal=
    signals.find(
        x=>x.symbol===symbol
    );

    const side=
        $('tradeSide')?.value;

    const entry=
        n($('tradeEntry')?.value);

    const sl=
        n($('tradeSL')?.value);

    const tp1=
        n($('tradeTP1')?.value);

    const tp2=
        n($('tradeTP2')?.value);

    const tp3=
        n($('tradeTP3')?.value);

    const lev=
        n($('tradeLev')?.value);

    const capital=
        n($('tradeCapital')?.value);


    if(
        !symbol||
        !entry||
        !sl||
        !tp1||
        !capital
    ){

        alert(
            'Lütfen işlem bilgilerini tamamla.'
        );

        return;
    }


    if(getOpenPosition()){

        alert(
            'Zaten açık bir paper pozisyon var. '+
            'Önce mevcut pozisyonu kapat.'
        );

        return;
    }


    if(side!=='LONG'&&side!=='SHORT'){

        alert(
            'İşlem yönü LONG veya SHORT olmalı.'
        );

        return;
    }


    const ticker=
        tickers.get(symbol);


    const currentPrice=
        n(ticker?.c)||entry;


    const position={

        id:
            Date.now()+
            '-' +
            Math.random()
                .toString(36)
                .slice(2,8),

        symbol,

        side,

        entry,

        currentPrice,

        sl,

        tp1,

        tp2:

            tp2||
            (
                side==='LONG'
                    ?entry+
                        Math.abs(tp1-entry)*2
                    :entry-
                        Math.abs(tp1-entry)*2
            ),

        tp3:

            tp3||
            (
                side==='LONG'
                    ?entry+
                        Math.abs(tp1-entry)*3
                    :entry-
                        Math.abs(tp1-entry)*3
            ),

        lev,

        capital,

        notional:
            capital*lev,

        openedAt:
            new Date().toISOString(),

        status:'Açık',

        maxPnl:0,

        minPnl:0,

        lastPnl:0,

        updatedAt:Date.now()
    };


    saveOpenPosition(position);


    renderOpenPosition();

    renderHistory();

    renderTradeStats();


    showView('trade');


    alert(
        'Paper pozisyon açıldı.\n\n'+
        'Gerçek emir gönderilmedi.'
    );
}


/* =========================================================
   PNL
   ========================================================= */

function calculatePnl(
    position,
    currentPrice
){

    const entry=
        n(position.entry);

    const price=
        n(currentPrice);

    const capital=
        n(position.capital);

    const lev=
        n(position.lev);


    if(
        !entry||
        !price
    ){

        return{

            pnl:0,

            pnlPct:0,

            leveragedPct:0,

            move:0
        };
    }


    let move;


    if(position.side==='LONG'){

        move=
            (price-entry)/entry;

    }else{

        move=
            (entry-price)/entry;
    }


    const pnl=
        capital*
        lev*
        move;


    const pnlPct=
        move*100;


    const leveragedPct=
        pnl/
        Math.max(capital,1e-12)*
        100;


    return{

        pnl,

        pnlPct,

        leveragedPct,

        move
    };
}


/* =========================================================
   SL / TP KONTROLÜ
   ========================================================= */

function checkAutoClose(position,price){

    if(!position)
        return null;


    const p=n(price);


    if(!p)
        return null;


    if(position.side==='LONG'){

        if(
            n(position.sl)>0&&
            p<=n(position.sl)
        ){

            return{
                reason:'SL',
                price:n(position.sl)
            };
        }


        if(
            n(position.tp1)>0&&
            p>=n(position.tp1)
        ){

            return{
                reason:'TP1',
                price:n(position.tp1)
            };
        }
    }


    if(position.side==='SHORT'){

        if(
            n(position.sl)>0&&
            p>=n(position.sl)
        ){

            return{
                reason:'SL',
                price:n(position.sl)
            };
        }


        if(
            n(position.tp1)>0&&
            p<=n(position.tp1)
        ){

            return{
                reason:'TP1',
                price:n(position.tp1)
            };
        }
    }


    return null;
}


/* =========================================================
   AÇIK POZİSYON
   ========================================================= */

function renderOpenPosition(){

    ensureV8TradeUI();


    const container=
        $('openPosition');

    if(!container)
        return;


    let position=
        getOpenPosition();


    if(!position){

        container.innerHTML=`

<div class="panel">

<h2>📭 Açık Pozisyon</h2>

<div class="empty">
Açık paper pozisyon bulunmuyor.
</div>

</div>

`;

        return;
    }


    const ticker=
        tickers.get(
            position.symbol
        );


    const currentPrice=
        n(ticker?.c)||
        n(position.currentPrice)||
        position.entry;


    /* Önce otomatik SL/TP */

    const auto=
        checkAutoClose(
            position,
            currentPrice
        );


    if(auto){

        closePaperPosition(
            auto.reason,
            auto.price,
            true
        );

        return;
    }


    position.currentPrice=
        currentPrice;


    const result=
        calculatePnl(
            position,
            currentPrice
        );


    position.lastPnl=
        result.pnl;


    position.maxPnl=
        Math.max(
            n(position.maxPnl),
            result.pnl
        );


    position.minPnl=
        Math.min(
            n(position.minPnl),
            result.pnl
        );


    position.updatedAt=
        Date.now();


    saveOpenPosition(position);


    const pnlClass=
        result.pnl>=0
            ?'green'
            :'red';


    container.innerHTML=`

<div class="panel">

<h2>📈 Açık Pozisyon</h2>

<div class="muted">
Paper Trading • Gerçek emir gönderilmedi
</div>


<div class="grid">

<div class="box">

<span>COIN</span>

<b>
${escapeHtml(position.symbol)}
</b>

</div>


<div class="box">

<span>YÖN</span>

<b class="${
    position.side==='LONG'
        ?'green'
        :'red'
}">
${position.side}
</b>

</div>


<div class="box">

<span>GİRİŞ</span>

<b>
${fmt(position.entry)}
</b>

</div>


<div class="box">

<span>ANLIK FİYAT</span>

<b>
${fmt(currentPrice)}
</b>

</div>


<div class="box">

<span>SERMAYE</span>

<b>
${fmt(position.capital)} USDT
</b>

</div>


<div class="box">

<span>NOTIONAL</span>

<b>
${fmt(position.notional)} USDT
</b>

</div>


<div class="box">

<span>KALDIRAÇ</span>

<b>
${position.lev}x
</b>

</div>


<div class="box">

<span>PNL</span>

<b class="${pnlClass}">
${result.pnl>=0?'+':''}${fmt(result.pnl)} USDT
</b>

</div>

</div>


<div class="grid">

<div class="box">

<span>FİYAT DEĞİŞİMİ</span>

<b class="${pnlClass}">
${result.pnlPct>=0?'+':''}${result.pnlPct.toFixed(2)}%
</b>

</div>


<div class="box">

<span>KALDIRAÇLI PNL</span>

<b class="${pnlClass}">
${result.leveragedPct>=0?'+':''}${result.leveragedPct.toFixed(2)}%
</b>

</div>


<div class="box">

<span>STOP LOSS</span>

<b class="red">
${fmt(position.sl)}
</b>

</div>


<div class="box">

<span>TP1</span>

<b class="green">
${fmt(position.tp1)}
</b>

</div>

</div>


<div class="grid">

<div class="box">

<span>TP2</span>

<b class="green">
${fmt(position.tp2)}
</b>

</div>


<div class="box">

<span>TP3</span>

<b class="green">
${fmt(position.tp3)}
</b>

</div>


<div class="box">

<span>EN İYİ PNL</span>

<b class="green">
+${fmt(position.maxPnl)} USDT
</b>

</div>


<div class="box">

<span>EN KÖTÜ PNL</span>

<b class="red">
${fmt(position.minPnl)} USDT
</b>

</div>

</div>


<div class="meta">

<span>
Açılış:
${new Date(position.openedAt)
    .toLocaleString('tr-TR')}
</span>

<span>
ID: ${escapeHtml(position.id)}
</span>

</div>


<button
class="secondary"
onclick="closePaperPosition()">

Pozisyonu Kapat

</button>

</div>

`;
}


/* =========================================================
   POZİSYON KAPAT
   ========================================================= */

function closePaperPosition(
    reason='MANUEL',
    forcedPrice=null,
    automatic=false
){

    const position=
        getOpenPosition();


    if(!position){

        if(!automatic){

            alert(
                'Açık pozisyon bulunmuyor.'
            );
        }

        return;
    }


    const ticker=
        tickers.get(
            position.symbol
        );


    const exitPrice=
        n(forcedPrice)||
        n(ticker?.c)||
        n(position.currentPrice)||
        position.entry;


    const result=
        calculatePnl(
            position,
            exitPrice
        );


    const record={

        id:position.id,

        symbol:position.symbol,

        side:position.side,

        entry:position.entry,

        exit:exitPrice,

        sl:position.sl,

        tp1:position.tp1,

        tp2:position.tp2,

        tp3:position.tp3,

        lev:position.lev,

        capital:position.capital,

        notional:position.notional,

        pnl:result.pnl,

        pnlPct:result.pnlPct,

        leveragedPct:
            result.leveragedPct,

        maxPnl:
            n(position.maxPnl),

        minPnl:
            n(position.minPnl),

        openedAt:
            position.openedAt,

        closedAt:
            new Date().toISOString(),

        status:'Kapalı',

        closeReason:reason
    };


    const history=
        getHistory();


    history.unshift(record);


    saveHistory(history);


    clearOpenPosition();


    renderOpenPosition();

    renderHistory();

    renderTradeStats();


    if(
        currentView==='trade'||
        currentView==='history'
    ){

        renderHistory();
    }


    if(automatic){

        setTimeout(()=>{

            alert(
                'Paper pozisyon otomatik kapatıldı.\n\n'+
                'Sebep: '+reason+'\n'+
                'Çıkış: '+fmt(exitPrice)+'\n'+
                'PNL: '+
                (result.pnl>=0?'+':'')+
                fmt(result.pnl)+
                ' USDT'
            );

        },50);

    }else{

        alert(
            'Paper pozisyon kapatıldı.\n\n'+
            'Sebep: '+reason+'\n'+
            'PNL: '+
            (result.pnl>=0?'+':'')+
            fmt(result.pnl)+
            ' USDT'
        );
    }
}


/* =========================================================
   GEÇMİŞ İSTATİSTİKLERİ
   ========================================================= */

function getStats(){

    const history=
        getHistory();


    const total=
        history.length;


    const wins=
        history.filter(
            x=>n(x.pnl)>0
        ).length;


    const losses=
        history.filter(
            x=>n(x.pnl)<0
        ).length;


    const breakeven=
        history.filter(
            x=>n(x.pnl)===0
        ).length;


    const totalPnl=
        history.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const winRate=
        total
            ?wins/total*100
            :0;


    const best=
        history.length
            ?Math.max(
                ...history.map(
                    x=>n(x.pnl)
                )
            )
            :0;


    const worst=
        history.length
            ?Math.min(
                ...history.map(
                    x=>n(x.pnl)
                )
            )
            :0;


    return{

        total,

        wins,

        losses,

        breakeven,

        totalPnl,

        winRate,

        best,

        worst
    };
}


function renderTradeStats(){

    ensureV8TradeUI();


    const el=
        $('tradeStats');

    if(!el)
        return;


    const s=getStats();


    const cls=
        s.totalPnl>=0
            ?'green'
            :'red';


    el.innerHTML=`

<div class="panel">

<h2>📊 Paper İstatistikleri</h2>

<div class="grid">

<div class="box">
<span>TOPLAM İŞLEM</span>
<b>${s.total}</b>
</div>

<div class="box">
<span>KAZANAN</span>
<b class="green">${s.wins}</b>
</div>

<div class="box">
<span>KAYBEDEN</span>
<b class="red">${s.losses}</b>
</div>

<div class="box">
<span>WIN RATE</span>
<b>${s.winRate.toFixed(1)}%</b>
</div>

<div class="box">
<span>TOPLAM PNL</span>
<b class="${cls}">
${s.totalPnl>=0?'+':''}${fmt(s.totalPnl)} USDT
</b>
</div>

<div class="box">
<span>EN İYİ</span>
<b class="green">
+${fmt(s.best)} USDT
</b>
</div>

<div class="box">
<span>EN KÖTÜ</span>
<b class="red">
${fmt(s.worst)} USDT
</b>
</div>

<div class="box">
<span>BAŞABAŞ</span>
<b>${s.breakeven}</b>
</div>

</div>

</div>

`;
}


/* =========================================================
   GEÇMİŞ
   ========================================================= */

function renderHistory(){

    ensureV8HistoryUI();


    const el=
        $('historyList');

    if(!el)
        return;


    const history=
        getHistory();


    const open=
        getOpenPosition();


    let html='';


    if(open){

        const ticker=
            tickers.get(
                open.symbol
            );


        const price=
            n(ticker?.c)||
            n(open.currentPrice)||
            open.entry;


        const result=
            calculatePnl(
                open,
                price
            );


        html+=`

<div class="history">

<b class="yellow">

🟡 ${escapeHtml(open.symbol)}
${open.side}
•
AÇIK

</b>


<div class="muted">

Giriş ${fmt(open.entry)}
•
Anlık ${fmt(price)}
•
${open.lev}x

</div>


<div class="${
    result.pnl>=0
        ?'green'
        :'red'
}">

PNL:

${result.pnl>=0?'+':''}
${fmt(result.pnl)}
USDT

</div>

</div>

`;
    }


    if(history.length){

        html+=
            history.map(x=>{

                const cls=
                    n(x.pnl)>=0
                        ?'green'
                        :'red';


                const reason=
                    x.closeReason||
                    'MANUEL';


                return`

<div class="history">

<b class="${cls}">

${escapeHtml(x.symbol)}
${x.side}
•
KAPALI

</b>


<div class="muted">

Çıkış:
${reason}

•
${x.lev}x

</div>


<div class="muted">

Giriş:
${fmt(x.entry)}

•

Çıkış:
${fmt(x.exit)}

</div>


<div class="${cls}">

PNL:

${x.pnl>=0?'+':''}
${fmt(x.pnl)}
USDT

•

${x.leveragedPct>=0?'+':''}
${n(x.leveragedPct).toFixed(2)}%

</div>


<div class="muted">

${new Date(x.openedAt)
    .toLocaleString('tr-TR')}

→

${new Date(x.closedAt)
    .toLocaleString('tr-TR')}

</div>


<div class="muted">

ID:
${escapeHtml(x.id)}

</div>

</div>

`;

            }).join('');
    }


    el.innerHTML=
        html||
        '<div class="empty">'+
        'Henüz paper işlem yok.'+
        '</div>';


    renderHistoryStats();
}


function renderHistoryStats(){

    ensureV8HistoryUI();


    const el=
        $('historyStats');

    if(!el)
        return;


    const s=getStats();


    const cls=
        s.totalPnl>=0
            ?'green'
            :'red';


    el.innerHTML=`

<div class="grid">

<div class="box">
<span>İŞLEM</span>
<b>${s.total}</b>
</div>

<div class="box">
<span>WIN RATE</span>
<b>${s.winRate.toFixed(1)}%</b>
</div>

<div class="box">
<span>KAZANAN</span>
<b class="green">${s.wins}</b>
</div>

<div class="box">
<span>KAYBEDEN</span>
<b class="red">${s.losses}</b>
</div>

<div class="box">
<span>TOPLAM PNL</span>
<b class="${cls}">
${s.totalPnl>=0?'+':''}
${fmt(s.totalPnl)} USDT
</b>
</div>

<div class="box">
<span>EN İYİ</span>
<b class="green">
+${fmt(s.best)}
</b>
</div>

</div>

`;
}


/* =========================
   V8.2 PERFORMANS / İSTATİSTİK
========================= */

function renderPerformance(){

    const el=$('performanceContent');

    if(!el)return;


    const history=getPaperHistory();


    /*
     Sadece gerçekten kapanmış
     işlemleri kullan.
    */

    const closed=
        history.filter(
            x=>
                x &&
                (
                    x.status==='Kapalı' ||
                    x.status==='Closed'
                )
        );


    if(!closed.length){

        el.innerHTML=`

<div class="performance-empty">

📊 Henüz tamamlanmış işlem yok.

<br><br>

İlk paper işlemini kapattığında
istatistikler burada görünecek.

</div>

`;

        return;
    }


    const total=closed.length;


    const winners=
        closed.filter(
            x=>n(x.pnl)>0
        );


    const losers=
        closed.filter(
            x=>n(x.pnl)<0
        );


    const breakeven=
        closed.filter(
            x=>n(x.pnl)===0
        );


    const winRate=
        total
            ?winners.length/total*100
            :0;


    const totalPnl=
        closed.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const grossProfit=
        winners.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const grossLoss=
        losers.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const avgWin=
        winners.length
            ?grossProfit/winners.length
            :0;


    const avgLoss=
        losers.length
            ?grossLoss/losers.length
            :0;


    const profitFactor=
        grossLoss<0
            ?grossProfit/Math.abs(grossLoss)
            :grossProfit>0
                ?Infinity
                :0;


    const best=
        [...closed].sort(
            (a,b)=>
                n(b.pnl)-n(a.pnl)
        )[0];


    const worst=
        [...closed].sort(
            (a,b)=>
                n(a.pnl)-n(b.pnl)
        )[0];


    /*
     LONG / SHORT
    */

    const longs=
        closed.filter(
            x=>x.side==='LONG'
        );


    const shorts=
        closed.filter(
            x=>x.side==='SHORT'
        );


    const longWins=
        longs.filter(
            x=>n(x.pnl)>0
        );


    const shortWins=
        shorts.filter(
            x=>n(x.pnl)>0
        );


    const longPnl=
        longs.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const shortPnl=
        shorts.reduce(
            (sum,x)=>
                sum+n(x.pnl),
            0
        );


    const longWinRate=
        longs.length
            ?longWins.length/longs.length*100
            :0;


    const shortWinRate=
        shorts.length
            ?shortWins.length/shorts.length*100
            :0;


    /*
     SİNYAL SKORU

     Eski işlemlerde score yoksa
     "—" altında bırakılır.
    */

    const scored=
        closed.filter(
            x=>
                Number.isFinite(
                    Number(x.score)
                )
        );


    const scoreBuckets=[

        {
            name:'80–100',
            min:80,
            max:100
        },

        {
            name:'70–79',
            min:70,
            max:79
        },

        {
            name:'65–69',
            min:65,
            max:69
        },

        {
            name:'55–64',
            min:55,
            max:64
        },

        {
            name:'0–54',
            min:0,
            max:54
        }

    ];


    const scoreStats=
        scoreBuckets.map(bucket=>{

            const rows=
                scored.filter(
                    x=>{
                        const s=n(x.score);

                        return(
                            s>=bucket.min&&
                            s<=bucket.max
                        );
                    }
                );


            const wins=
                rows.filter(
                    x=>n(x.pnl)>0
                ).length;


            return{

                ...bucket,

                count:rows.length,

                wins,

                rate:
                    rows.length
                        ?wins/rows.length*100
                        :0
            };

        });


    /*
     KAPANIŞ NEDENLERİ
    */

    const closeReasons={};


    closed.forEach(x=>{

        const reason=
            x.closeReason||
            'MANUEL';


        closeReasons[reason]=
            (closeReasons[reason]||0)+1;

    });


    const pnlClass=
        totalPnl>=0
            ?'performance-positive'
            :'performance-negative';


    const bestClass=
        n(best?.pnl)>=0
            ?'performance-positive'
            :'performance-negative';


    const worstClass=
        n(worst?.pnl)>=0
            ?'performance-positive'
            :'performance-negative';


    let scoreHtml='';


    if(scored.length){

        scoreHtml=
            scoreStats.map(s=>`

<div class="performance-row">

<span>

<b>${s.name}</b>

<br>

<span class="muted">

${s.count} işlem
•
${s.wins} kazanan

</span>

</span>


<span
class="${
    s.rate>=60
        ?'performance-positive'
        :s.rate>=50
        ?'performance-neutral'
        :'performance-negative'
}">

${s.count
    ?s.rate.toFixed(1)+'%'
    :'—'}

</span>

</div>

`).join('');

    }else{

        scoreHtml=`

<div class="performance-empty">

Geçmiş işlemlerde sinyal skoru
kaydı bulunmuyor.

Yeni işlemler V8.2 ile skor bilgisi
kaydetmeye başlayacak.

</div>

`;

    }


    const reasonsHtml=
        Object.entries(closeReasons)
            .sort(
                (a,b)=>b[1]-a[1]
            )
            .map(
                ([reason,count])=>`

<div class="performance-row">

<span>${reason}</span>

<b>${count}</b>

</div>

`
            )
            .join('');


    el.innerHTML=`

<div class="performance-grid">


<div class="performance-card">

<span>TOPLAM İŞLEM</span>

<b>${total}</b>

</div>


<div class="performance-card">

<span>WIN RATE</span>

<b class="${
    winRate>=50
        ?'performance-positive'
        :'performance-negative'
}">

${winRate.toFixed(1)}%

</b>

</div>


<div class="performance-card">

<span>🏆 KAZANAN</span>

<b class="performance-positive">

${winners.length}

</b>

</div>


<div class="performance-card">

<span>⚠ KAYBEDEN</span>

<b class="performance-negative">

${losers.length}

</b>

</div>


<div class="performance-card">

<span>BERABERE</span>

<b class="performance-neutral">

${breakeven.length}

</b>

</div>


<div class="performance-card">

<span>PROFIT FACTOR</span>

<b>

${
    Number.isFinite(profitFactor)
        ?profitFactor.toFixed(2)
        :'∞'
}

</b>

</div>


<div class="performance-card performance-wide">

<span>💰 TOPLAM PNL</span>

<b class="${pnlClass}">

${totalPnl>=0?'+':''}${fmt(totalPnl)} USDT

</b>

</div>


<div class="performance-card">

<span>📈 ORTALAMA KAZANÇ</span>

<b class="performance-positive">

${avgWin>=0?'+':''}${fmt(avgWin)} USDT

</b>

</div>


<div class="performance-card">

<span>📉 ORTALAMA KAYIP</span>

<b class="performance-negative">

${avgLoss>=0?'+':''}${fmt(avgLoss)} USDT

</b>

</div>


<div class="performance-card performance-wide">

<span>🏆 EN İYİ İŞLEM</span>

<b class="performance-positive">

${best?.symbol||'—'}

</b>

<div class="muted">

${best?.side||'—'}

•

${n(best?.pnl)>=0?'+':''}${fmt(best?.pnl)} USDT

</div>

</div>


<div class="performance-card performance-wide">

<span>⚠ EN KÖTÜ İŞLEM</span>

<b class="${worstClass}">

${worst?.symbol||'—'}

</b>

<div class="muted">

${worst?.side||'—'}

•

${n(worst?.pnl)>=0?'+':''}${fmt(worst?.pnl)} USDT

</div>

</div>


<div class="performance-card performance-wide">

<div class="performance-title">

🟢 LONG PERFORMANSI

</div>


<div class="performance-row">

<span>İşlem</span>

<b>${longs.length}</b>

</div>


<div class="performance-row">

<span>Kazanan</span>

<b class="performance-positive">

${longWins.length}

</b>

</div>


<div class="performance-row">

<span>Win Rate</span>

<b class="${
    longWinRate>=50
        ?'performance-positive'
        :'performance-negative'
}">

${longWinRate.toFixed(1)}%

</b>

</div>


<div class="performance-row">

<span>Toplam PNL</span>

<b class="${
    longPnl>=0
        ?'performance-positive'
        :'performance-negative'
}">

${longPnl>=0?'+':''}${fmt(longPnl)} USDT

</b>

</div>

</div>


<div class="performance-card performance-wide">

<div class="performance-title">

🔴 SHORT PERFORMANSI

</div>


<div class="performance-row">

<span>İşlem</span>

<b>${shorts.length}</b>

</div>


<div class="performance-row">

<span>Kazanan</span>

<b class="performance-positive">

${shortWins.length}

</b>

</div>


<div class="performance-row">

<span>Win Rate</span>

<b class="${
    shortWinRate>=50
        ?'performance-positive'
        :'performance-negative'
}">

${shortWinRate.toFixed(1)}%

</b>

</div>


<div class="performance-row">

<span>Toplam PNL</span>

<b class="${
    shortPnl>=0
        ?'performance-positive'
        :'performance-negative'
}">

${shortPnl>=0?'+':''}${fmt(shortPnl)} USDT

</b>

</div>

</div>


<div class="performance-card performance-wide">

<div class="performance-title">

📊 SİNYAL SKORU BAŞARISI

</div>

${scoreHtml}

</div>


<div class="performance-card performance-wide">

<div class="performance-title">

🚪 KAPANIŞ NEDENLERİ

</div>

${reasonsHtml}

</div>


</div>

`;
}


/* =========================================================
   AYARLAR
   ========================================================= */

function saveSettings(){

    const m=
        Math.min(
            95,
            Math.max(
                50,
                n($('minScore')?.value)||65
            )
        );


    const s=
        Math.min(
            600,
            Math.max(
                30,
                n($('scanSeconds')?.value)||90
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


    scanTimer=
        setInterval(
            scan,
            s*1000
        );


    render();


    alert(
        'Ayarlar kaydedildi.'
    );
}


function clearHistory(){

    if(
        !confirm(
            'Paper işlem geçmişi silinsin mi?'
        )
    )
        return;


    localStorage.removeItem(
        'paperHistory'
    );


    renderHistory();

    renderTradeStats();


    alert(
        'Paper geçmişi temizlendi.'
    );
}


/* =========================================================
   TARAMA
   ========================================================= */

async function scan(){

    if(
        busy||
        !tickers.size
    )
        return;


    busy=true;


    if($('refresh'))
        $('refresh').textContent=
            'V8 teknik motoru hesaplıyor…';


    try{

        const top=
            [...tickers.values()]
            .filter(
                x=>
                    x.s?.endsWith('USDT')&&
                    n(x.q)>1000000&&
                    n(x.c)>0
            )
            .sort(
                (a,b)=>
                    n(b.q)-n(a.q)
            )
            .slice(0,8);


        const out=[];


        for(
            let i=0;
            i<top.length;
            i+=2
        ){

            const r=
                await Promise.all(
                    top
                    .slice(i,i+2)
                    .map(
                        x=>
                            analyze(
                                x.s,
                                x
                            )
                            .catch(
                                ()=>null
                            )
                    )
                );


            out.push(
                ...r.filter(Boolean)
            );


            await sleep(250);
        }


        signals=
            out.sort(
                (a,b)=>
                    Math.abs(b.score-50)-
                    Math.abs(a.score-50)
            );


        lastScan=
            Date.now();


        render();


        if(currentView==='markets')
            renderMarkets();


        if(currentView==='trade')
            populateTrade();


        if(currentView==='history')
            renderHistory();


        if($('refresh')){

            $('refresh').textContent=
                'Son tarama '+
                new Date(lastScan)
                    .toLocaleTimeString('tr-TR')+
                ' • V8 • 8 yüksek hacimli coin';
        }


    }catch(e){

        if($('refresh')){

            $('refresh').textContent=
                'Tarama hatası: '+
                e.message;
        }


    }finally{

        busy=false;
    }
}


/* =========================================================
   WEBSOCKET
   ========================================================= */

function connect(){

    try{

        ws=
            new WebSocket(WS);


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

                const data=
                    JSON.parse(e.data);


                const a=
                    data.data||[];


                a.forEach(x=>{

                    if(x?.s)
                        tickers.set(
                            x.s,
                            x
                        );
                });


                if(
                    currentView==='markets'
                ){

                    renderMarkets();
                }


                if(
                    getOpenPosition()
                ){

                    renderOpenPosition();


                    if(
                        currentView==='history'
                    ){

                        renderHistory();
                    }
                }


                if(
                    Date.now()-lastScan>
                    (
                        n(
                            localStorage.getItem(
                                'scanSeconds'
                            )
                        )||90
                    )*1000
                ){

                    scan();
                }


            }catch(_){}
        };


        ws.onclose=()=>{

            if($('status')){

                $('status').textContent=
                    'YENİDEN BAĞLANIYOR';

                $('status')
                    .classList.remove('live');
            }


            setTimeout(
                connect,
                2000
            );
        };


        ws.onerror=()=>{

            try{
                ws.close();
            }catch(_){}
        };


    }catch(_){

        setTimeout(
            connect,
            3000
        );
    }
}


/* =========================================================
   POZİSYON GÜNCELLEME TIMER
   ========================================================= */

function positionTick(){

    const position=
        getOpenPosition();


    if(!position)
        return;


    renderOpenPosition();


    if(currentView==='history')
        renderHistory();
}


/* =========================================================
   EVENTLER
   ========================================================= */

function bindEvents(){

    const tradeCoin=
        $('tradeCoin');


    if(tradeCoin){

        tradeCoin.addEventListener(
            'change',
            applyTradeFromSelection
        );
    }


    [
        'tradeEntry',
        'tradeSL',
        'tradeTP1',
        'tradeTP2',
        'tradeTP3',
        'tradeLev',
        'tradeCapital'
    ].forEach(id=>{

        const el=$(id);

        if(el){

            el.addEventListener(
                'input',
                calcTrade
            );
        }
    });
}


/* =========================================================
   V8 BAŞLANGIÇ
   ========================================================= */

function initV8(){

    bindEvents();

    ensureV8TradeUI();

    ensureV8HistoryUI();

    render();

    renderOpenPosition();

    renderHistory();

    renderTradeStats();

    renderHistoryStats();

    connect();

    scan();


    const seconds=
        n(
            localStorage.getItem(
                'scanSeconds'
            )
        )||90;


    scanTimer=
        setInterval(
            scan,
            seconds*1000
        );


    positionTimer=
        setInterval(
            positionTick,
            1000
        );
}


/* =========================================================
   BAŞLAT
   ========================================================= */

initV8();

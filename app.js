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
let positionTimer=null;
let closingPosition=false;


/* =========================
   YARDIMCI
========================= */

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

    if(v>=1e9)return(v/1e9).toFixed(1)+'B';
    if(v>=1e6)return(v/1e6).toFixed(1)+'M';
    if(v>=1e3)return(v/1e3).toFixed(1)+'K';

    return v.toFixed(0);
};


/* =========================
   API
========================= */

async function api(path,p={}){

    const u=new URL(API+path);

    Object.entries(p).forEach(([k,v])=>{
        u.searchParams.set(k,v);
    });

    const r=await fetch(u,{
        cache:'no-store'
    });

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

    const k=2/(p+1);

    let e=
        a
            .slice(0,p)
            .reduce((x,y)=>x+y,0)/p;

    for(let i=p;i<a.length;i++){
        e=a[i]*k+e*(1-k);
    }

    return e;
}


function rsi(a,p=14){

    if(a.length<p+1)return 50;

    let g=0;
    let l=0;

    for(let i=1;i<=p;i++){

        const d=a[i]-a[i-1];

        g+=d>0?d:0;
        l+=d<0?-d:0;
    }

    let ag=g/p;
    let al=l/p;

    for(let i=p+1;i<a.length;i++){

        const d=a[i]-a[i-1];

        ag=
            (ag*(p-1)+(d>0?d:0))/p;

        al=
            (al*(p-1)+(d<0?-d:0))/p;
    }

    return al===0
        ?100
        :100-100/(1+ag/al);
}


function atr(c,p=14){

    if(c.length<p+1)return 0;

    const t=[];

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

    const vals=[];

    for(let i=26;i<=a.length;i++){

        const x=a.slice(0,i);

        vals.push(
            ema(x,12)-ema(x,26)
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
        c
            .slice(-21,-1)
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

    if(x>=55)return'LONG';

    if(x<=45)return'SHORT';

    return'NÖTR';
}


function candleBias(c){

    const x=c.at(-1);

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
    )return 1;

    if(
        x.c<x.o&&
        body/range>.55
    )return -1;

    if(
        lower/range>.45&&
        x.c>=x.o
    )return 1;

    if(
        upper/range>.45&&
        x.c<=x.o
    )return -1;

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


    if(m1.h>0)
        longPts+=8;
    else if(m1.h<0)
        shortPts+=8;


    if(m15.h>0)
        longPts+=7;
    else if(m15.h<0)
        shortPts+=7;


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


    if(volume>=1.25){

        if(t5.s>0)
            longPts+=7;
        else if(t5.s<0)
            shortPts+=7;
    }


    if(fund>.0005)
        shortPts+=4;
    else if(fund<-.0005)
        longPts+=4;


    if(lsr>1.25)
        shortPts+=4;
    else if(
        lsr&&
        lsr<.8
    )
        longPts+=4;


    const cb=
        candleBias(c5);


    if(cb>0)
        longPts+=4;
    else if(cb<0)
        shortPts+=4;


    const score=
        clamp(
            Math.round(
                50+
                (longPts-shortPts)*.5
            )
        );


    const s=side(score);


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


    const rr1=
        Math.abs(tp1-price)/
        Math.max(
            Math.abs(price-sl),
            1e-12
        );


    const rr2=
        Math.abs(tp2-price)/
        Math.max(
            Math.abs(price-sl),
            1e-12
        );


    const rr3=
        Math.abs(tp3-price)/
        Math.max(
            Math.abs(price-sl),
            1e-12
        );


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
   SİNYAL LİSTESİ
========================= */

function render(){

    const list=$('list');

    if(!list)return;


    let rows=
        signals.filter(
            x=>
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


    if($('long')){

        $('long').textContent=
            signals.filter(
                x=>
                    x.side==='LONG'&&
                    x.score>=65
            ).length;
    }


    if($('short')){

        $('short').textContent=
            signals.filter(
                x=>
                    x.side==='SHORT'&&
                    x.score<=35
            ).length;
    }


    if($('count'))
        $('count').textContent=
            tickers.size;


    const min=
        Number(
            localStorage.getItem(
                'minScore'
            )||65
        );


    rows=
        rows.filter(
            x=>
                x.side==='NÖTR'||
                x.score>=min||
                100-x.score>=min
        );


    if(!rows.length){

        list.innerHTML=
            '<div class="empty">Henüz güçlü sinyal yok. Tarama devam ediyor…</div>';

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


                return`

<article class="coin">

<div class="row">

<div>

<div class="symbol">
${x.symbol}
</div>

<div class="muted">
${x.confirmation}
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

<b>${x.confirmation}</b>
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
<b>
${x.lsr?x.lsr.toFixed(2):'—'}
</b>
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

    if(!el)return;


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


function setFilter(f){

    filter=f;

    [
        'all',
        'longTab',
        'shortTab'
    ].forEach(id=>{
        $(id)?.classList.remove('on');
    });


    const id=
        f==='all'
            ?'all'
            :f==='long'
            ?'longTab'
            :'shortTab';


    $(id)?.classList.add('on');

    render();
}


/* =========================
   ALT MENÜ
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

        const el=$(x+'View');

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


    if(v==='trade'){

        populateTrade();

        renderOpenPosition();
    }


    if(v==='history')
        renderHistory();


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


/* =========================
   PİYASALAR
========================= */

function renderMarkets(){

    const el=$('marketList');

    if(!el)return;


    const a=
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


    el.innerHTML=
        a.length

            ?a.map(x=>`

<div class="marketrow">

<b>${x.s}</b>

<span>${fmt(x.c)}</span>

<span class="${n(x.P)>=0?'green':'red'}">

${n(x.P)>=0?'+':''}
${n(x.P).toFixed(2)}%

</span>

</div>

`).join('')

            :'<div class="empty">Canlı piyasa verisi bekleniyor…</div>';
}


/* =========================
   PAPER STORAGE
========================= */

function getOpenPosition(){

    try{

        const p=
            JSON.parse(
                localStorage.getItem(
                    'openPaperPosition'
                )||'null'
            );

        return p;

    }catch(_){

        return null;
    }
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


function getPaperHistory(){

    try{

        const raw=
            localStorage.getItem(
                'paperHistory'
            );

        if(!raw)return[];

        const parsed=
            JSON.parse(raw);

        return Array.isArray(parsed)
            ?parsed
            :[];

    }catch(_){

        return[];
    }
}


function savePaperHistory(history){

    localStorage.setItem(
        'paperHistory',
        JSON.stringify(
            history.slice(0,100)
        )
    );
}


/* =========================
   PAPER İŞLEM FORMU
========================= */

function populateTrade(){

    const list=
        signals.filter(
            x=>x.side!=='NÖTR'
        );


    const select=$('tradeCoin');

    if(!select)return;


    const open=
        getOpenPosition();


    select.innerHTML=
        list.length

            ?list.map(x=>`

<option value="${x.symbol}">
${x.symbol} • ${x.side} ${x.score}/100
</option>

`).join('')

            :'<option value="">Henüz sinyal yok</option>';


    if(open){

        if(
            list.some(
                x=>x.symbol===open.symbol
            )
        ){

            select.value=
                open.symbol;
        }
    }


    applyTradeFromSelection();

    renderOpenPosition();
}


function applyTradeFromSelection(){

    const select=$('tradeCoin');

    if(!select)return;


    const x=
        signals.find(
            s=>s.symbol===select.value
        );


    if(!x)return;


    const open=
        getOpenPosition();


    /*
     Açık pozisyonun form değerlerini
     tarama sırasında ezme.
    */

    if(
        open&&
        open.symbol===x.symbol
    ){

        renderOpenPosition();

        return;
    }


    if($('tradeSide'))
        $('tradeSide').value=
            x.side;


    if($('tradeLev'))
        $('tradeLev').value=
            x.lev;


    if($('tradeEntry'))
        $('tradeEntry').value=
            x.entry;


    if($('tradeSL'))
        $('tradeSL').value=
            x.sl;


    if($('tradeTP1'))
        $('tradeTP1').value=
            x.tp1;


    calcTrade();
}


function prepareTrade(symbol){

    showView('trade');


    const select=$('tradeCoin');


    if(select)
        select.value=symbol;


    applyTradeFromSelection();
}


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


/* =========================
   PAPER POZİSYON AÇ
========================= */

function savePaperTrade(){

    const symbol=
        $('tradeCoin')?.value;

    const side=
        $('tradeSide')?.value;

    const entry=
        n($('tradeEntry')?.value);

    const sl=
        n($('tradeSL')?.value);

    const tp1=
        n($('tradeTP1')?.value);

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
            'Zaten açık bir paper pozisyon var. Önce mevcut pozisyonu kapat.'
        );

        return;
    }


    const ticker=
        tickers.get(symbol);


    const currentPrice=
        n(ticker?.c)||entry;


    const position={

        id:Date.now(),

        symbol,

        side,

        entry,

        currentPrice,

        sl,

        tp1,

        lev,

        capital,

        notional:
            capital*lev,

        openedAt:
            new Date().toISOString(),

        status:'Açık',

        closeReason:null,

        maxPnl:0,

        minPnl:0
    };


    saveOpenPosition(
        position
    );


    renderOpenPosition();

    renderHistory();


    alert(
        'Paper pozisyon açıldı. Gerçek emir gönderilmedi.'
    );
}


/* =========================
   CANLI PNL
========================= */

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

            leveragedPct:0
        };
    }


    let move;


    if(
        position.side==='LONG'
    ){

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
        pnl/capital*100;


    return{

        pnl,

        pnlPct,

        leveragedPct
    };
}


/* =========================
   SL / TP KONTROLÜ
========================= */

function checkAutoExit(
    position,
    currentPrice
){

    if(
        !position||
        closingPosition
    )
        return null;


    const price=
        n(currentPrice);


    if(!price)
        return null;


    const sl=
        n(position.sl);

    const tp1=
        n(position.tp1);


    if(
        position.side==='LONG'
    ){

        if(
            sl>0&&
            price<=sl
        ){

            return{

                reason:'STOP LOSS',

                price
            };
        }


        if(
            tp1>0&&
            price>=tp1
        ){

            return{

                reason:'TP1',

                price
            };
        }

    }else{

        if(
            sl>0&&
            price>=sl
        ){

            return{

                reason:'STOP LOSS',

                price
            };
        }


        if(
            tp1>0&&
            price<=tp1
        ){

            return{

                reason:'TP1',

                price
            };
        }
    }


    return null;
}


/* =========================
   OTOMATİK / MANUEL KAPATMA
========================= */

function autoClosePaperPosition(
    reason,
    exitPrice
){

    if(closingPosition)
        return;


    const position=
        getOpenPosition();


    if(!position)
        return;


    closingPosition=true;


    try{

        const price=
            n(exitPrice)||
            n(position.currentPrice)||
            n(position.entry);


        const result=
            calculatePnl(
                position,
                price
            );


        const record={

            id:position.id,

            symbol:position.symbol,

            side:position.side,

            entry:n(position.entry),

            exit:price,

            sl:n(position.sl),

            tp1:n(position.tp1),

            lev:n(position.lev),

            capital:n(position.capital),

            notional:n(
                position.notional
            ),

            pnl:result.pnl,

            pnlPct:result.pnlPct,

            leveragedPct:
                result.leveragedPct,

            openedAt:
                position.openedAt,

            closedAt:
                new Date().toISOString(),

            status:'Kapalı',

            closeReason:
                reason||'MANUEL',

            maxPnl:
                n(position.maxPnl),

            minPnl:
                n(position.minPnl)
        };


        const history=
            getPaperHistory();


        /*
         Aynı işlem daha önce
         kaydedilmişse tekrar ekleme.
        */

        const exists=
            history.some(
                x=>
                    String(x.id)===
                    String(record.id)
            );


        if(!exists){

            history.unshift(
                record
            );

            savePaperHistory(
                history
            );
        }


        clearOpenPosition();


        renderOpenPosition();

        renderHistory();


        if(
            reason==='STOP LOSS'
        ){

            alert(
                `Paper pozisyon otomatik kapatıldı.

${record.symbol} ${record.side}

Kapanış nedeni:
STOP LOSS

Çıkış:
${fmt(record.exit)}

PNL:
${record.pnl>=0?'+':''}${fmt(record.pnl)} USDT`
            );

        }else if(
            reason==='TP1'
        ){

            alert(
                `Paper pozisyon otomatik kapatıldı.

${record.symbol} ${record.side}

Kapanış nedeni:
TP1

Çıkış:
${fmt(record.exit)}

PNL:
${record.pnl>=0?'+':''}${fmt(record.pnl)} USDT`
            );
        }

    }finally{

        closingPosition=false;
    }
}


/* =========================
   AÇIK POZİSYON GÖRÜNÜMÜ
========================= */

function renderOpenPosition(){

    const container=
        $('openPosition');


    if(!container)
        return;


    const position=
        getOpenPosition();


    if(!position){

        container.innerHTML=`

<div class="empty">

Açık paper pozisyon bulunmuyor.

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


    position.currentPrice=
        currentPrice;


    const result=
        calculatePnl(
            position,
            currentPrice
        );


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


    saveOpenPosition(
        position
    );


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
${position.symbol}
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
${fmt(position.capital)}
USDT
</b>

</div>


<div class="box">

<span>NOTIONAL</span>

<b>
${fmt(position.notional)}
USDT
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

${result.pnl>=0?'+':''}
${fmt(result.pnl)}
USDT

</b>

</div>

</div>


<div class="grid">

<div class="box">

<span>FİYAT DEĞİŞİMİ</span>

<b class="${pnlClass}">

${result.pnlPct>=0?'+':''}
${result.pnlPct.toFixed(2)}%

</b>

</div>


<div class="box">

<span>KALDIRAÇLI PNL</span>

<b class="${pnlClass}">

${result.leveragedPct>=0?'+':''}
${result.leveragedPct.toFixed(2)}%

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


<div class="meta">

<span>

Açılış:
${new Date(
    position.openedAt
)
    .toLocaleString('tr-TR')}

</span>


<span>

En iyi:
${result.pnl>=0?'+':''}
${fmt(position.maxPnl)}
USDT

</span>

</div>


<button
class="secondary"
onclick="closePaperPosition('MANUEL')">

Pozisyonu Kapat

</button>

</div>

`;
}


/* =========================
   POZİSYON KAPAT
========================= */

function closePaperPosition(
    reason='MANUEL'
){

    if(closingPosition)
        return;


    const position=
        getOpenPosition();


    if(!position){

        alert(
            'Açık pozisyon bulunmuyor.'
        );

        return;
    }


    const ticker=
        tickers.get(
            position.symbol
        );


    const exitPrice=
        n(ticker?.c)||
        n(position.currentPrice)||
        n(position.entry);


    autoClosePaperPosition(
        reason,
        exitPrice
    );


    if(reason==='MANUEL'){

        showView('history');


        const history=
            getPaperHistory();


        const last=
            history[0];


        if(last){

            alert(
                `Paper pozisyon kapatıldı.

${last.symbol} ${last.side}

Çıkış:
${fmt(last.exit)}

PNL:
${last.pnl>=0?'+':''}${fmt(last.pnl)} USDT

Geçmişe kaydedildi.`
            );
        }
    }
}


/* =========================
   GEÇMİŞ
========================= */

function renderHistory(){

    const el=
        $('historyList');


    if(!el)
        return;


    const history=
        getPaperHistory();


    const open=
        getOpenPosition();


    let html='';


    /*
     Halen açık işlem varsa
     geçmiş ekranında açık olarak
     gösterilir.
    */

    if(open){

        const ticker=
            tickers.get(
                open.symbol
            );


        const price=
            n(ticker?.c)||
            n(open.currentPrice)||
            n(open.entry);


        const result=
            calculatePnl(
                open,
                price
            );


        html+=`

<div class="history">

<b class="yellow">

🟡
${open.symbol}
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
            history
                .map((x,index)=>{

                    const pnl=
                        n(x.pnl);


                    const pnlClass=
                        pnl>=0
                            ?'green'
                            :'red';


                    const reason=
                        x.closeReason||
                        'MANUEL';


                    return`

<div class="history">

<div class="row">

<div>

<b>
${index+1}.
${x.symbol}
</b>

<div class="muted">

${x.side}
•
${x.status}

</div>

</div>


<div class="${pnlClass}">

<b>

${pnl>=0?'+':''}
${fmt(pnl)}
USDT

</b>

</div>

</div>


<div class="grid">

<div class="box">

<span>GİRİŞ</span>

<b>
${fmt(x.entry)}
</b>

</div>


<div class="box">

<span>ÇIKIŞ</span>

<b>
${fmt(x.exit)}
</b>

</div>


<div class="box">

<span>KALDIRAÇ</span>

<b>
${x.lev}x
</b>

</div>


<div class="box">

<span>SERMAYE</span>

<b>
${fmt(x.capital)}
USDT
</b>

</div>

</div>


<div class="meta">

<span>

Kapanış:
<b>
${reason}
</b>

</span>


<span>

Kaldıraçlı:

<span class="${pnlClass}">

${n(x.leveragedPct)>=0?'+':''}
${n(x.leveragedPct).toFixed(2)}%

</span>

</span>

</div>


<div class="muted">

Açılış:

${x.openedAt
    ?new Date(
        x.openedAt
    ).toLocaleString('tr-TR')
    :'—'
}

<br>

Kapanış:

${x.closedAt
    ?new Date(
        x.closedAt
    ).toLocaleString('tr-TR')
    :'—'
}

</div>

</div>

`;

                })
                .join('');
    }


    el.innerHTML=
        html||
        '<div class="empty">Henüz tamamlanmış paper işlem yok.</div>';
}


/* =========================
   AYARLAR
========================= */

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
        confirm(
            'Paper işlem geçmişi silinsin mi?'
        )
    ){

        localStorage.removeItem(
            'paperHistory'
        );

        renderHistory();
    }
}


/* =========================
   TARAMA
========================= */

async function scan(){

    if(
        busy||
        !tickers.size
    )
        return;


    busy=true;


    if($('refresh'))
        $('refresh').textContent=
            'V7 teknik motoru hesaplıyor…';


    try{

        const top=
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


            await new Promise(
                r=>setTimeout(
                    r,
                    250
                )
            );
        }


        signals=
            out.sort(
                (a,b)=>
                    Math.abs(
                        b.score-50
                    )-
                    Math.abs(
                        a.score-50
                    )
            );


        lastScan=
            Date.now();


        render();


        if(
            currentView==='markets'
        )
            renderMarkets();


        if(
            currentView==='trade'
        ){

            populateTrade();

            renderOpenPosition();
        }


        if(
            currentView==='history'
        )
            renderHistory();


        if($('refresh'))
            $('refresh').textContent=
                'Son tarama '+
                new Date(lastScan)
                    .toLocaleTimeString(
                        'tr-TR'
                    )+
                ' • V7 • 8 yüksek hacimli coin';


    }catch(e){

        if($('refresh'))
            $('refresh').textContent=
                'Tarama hatası: '+
                e.message;

    }finally{

        busy=false;
    }
}


/* =========================
   WEBSOCKET
========================= */

function connect(){

    try{

        ws=
            new WebSocket(WS);


        ws.onopen=()=>{

            if($('status')){

                $('status').textContent=
                    '● CANLI';


                $('status')
                    .classList.add(
                        'live'
                    );
            }
        };


        ws.onmessage=e=>{

            try{

                const a=
                    JSON.parse(
                        e.data
                    ).data||[];


                a.forEach(x=>{
                    tickers.set(
                        x.s,
                        x
                    );
                });


                if(
                    currentView==='markets'
                )
                    renderMarkets();


                /*
                 Açık pozisyon varsa
                 fiyat ve PNL güncellenir.
                */

                const position=
                    getOpenPosition();


                if(position){

                    const ticker=
                        tickers.get(
                            position.symbol
                        );


                    const currentPrice=
                        n(ticker?.c)||
                        n(position.currentPrice)||
                        n(position.entry);


                    position.currentPrice=
                        currentPrice;


                    const result=
                        calculatePnl(
                            position,
                            currentPrice
                        );


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


                    saveOpenPosition(
                        position
                    );


                    renderOpenPosition();


                    if(
                        currentView==='history'
                    )
                        renderHistory();


                    /*
                     Önce fiyat güncellenir,
                     sonra SL / TP kontrol edilir.
                    */

                    const exit=
                        checkAutoExit(
                            position,
                            currentPrice
                        );


                    if(exit){

                        autoClosePaperPosition(
                            exit.reason,
                            exit.price
                        );
                    }
                }


                if(
                    Date.now()-lastScan>
                    90000
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


        ws.onerror=()=>{
            ws.close();
        };


    }catch(_){

        setTimeout(
            connect,
            3000
        );
    }
}


/* =========================
   EVENTLER
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

    const el=$(id);


    if(el){

        el.addEventListener(
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
        (
            n(
                localStorage.getItem(
                    'scanSeconds'
                )
            )||90
        )*1000
    );


/*
 V7 güvenlik zamanlayıcısı.

 WebSocket gecikse bile açık pozisyon
 fiyatı düzenli kontrol edilir.

 Aynı zamanda SL / TP otomatik
 kapanış kontrolü yapılır.
*/

positionTimer=
    setInterval(
        ()=>{

            const position=
                getOpenPosition();


            if(!position)
                return;


            const ticker=
                tickers.get(
                    position.symbol
                );


            const currentPrice=
                n(ticker?.c)||
                n(position.currentPrice)||
                n(position.entry);


            position.currentPrice=
                currentPrice;


            const result=
                calculatePnl(
                    position,
                    currentPrice
                );


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


            saveOpenPosition(
                position
            );


            renderOpenPosition();


            if(
                currentView==='history'
            )
                renderHistory();


            const exit=
                checkAutoExit(
                    position,
                    currentPrice
                );


            if(exit){

                autoClosePaperPosition(
                    exit.reason,
                    exit.price
                );
            }

        },
        1000
    );

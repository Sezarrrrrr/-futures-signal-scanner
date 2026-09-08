/* =========================================================
   FUTURES SIGNAL SCANNER V9
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

        const x=
            a.slice(0,i);

        const fast=
            ema(x,12);

        const slow=
            ema(x,26);

        vals.push(
            (fast||0)-(slow||0)
        );

    }

    const m=
        vals.at(-1)||0;

    const s=
        ema(vals,9)||0;

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

    const a=
        c.map(x=>x.c);

    const e20=
        ema(a,20);

    const e50=
        ema(a,50);

    const e200=
        ema(a,200);

    const p=
        a.at(-1);

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

    const x=
        c.at(-1);

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


    const c5=
        parseK(r5);

    const c15=
        parseK(r15);

    const c1=
        parseK(r1);


    const a5=
        c5.map(x=>x.c);

    const a15=
        c15.map(x=>x.c);

    const a1=
        c1.map(x=>x.c);


    const t5=
        trend(c5);

    const t15=
        trend(c15);

    const t1=
        trend(c1);


    const r5v=
        rsi(a5);

    const r15v=
        rsi(a15);

    const r1v=
        rsi(a1);


    const m5=
        macd(a5);

    const m15=
        macd(a15);

    const m1=
        macd(a1);


    const volume=
        vr(c5);

    const A=
        atr(c5);


    const fund=
        n(f?.[0]?.fundingRate);

    const oiNow=
        n(oi?.openInterest);

    const lsr=
        n(ls?.[0]?.longShortRatio);


    const price=
        n(t?.c)||
        a5.at(-1);


    const e1=
        ema(a1,20);

    const e150=
        ema(a1,50);


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

    const cb=
        candleBias(c5);

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


    const s=
        side(score);


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

        sl=
            price+risk;

        tp1=
            price-risk;

        tp2=
            price-risk*2;

        tp3=
            price-risk*3;

    }else{

        sl=
            price-risk;

        tp1=
            price+risk;

        tp2=
            price+risk*2;

        tp3=
            price+risk*3;

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

    const list=
        $('list');

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

        $('tradeTP3').value=p.tp3;

    if($('tradeCapital'))
        $('tradeCapital').value=p.capital||100;

    calcTrade();
}


/* =========================================================
   TRADE HESAPLAMA
   ========================================================= */

function calcTrade(){

    const entry=
        n($('tradeEntry')?.value);

    const sl=
        n($('tradeSL')?.value);

    const tp1=
        n($('tradeTP1')?.value);

    const lev=
        n($('tradeLev')?.value)||1;

    const capital=
        n($('tradeCapital')?.value)||100;

    const side=
        $('tradeSide')?.value||'LONG';

    if(!entry||!sl)
        return;

    const stopDistance=
        Math.abs(entry-sl);

    const riskPercent=
        entry
            ?stopDistance/entry*100
            :0;

    const riskBudget=
        capital*.01;

    const plannedRisk=
        Math.min(
            riskBudget,
            capital*.02
        );

    const quantity=
        stopDistance>0
            ?plannedRisk/stopDistance
            :0;

    const notional=
        quantity*entry;

    const maxNotional=
        capital*lev;

    const safeQuantity=
        Math.min(
            quantity,
            entry>0
                ?maxNotional/entry
                :0
        );

    if($('tradeRisk'))
        $('tradeRisk').textContent=
            riskPercent.toFixed(2)+'%';

    if($('tradeRiskBudget'))
        $('tradeRiskBudget').textContent=
            riskBudget.toFixed(4)+' USDT';

    if($('tradeQuantity'))
        $('tradeQuantity').textContent=
            safeQuantity.toFixed(6);

    if($('tradeNotional'))
        $('tradeNotional').textContent=
            (safeQuantity*entry).toFixed(2)+' USDT';

}


/* =========================================================
   TRADE HAZIRLAMA
   ========================================================= */

function prepareTrade(symbol){

    showView('trade');

    const select=
        $('tradeCoin');

    if(!select)
        return;

    select.value=symbol;

    applyTradeFromSelection();
}


/* =========================================================
   V10.5 RİSK TABANLI PAPER İŞLEM
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

    const tp2Input=
        n($('tradeTP2')?.value);

    const tp3Input=
        n($('tradeTP3')?.value);

    const lev=
        n($('tradeLev')?.value);

    const capital=
        n($('tradeCapital')?.value)||100;


    /* ---------------------------------------------------------
       TEMEL KONTROLLER
    --------------------------------------------------------- */

    if(!symbol){

        alert(
            'Lütfen coin seç.'
        );

        return;
    }


    if(
        side!=='LONG'&&
        side!=='SHORT'
    ){

        alert(
            'İşlem yönü LONG veya SHORT olmalı.'
        );

        return;
    }


    if(!entry||entry<=0){

        alert(
            'Geçerli bir giriş fiyatı gir.'
        );

        return;
    }


    if(!sl||sl<=0){

        alert(
            'Geçerli bir Stop Loss gir.'
        );

        return;
    }


    if(!tp1||tp1<=0){

        alert(
            'Geçerli bir TP1 gir.'
        );

        return;
    }


    if(!lev||lev<=0){

        alert(
            'Geçerli bir kaldıraç seç.'
        );

        return;
    }


    if(
        !Number.isFinite(capital)||
        capital<=0
    ){

        alert(
            'Sermaye değeri geçersiz.'
        );

        return;
    }


    /* ---------------------------------------------------------
       RİSK HESABI
    --------------------------------------------------------- */

    const stopDistance=
        Math.abs(
            entry-sl
        );


    if(
        !Number.isFinite(stopDistance)||
        stopDistance<=0
    ){

        alert(
            'SL mesafesi geçersiz.'
        );

        return;
    }


    const riskPercent=
        entry
            ?stopDistance/entry*100
            :0;


    const riskBudget=
        capital*.01;


    const plannedRisk=
        Math.min(
            riskBudget,
            capital*.02
        );


    let quantity=
        plannedRisk/
        stopDistance;


    const maxNotional=
        capital*lev;


    const maxQuantity=
        entry>0
            ?maxNotional/entry
            :0;


    quantity=
        Math.min(
            quantity,
            maxQuantity
        );


    if(
        !Number.isFinite(quantity)||
        quantity<=0
    ){

        alert(
            'Hesaplanan işlem miktarı geçersiz.'
        );

        return;
    }


    const notional=
        quantity*entry;


    if(
        !Number.isFinite(notional)||
        notional<=0
    ){

        alert(
            'Notional değeri geçersiz.'
        );

        return;
    }


    if(
        notional>
        maxNotional
    ){

        alert(
            'Güvenlik hatası: '+
            'maksimum notional aşıldı.'
        );

        return;
    }


    /* ---------------------------------------------------------
       TP DEĞERLERİ
    --------------------------------------------------------- */

    const tp2=
        tp2Input>0
            ?tp2Input
            :side==='LONG'
            ?entry+stopDistance*2
            :entry-stopDistance*2;


    const tp3=
        tp3Input>0
            ?tp3Input
            :side==='LONG'
            ?entry+stopDistance*3
            :entry-stopDistance*3;


    /* ---------------------------------------------------------
       POZİSYON
    --------------------------------------------------------- */

    const position={

        id:
            'PAPER-'+
            Date.now(),

        symbol,

        side,

        score:
            signal?.score??null,

        confirmation:
            signal?.confirmation||'',

        entry,

        currentPrice:
            entry,

        sl,

        tp1,

        tp2,

        tp3,

        lev,

        capital,

        riskPercent,

        riskBudget,

        stopDistance,

        plannedRisk,

        notional,

        initialQuantity:
            quantity,

        quantity,

        remainingQuantity:
            quantity,

        tp1Hit:false,

        tp2Hit:false,

        tp3Hit:false,

        slHit:false,

        realizedPnl:0,

        unrealizedPnl:0,

        maxPnl:0,

        minPnl:0,

        openedAt:
            new Date().toISOString(),

        updatedAt:
            Date.now(),

        status:'Açık'

    };


    /* ---------------------------------------------------------
       KAYDET
    --------------------------------------------------------- */

    saveOpenPosition(
        position
    );


    /* ---------------------------------------------------------
       EKRANI GÜNCELLE
    --------------------------------------------------------- */

    renderOpenPosition();

    renderHistory();

    renderTradeStats();

    showView(
        'trade'
    );


    /* ---------------------------------------------------------
       V10.5 KONSOL
    --------------------------------------------------------- */

    console.log(
        'V10.5 RISK POSITION:',
        {

            symbol:
                position.symbol,

            side:
                position.side,

            capital:
                position.capital,

            riskPercent:
                position.riskPercent,

            riskBudget:
                position.riskBudget,

            leverage:
                position.lev,

            stopDistance:
                position.stopDistance,

            plannedRisk:
                position.plannedRisk,

            notional:
                position.notional,

            initialQuantity:
                position.initialQuantity,

            quantity:
                position.quantity,

            entry:
                position.entry,

            sl:
                position.sl

        }
    );


    alert(

        'Paper pozisyon açıldı.\n\n' +

        'Coin: ' +
        position.symbol +

        '\n' +

        'Yön: ' +
        position.side +

        '\n' +

        'Sermaye: ' +
        position.capital.toFixed(2) +

        ' USDT\n' +

        'Risk: ' +
        position.riskPercent.toFixed(2) +

        '%\n' +

        'Risk bütçesi: ' +
        position.riskBudget.toFixed(4) +

        ' USDT\n' +

        'Notional: ' +
        position.notional.toFixed(2) +

        ' USDT\n' +

        'Miktar: ' +
        position.quantity.toFixed(6) +

        '\n\n' +

        'Gerçek emir gönderilmedi.'

    );

}

    if($('tradeTP1'))
        $('tradeTP1').value=p.tp1;

    if($('tradeTP2'))
        $('tradeTP2').value=p.tp2;

    if($('tradeTP3'))
        $('tradeTP3').value=p.tp3;

    if($('tradeCapital'))
        $('tradeCapital').value=
            p.capital||100;

    calcTrade();
}


/* =========================================================
   DETAY
   ========================================================= */

function toggleDetail(symbol){

    const el=
        $('d-'+symbol);

    if(!el)
        return;

    const wasOpen=
        el.classList.contains('open');

    document
        .querySelectorAll('.detail')
        .forEach(x=>
            x.classList.remove('open')
        );

    if(wasOpen){

        openDetailSymbol=
            null;

    }else{

        el.classList.add('open');

        openDetailSymbol=
            symbol;

    }
}


/* =========================================================
   FİLTRE
   ========================================================= */

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

    const el=
        $('marketList');

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

<b>
${escapeHtml(x.s)}
</b>

<span>
${fmt(x.c)}
</span>

<span class="${n(x.P)>=0?'green':'red'}">

${n(x.P)>=0?'+':''}
${n(x.P).toFixed(2)}%

</span>

</div>

`).join('')

            :'<div class="empty">'+
             'Canlı piyasa verisi bekleniyor…'+
             '</div>';

}


/* =========================================================
   PAPER STORAGE
   ========================================================= */

function getOpenPosition(){

    try{

        const raw=
            localStorage.getItem(
                'openPaperPosition'
            );


        if(!raw)
            return null;


        const p=
            JSON.parse(raw);


        if(!p||!p.symbol)
            return null;


        return normalizePosition(p);

    }catch(_){

        return null;

    }

}


/* =========================================================
   V10.4 POSITION NORMALIZER
   ---------------------------------------------------------
   V9 + V10 + V10.4 alanlarını korur.
   Pozisyon okunurken V10 alanlarının kaybolmasını engeller.
   ========================================================= */

function normalizePosition(p){

    if(!p||!p.symbol)
        return null;


    const entry=
        n(p.entry);


    const lev=
        n(p.lev)||1;


    const capital=
        n(p.capital);


    const notional=
        n(p.notional)||
        (
            capital>0
                ?capital*lev
                :0
        );


    let initialQuantity=
        n(p.initialQuantity);


    if(initialQuantity<=0){

        initialQuantity=
            entry>0&&
            notional>0

                ?notional/entry

                :0;

    }


    let quantity=
        n(p.quantity);


    if(quantity<=0)
        quantity=initialQuantity;


    return{

        id:
            p.id||
            Date.now(),


        symbol:
            p.symbol,


        side:
            p.side==='SHORT'
                ?'SHORT'
                :'LONG',


        score:
            Number.isFinite(
                Number(p.score)
            )
                ?n(p.score)
                :null,


        confirmation:
            p.confirmation||'',


        quality:
            p.quality||'',


        entry,


        currentPrice:
            n(p.currentPrice)||
            entry,


        initialSl:
            n(p.initialSl)||
            n(p.sl),


        sl:
            n(p.sl),


        tp1:
            n(p.tp1),


        tp2:
            n(p.tp2),


        tp3:
            n(p.tp3),


        capital,


        lev,


        notional,


        initialQuantity,


        quantity,


        tp1Hit:
            p.tp1Hit===true,


        tp2Hit:
            p.tp2Hit===true,


        tp3Hit:
            p.tp3Hit===true,


        tp1At:
            p.tp1At||null,


        tp2At:
            p.tp2At||null,


        tp3At:
            p.tp3At||null,


        tp1Price:
            n(p.tp1Price),


        tp2Price:
            n(p.tp2Price),


        tp3Price:
            n(p.tp3Price),


        breakEven:
            p.breakEven===true||
            p.breakEvenActive===true,


        breakEvenActive:
            p.breakEvenActive===true||
            p.breakEven===true,


        breakEvenPrice:
            n(p.breakEvenPrice)||
            entry,


        trailingActive:
            p.trailingActive===true,


        trailingStop:
            n(p.trailingStop),


        trailingR:
            n(p.trailingR),


        realizedPNL:
            n(
                p.realizedPNL??
                p.realizedNetPnl
            ),


        grossPNL:
            n(
                p.grossPNL??
                p.realizedGrossPnl
            ),


        commission:
            n(
                p.commission??
                p.realizedFees
            ),


        realizedGrossPnl:
            n(
                p.realizedGrossPnl??
                p.grossPNL
            ),


        realizedFees:
            n(
                p.realizedFees??
                p.commission
            ),


        realizedNetPnl:
            n(
                p.realizedNetPnl??
                p.realizedPNL
            ),


        initialRiskDistance:
            n(p.initialRiskDistance),


        initialRiskPct:
            n(p.initialRiskPct),


        accountRiskPct:
            n(p.accountRiskPct),


        riskAmount:
            n(p.riskAmount),


        maxRiskPct:
            n(p.maxRiskPct),


        maxRiskWarning:
            p.maxRiskWarning===true,


        remainingQtyPct:
            p.remainingQtyPct!==undefined
                ?n(p.remainingQtyPct)
                :1,


        tp1QtyPct:
            n(p.tp1QtyPct),


        tp2QtyPct:
            n(p.tp2QtyPct),


        tp3QtyPct:
            n(p.tp3QtyPct),


        legs:
            p.legs||{},


        events:
            Array.isArray(p.events)
                ?p.events
                :[],


        openedAt:
            p.openedAt||
            new Date().toISOString(),


        status:
            p.status||
            'Açık',


        closed:
            p.closed===true,


        closedAt:
            p.closedAt||null,


        closeReason:
            p.closeReason||'',


        closePrice:
            n(p.closePrice),


        maxPnl:
            n(p.maxPnl),


        minPnl:
            n(p.minPnl),


        lastPnl:
            n(p.lastPnl),


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
            history.slice(0,500)
        )
    );

}

/* =========================================================
   TRADE EKRANI
   ========================================================= */

function populateTrade(){

    const select=
        $('tradeCoin');

    if(!select)
        return;


    const current=
        select.value;


    const list=
        signals
        .filter(x=>x.side!=='NÖTR');


    select.innerHTML=
        '<option value="">Coin seç</option>'+
        list.map(x=>
            `<option value="${escapeHtml(x.symbol)}">
                ${escapeHtml(x.symbol)}
            </option>`
        ).join('');


    if(current)
        select.value=current;


    applyTradeFromSelection();

}


/* =========================================================
   TRADE SEÇİMİ
   ========================================================= */

function applyTradeFromSelection(){

    const symbol=
        $('tradeCoin')?.value;


    if(!symbol)
        return;


    const signal=
        signals.find(
            x=>x.symbol===symbol
        );


    if(!signal)
        return;


    const side=
        $('tradeSide');


    const entry=
        $('tradeEntry');

    const sl=
        $('tradeSL');

    const tp1=
        $('tradeTP1');

    const tp2=
        $('tradeTP2');

    const tp3=
        $('tradeTP3');

    const lev=
        $('tradeLev');


    if(side)
        side.value=
            signal.side;


    if(entry)
        entry.value=
            signal.entry;


    if(sl)
        sl.value=
            signal.sl;


    if(tp1)
        tp1.value=
            signal.tp1;


    if(tp2)
        tp2.value=
            signal.tp2;


    if(tp3)
        tp3.value=
            signal.tp3;


    if(lev)
        lev.value=
            signal.lev;


    if($('tradeCapital')&&
       !$('tradeCapital').value){

        $('tradeCapital').value=
            100;

    }


    calcTrade();

}


/* =========================================================
   AÇIK POZİSYON EKRANI
   ========================================================= */

function renderOpenPosition(){

    const el=
        $('openPosition');

    if(!el)
        return;


    const p=
        getOpenPosition();


    if(!p){

        el.innerHTML=
            '<div class="empty">'+
            'Açık PAPER pozisyon yok.'+
            '</div>';

        return;

    }


    const price=
        n(
            tickers
            .get(p.symbol)
            ?.c
        )||
        p.currentPrice||
        p.entry;


    const pnl=
        calculatePnl(
            p,
            price
        );


    p.currentPrice=
        price;


    p.lastPnl=
        pnl;


    saveOpenPosition(p);


    const cls=
        p.side==='LONG'
            ?'long'
            :'short';


    el.innerHTML=`

<div class="position-card ${cls}">

    <div class="position-head">

        <div>

            <b>
                ${escapeHtml(p.symbol)}
            </b>

            <span class="badge">
                ${escapeHtml(p.side)}
            </span>

        </div>

        <div class="position-status">
            ${escapeHtml(p.status)}
        </div>

    </div>


    <div class="position-grid">

        <div>
            <small>Giriş</small>
            <strong>
                ${fmt(p.entry)}
            </strong>
        </div>


        <div>
            <small>Anlık</small>
            <strong>
                ${fmt(price)}
            </strong>
        </div>


        <div>
            <small>Miktar</small>
            <strong>
                ${fmt(p.quantity)}
            </strong>
        </div>


        <div>
            <small>Notional</small>
            <strong>
                ${fmt(p.notional)}
            </strong>
        </div>


        <div>
            <small>SL</small>
            <strong>
                ${fmt(p.sl)}
            </strong>
        </div>


        <div>
            <small>TP1</small>
            <strong>
                ${fmt(p.tp1)}
            </strong>
        </div>


        <div>
            <small>TP2</small>
            <strong>
                ${fmt(p.tp2)}
            </strong>
        </div>


        <div>
            <small>TP3</small>
            <strong>
                ${fmt(p.tp3)}
            </strong>
        </div>

    </div>


    <div class="pnl">

        <span>
            Anlık PNL
        </span>

        <strong class="${pnl>=0?'green':'red'}">
            ${pnl>=0?'+':''}${pnl.toFixed(2)} USDT
        </strong>

    </div>


    <div class="position-actions">

        <button
            onclick="closePaperPosition('MANUAL')"
        >
            İşlemi Kapat
        </button>

    </div>

</div>

`;


}


/* =========================================================
   PNL
   ========================================================= */

function calculatePnl(
    position,
    currentPrice
){

    if(
        !position||
        !position.entry||
        !position.quantity
    )
        return 0;


    const diff=
        position.side==='LONG'

            ?currentPrice-position.entry

            :position.entry-currentPrice;


    const pnl=
        diff*
        position.quantity;


    return Number.isFinite(pnl)
        ?pnl
        :0;

}


/* =========================================================
   MANUEL POZİSYON KAPAT
   ========================================================= */

function closePaperPosition(
    reason='MANUAL'
){

    const p=
        getOpenPosition();


    if(!p)
        return;


    const price=
        n(
            tickers
            .get(p.symbol)
            ?.c
        )||
        p.currentPrice||
        p.entry;


    const pnl=
        calculatePnl(
            p,
            price
        );


    p.currentPrice=
        price;


    p.closePrice=
        price;


    p.realizedPNL=
        n(p.realizedPNL)+pnl;


    p.realizedNetPnl=
        p.realizedPNL;


    p.status=
        'Kapalı';


    p.closed=
        true;


    p.closedAt=
        new Date().toISOString();


    p.closeReason=
        reason;


    p.quantity=
        0;


    saveHistory([
        p,
        ...getHistory()
    ]);


    clearOpenPosition();


    renderOpenPosition();

    renderHistory();

    renderTradeStats();


    alert(
        'Paper pozisyon kapatıldı.\n\n'+
        'Coin: '+p.symbol+'\n'+
        'PNL: '+pnl.toFixed(2)+' USDT'
    );

}


/* =========================================================
   HISTORY
   ========================================================= */

function renderHistory(){

    const el=
        $('historyList');

    if(!el)
        return;


    const history=
        getHistory();


    if(!history.length){

        el.innerHTML=
            '<div class="empty">'+
            'Henüz kapanmış PAPER işlem yok.'+
            '</div>';

        return;

    }


    el.innerHTML=
        history
        .slice(0,100)
        .map(p=>{

            const pnl=
                n(
                    p.realizedNetPnl??
                    p.realizedPNL
                );


            return`

<div class="history-row">

    <div>

        <b>
            ${escapeHtml(p.symbol)}
        </b>

        <span>
            ${escapeHtml(p.side)}
        </span>

    </div>


    <div>

        ${fmt(p.entry)}
        →
        ${fmt(p.closePrice)}

    </div>


    <strong
        class="${pnl>=0?'green':'red'}"
    >

        ${pnl>=0?'+':''}
        ${pnl.toFixed(2)}
        USDT

    </strong>

</div>

`;

        })
        .join('');

}


/* =========================================================
   TRADE STATS
   ========================================================= */

function renderTradeStats(){

    const history=
        getHistory();


    const closed=
        history.length;


    const pnl=
        history.reduce(
            (sum,p)=>
                sum+
                n(
                    p.realizedNetPnl??
                    p.realizedPNL
                ),
            0
        );


    if($('closedCount'))
        $('closedCount').textContent=
            closed;


    if($('totalPnl'))
        $('totalPnl').textContent=
            pnl.toFixed(2)+' USDT';

}

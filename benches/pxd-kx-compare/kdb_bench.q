/ pxd-kx-compare: aligned micro benchmarks (write/reread/randomread/cpu/cpucache)

rowsEnv: getenv `ROWS;
randEnv: getenv `RANDOM_READS;
cacheEnv: getenv `CACHE_ROWS;
symEnv: getenv `SYMBOL_CARD;
memEnv: getenv `MEM_LIMIT_MB;

rows: $[0<count rowsEnv; "J"$rowsEnv; 10000000];
randreads: $[0<count randEnv; "J"$randEnv; 1000000];
cacherows: $[0<count cacheEnv; "J"$cacheEnv; 1000000];
symcard: $[0<count symEnv; "J"$symEnv; 100000];
memlimitmb: $[0<count memEnv; "J"$memEnv; 0];
symcard: $[symcard>rows; rows; symcard];

rowbytes: 24
lcgmult: 6364136223846793005j;
lcginc: 1j;
lcgshift: 33;
seed0: 1311768467463790320j;

emit:{[name;rows;ops;bytes;elapsed]
  secs: elapsed % 1000f;
  opsps: $[secs=0f; 0f; ops % secs];
  mbps: $[secs=0f; 0f; (bytes % 1048576f) % secs];
  line: raze (name; ","; string rows; ","; string ops; ","; string bytes; ",";
      string elapsed; ","; string opsps; ","; string mbps);
  -1 line
 }

if[memlimitmb>0;
  estbytes: rows*64 + cacherows*64 + symcard*32 + randreads*16;
  limitbytes: memlimitmb * 1024 * 1024;
  if[estbytes>limitbytes;
    -2 "memlimit exceeded, exiting";
    \\
  ];
 ];

logLine: raze ("rows="; string rows; " randreads="; string randreads; " cacherows="; string cacherows; " symcard="; string symcard; " memlimitmb="; string memlimitmb);
-2 logLine;

elapsedMs:{[f]
  t0:.z.p;
  r:f[];
  t1:.z.p;
  1e-6 * (t1 - t0)
 }

header: "test,rows,ops,bytes,elapsed_ms,ops_per_sec,mb_per_sec";
-1 header;

symvals: `$"S",/:string each til symcard;

/ write
writeMs: elapsedMs{
  t_sym: symvals (til rows) mod symcard;
  t_ts: til rows;
  t_val: til rows;
  t_val: 0f + (t_val mod 10000);
  t: ([] symbol:t_sym; ts: t_ts; val: t_val);
  trades:: `symbol`ts`value xcol t;
 };
emit["write"; rows; rows; rows*rowbytes; writeMs];

/ reread
rereadMs: elapsedMs{sum trades[`value]};
emit["reread"; rows; rows; rows*rowbytes; rereadMs];

/ randomread
randomMs: elapsedMs{
  s: seed0 + lcgmult * til randreads;
  idx: (s >> lcgshift) mod rows;
  sum trades[`value] idx
 };
emit["randomread"; rows; randreads; randreads*rowbytes; randomMs];

/ cpu
cpuMs: elapsedMs{v: 0.1 * til rows; v: v*1.0001+0.1; sum v};
emit["cpu"; rows; rows; rows*16; cpuMs];

/ cpucache
cpucacheMs: elapsedMs{v:0.2*til cacherows; acc:0f; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; acc};
emit["cpucache"; cacherows; cacherows*5; cacherows*5*16; cpucacheMs];

\\

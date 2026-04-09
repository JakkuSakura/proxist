/ pxd-kx-compare: aligned micro benchmarks (write/reread/randomread/cpu/cpucache)

rowsEnv: getenv `ROWS;
randEnv: getenv `RANDOM_READS;
cacheEnv: getenv `CACHE_ROWS;

rows: $[count rowsEnv; "J"$rowsEnv; 10000000];
randreads: $[count randEnv; "J"$randEnv; 1000000];
cacherows: $[count cacheEnv; "J"$cacheEnv; 1000000];

rowbytes: 24
lcgmult: 6364136223846793005j;
lcginc: 1j;
lcgshift: 33;
seed0: 1311768467463790320j;

emit:{[name;rows;ops;bytes;elapsed]
  secs: elapsed % 1000f;
  opsps: $[secs=0f; 0f; ops % secs];
  mbps: $[secs=0f; 0f; bytes % 1048576f % secs];
  -1 raze (name; ","; string rows; ","; string ops; ","; string bytes; ",";
      string elapsed; ","; string opsps; ","; string mbps)
 }

elapsedMs:{[f]
  t0:.z.p;
  r:f[];
  t1:.z.p;
  1e-6 * (t1 - t0)
 }

-1 "test,rows,ops,bytes,elapsed_ms,ops_per_sec,mb_per_sec";

sym:{[n]`$"S",/:string each til n};

/ write
writeMs: elapsedMs{
  t_sym: sym rows;
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
  s: seed0;
  acc: 0f;
  n: rows;
  do[randreads;
    s: s * lcgmult + lcginc;
    i: (s >> lcgshift) mod n;
    acc+: trades[`value] i;
    ];
  acc
 };
emit["randomread"; rows; randreads; randreads*rowbytes; randomMs];

/ cpu
cpuMs: elapsedMs{v: 0.1 * til rows; v: v*1.0001+0.1; sum v};
emit["cpu"; rows; rows; rows*16; cpuMs];

/ cpucache
cpucacheMs: elapsedMs{v:0.2*til cacherows; acc:0f; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; acc};
emit["cpucache"; cacherows; cacherows*5; cacherows*5*16; cpucacheMs];

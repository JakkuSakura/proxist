/ pxd-kx-compare: aligned micro benchmarks (write/reread/randomread/cpu/cpucache)

rows: 10000000;
randreads: 1000000;
cacherows: 1000000;

rowbytes: 24

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
t_sym: sym rows;
t_ts: til rows;
t_val: (til rows) mod 10000f;
t0:.z.p;
trades: ([] sym:t_sym; ts: t_ts; val: t_val);
t1:.z.p;
writeMs: 1e-6 * (t1 - t0);
emit["write"; rows; rows; rows*rowbytes; writeMs];

/ reread
rereadMs: elapsedMs{sum trades`val};
emit["reread"; rows; rows; rows*rowbytes; rereadMs];

/ randomread
ridx: randreads ? rows;
randomMs: elapsedMs{sum (trades`val) ridx};
emit["randomread"; rows; randreads; randreads*rowbytes; randomMs];

/ cpu
cpuMs: elapsedMs{v: 0.1 * til rows; v: v*1.0001+0.1; sum v};
emit["cpu"; rows; rows; rows*16; cpuMs];

/ cpucache
cpucacheMs: elapsedMs{v:0.2*til cacherows; acc:0f; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; v:v*1.0001+0.2; acc+:sum v; acc};
emit["cpucache"; cacherows; cacherows*5; cacherows*5*16; cpucacheMs];

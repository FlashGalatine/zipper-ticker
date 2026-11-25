// Zipper verify:cs — compiles the REAL actions/*.cs with the Windows C# compiler and
// runs the three bus actions against a stand-in for `CPH` (tools/cs/CphStub.cs).
// Without this, no check executes the C# at all; the mock only copies it.
//
//   node verify-cs.mjs        (or: npm run verify:cs)
//
// Windows only: it needs csc.exe from the .NET Framework (set CSC to override the
// path). Elsewhere it says so and checks nothing. It is not part of `npm run verify`,
// and not part of the release gate.
//
// It proves that all four actions compile as C# 5 (the three bus actions against
// mscorlib alone; Ticker Sidecar Start with System.dll added, as its header tells the
// streamer to do), and that Ticker Command, Ticker Push and Ticker Announce relay,
// store, refuse and broadcast as tools/cs/ticker-cases.json says. verify.mjs runs the
// same table against the mock's copy of the actions. Ticker Sidecar Start is compiled
// and never run: its BUNDLE names a real folder, and a sidecar may be running from it.
// None of this replaces compiling the actions in Streamer.bot, whose own compiler and
// reference set have the final say.
//
// The programs are built into tools/cs/build/ (ignored by git), not the system temp
// folder, and each is started ONCE for all of its cases: an antivirus has removed a
// freshly compiled test program from the temp folder on this machine before. If one
// cannot be started, the error is printed, with the folder to allow.

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadTickerCases } from './tools/cs/ticker-cases.mjs';

const ROOT = dirname(fileURLToPath(import.meta.url));
const CSC = process.env.CSC || 'C:\\Windows\\Microsoft.NET\\Framework64\\v4.0.30319\\csc.exe';
const BUILD = join(ROOT, 'tools', 'cs', 'build');
const STUB = join(ROOT, 'tools', 'cs', 'CphStub.cs');
const HARNESS = join(ROOT, 'tools', 'cs', 'Harness.cs');

console.log('Zipper — the C# actions, compiled and run\n');
if (!existsSync(CSC)) {
  console.log(`SKIPPED: no C# compiler at ${CSC} (Windows with the .NET Framework is needed; set CSC to its path). The C# was not compiled and nothing was checked.`);
  process.exit(0);
}

let pass = 0, fail = 0;
function check(name, ok, detail) {
  if (ok) { pass++; console.log(`  ✔ ${name}`); }
  else { fail++; console.log(`  ✘ ${name}${detail ? ` — ${detail}` : ''}`); }
}
function stop(message) {
  console.error('\n' + message);
  fail++;
  console.log(`\nSTOPPED — ${pass} passed, ${fail} failed`);
  process.exit(1);
}

// ── Build ────────────────────────────────────────────────────────────────────
// Streamer.bot injects the `CPH` member itself; here the class is given a field of the
// stub's type. Nothing else in the action's text is changed.
rmSync(BUILD, { recursive: true, force: true });
mkdirSync(BUILD, { recursive: true });

function prepare(file) {
  const src = readFileSync(join(ROOT, 'actions', file), 'utf8').replace(/\r\n/g, '\n');
  const marker = 'public class CPHInline\n{';
  if (!src.includes(marker)) stop(`${file} does not declare \`public class CPHInline\`; the harness cannot give it a CPH.`);
  const out = join(BUILD, file);
  writeFileSync(out, src.replace(marker, marker + '\n    public CphStub CPH;'), 'utf8');
  return { src, path: out };
}

// /noconfig: no default reference list, so only mscorlib (and what /r: adds) is there.
// /codepage:65001: the action files are UTF-8 with no byte-order mark, and some hold
// em dashes inside string literals.
function compile(files, target, out, refs = []) {
  const r = spawnSync(CSC, ['/nologo', '/noconfig', '/codepage:65001', '/warnaserror-', `/target:${target}`, `/out:${out}`,
    ...refs.map((x) => `/r:${x}`), ...files], { encoding: 'utf8' });
  if (r.error) stop('The compiler could not be started: ' + r.error.message);
  return { ok: r.status === 0 && existsSync(out), text: (r.stdout || '') + (r.stderr || '') };
}

console.log('[1] compile');
const BUS = [
  { file: 'ticker-command.cs', action: 'Ticker Command', exe: 'ticker-command.exe' },
  { file: 'ticker-push.cs', action: 'Ticker Push', exe: 'ticker-push.exe' },
  { file: 'ticker-announce.cs', action: 'Ticker Announce', exe: 'ticker-announce.exe' },
];
const sources = {};
for (const a of BUS) {
  const { src, path } = prepare(a.file);
  sources[a.action] = src;
  a.exePath = join(BUILD, a.exe);
  const built = compile([STUB, HARNESS, path], 'exe', a.exePath);
  check(`${a.file} compiles as C# 5 with mscorlib alone`, built.ok, built.text);
  check(`${a.file}: and the compiler gives no warning`, built.ok && !/warning CS/.test(built.text), built.text);
  if (!built.ok) stop(`Nothing was run: ${a.file} did not compile.`);
}
{
  const { path } = prepare('ticker-sidecar-start.cs');
  const withSystem = compile([STUB, path], 'library', join(BUILD, 'ticker-sidecar-start.dll'), ['System.dll']);
  check('ticker-sidecar-start.cs compiles as C# 5 with System.dll added (the References step in its header); it is never run', withSystem.ok && !/warning CS/.test(withSystem.text), withSystem.text);
  const without = compile([STUB, path], 'library', join(BUILD, 'ticker-sidecar-start-nosystem.dll'));
  check('ticker-sidecar-start.cs does not compile with mscorlib alone (CS0246), as its header warns', !without.ok && /CS0246/.test(without.text), without.text.slice(0, 300));
}

// ── The table, and a few calls only the C# can make ──────────────────────────
const table = loadTickerCases();
console.log('\n[2] tools/cs/ticker-cases.json against the compiled actions');
{
  const m = /const string EMPTY_UPDATE = "((?:[^"\\]|\\.)*)";/.exec(sources['Ticker Push']);
  const csEmpty = m ? JSON.parse('"' + m[1] + '"') : null;
  check('ticker-push.cs\'s EMPTY_UPDATE is the table\'s emptyUpdate', csEmpty === table.emptyUpdate, String(csEmpty));
  check('ticker-push.cs refuses with the table\'s refusedMessage', sources['Ticker Push'].includes(`throw new Exception(${JSON.stringify(table.refusedMessage)})`));
}

const b64 = (text) => Buffer.from(text, 'utf8').toString('base64');
const runLine = (args) => {
  const parts = Object.entries(args).map(([k, v]) => `${k}=${b64(v)}`);
  return parts.length ? '#run64 ' + parts.join('\t') : '(no args)';
};
const unesc = (v) => v.replace(/\\(.)/g, (_, ch) => ({ t: '\t', n: '\n', r: '\r', e: '\u001e' }[ch] || ch));
function readRecords(file) {
  return readFileSync(file, 'utf8').split('\n').filter(Boolean).map((text) => {
    const rec = { g: {} };
    for (const part of text.split('\t')) {
      const e = part.indexOf('='), k = part.slice(0, e), v = part.slice(e + 1);
      if (k === 'ret') rec.ret = v === 'true';
      else if (k === 'warn') rec.warn = Number(v);
      else if (k === 'info') rec.info = Number(v);
      else if (k === 'log') rec.log = v ? v.split('\u001e') : [];
      else if (k === 'bc') rec.bc = v ? v.split('\u001e').map(unesc) : [];
      else rec.g[k] = unesc(v);
    }
    return rec;
  });
}
const short = (r) => { const s = JSON.stringify(r) || ''; return s.length > 600 ? s.slice(0, 600) + '…' : s; };

// Extra calls, after the table's rows, for what the mock cannot stand for.
const EXTRA = {
  'Ticker Push': [
    {
      name: 'writing the stored payload fails (SetGlobalVar made to throw): false, one warning that carries the exception, ticker:error with its message, the stored payload untouched',
      lines: ['#reset', '#global64 ticker.payload=' + b64(table.storedBefore), '#throwtext64=' + b64(table.throwMessage), '#throwset=ticker.payload', runLine({ tickerPayload: '{"n":9}' })],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && /^\[Ticker Push\] ERROR: System\.InvalidOperationException: stub: this run was made to fail/.test(r.log[0])
        && r.bc.length === 1 && r.bc[0] === table.errorWire(table.throwMessage) && r.g['ticker.payload'] === table.storedBefore,
    },
  ],
  'Ticker Command': [
    {
      name: 'the info line is "[Ticker Command] " and the JSON it broadcast',
      lines: ['#reset', runLine({ command: 'status' })],
      test: (r) => r.ret === true && r.info === 1 && r.log[0] === '[Ticker Command] {"type":"ticker:command","command":"status","value":""}',
    },
  ],
  'Ticker Announce': [
    {
      name: 'a skipped announcement logs one warning and broadcasts nothing',
      lines: ['#reset', '(no args)'],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && /^\[Ticker Announce\] no text and nothing to compose from/.test(r.log[0]) && r.bc.length === 0,
    },
  ],
};

for (const a of BUS) {
  const rows = table.rows.filter((r) => r.action === a.action);
  const lines = [];
  for (const row of rows) {
    lines.push('#reset');
    if (row.start !== null) lines.push('#global64 ticker.payload=' + b64(row.start));
    if (row.throw) lines.push('#throwtext64=' + b64(table.throwMessage), '#throwarg=' + row.throw);
    lines.push(runLine(row.args));
  }
  for (const x of EXTRA[a.action]) lines.push(...x.lines);
  const script = join(BUILD, a.exe.replace('.exe', '-script.txt'));
  const out = join(BUILD, a.exe.replace('.exe', '-out.txt'));
  writeFileSync(script, lines.join('\n') + '\n', 'utf8');

  const ran = spawnSync(a.exePath, [script, out], { encoding: 'utf8' });
  if (ran.error) {
    stop(`The compiled program could not be started: ${ran.error.message}\n(${a.exePath})\n`
      + (existsSync(a.exePath) ? 'The file is still there.' : `The file is gone: an antivirus most likely removed it after it was compiled. Allow the folder ${BUILD} and run again.`));
  }
  if (ran.status !== 0 || !existsSync(out)) stop(`${a.exe} ended with code ${ran.status} and ${existsSync(out) ? 'an' : 'no'} output file.\n${ran.stderr || ran.stdout || ''}`);
  const records = readRecords(out);
  check(`${a.action}: one result for each of the ${rows.length + EXTRA[a.action].length} calls`, records.length === rows.length + EXTRA[a.action].length, `${records.length} results`);

  const wrong = [];
  rows.forEach((row, i) => {
    const r = records[i];
    const ok = !!r && r.ret === row.ret
      && (row.ret ? r.warn === 0 && r.info === 1 : r.warn === 1 && r.info === 0)
      && (row.broadcast === null ? r.bc.length === 0 : r.bc.length === 1 && r.bc[0] === row.broadcast)
      && (a.action !== 'Ticker Push' || (row.stored === null ? !('ticker.payload' in r.g) : r.g['ticker.payload'] === row.stored));
    if (!ok) wrong.push(`"${row.name}": wanted ret=${row.ret} broadcast=${row.broadcast === null ? 'none' : row.broadcast}${a.action === 'Ticker Push' ? ` stored=${row.stored}` : ''}; got ${short(r)}`);
  });
  check(`${a.action}: the compiled action answers every one of the table's ${rows.length} rows as the table says`, wrong.length === 0, wrong.join(' || '));
  EXTRA[a.action].forEach((x, i) => {
    const r = records[rows.length + i];
    let good = false;
    try { good = !!r && x.test(r); } catch { good = false; }
    check(`${a.action}: ${x.name}`, good, short(r));
  });
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);

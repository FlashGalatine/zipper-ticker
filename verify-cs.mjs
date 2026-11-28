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
// store, refuse, log and broadcast as tools/cs/ticker-cases.json says (a trigger's
// Boolean and Int32 arguments passed as such). verify.mjs runs the same table against
// the mock's copy of the actions. Ticker Sidecar Start itself is compiled and never run:
// its port probe would reach the guard port of a sidecar running on this machine. A copy
// of it is run instead, with BUNDLE and GUARD_PORT read from tools/cs/LauncherHarness.cs:
// ports the system hands out and folders in tools/cs/build holding a stand-in
// zipper-sidecar.mjs, which node runs (hidden, as the action launches it) and which ends
// by itself. The rest of what it says is read from its text.
// None of this replaces compiling the actions in Streamer.bot, whose own compiler and
// reference set have the final say.
//
// The programs are built into tools/cs/build/ (ignored by git), not the system temp
// folder, and each is started ONCE for all of its cases: an antivirus has removed a
// freshly compiled test program from the temp folder on this machine before. If one
// cannot be started, the error is printed, with the folder to allow.

import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadTickerCases } from './tools/cs/ticker-cases.mjs';

const ROOT = dirname(fileURLToPath(import.meta.url));
const CSC = process.env.CSC || 'C:\\Windows\\Microsoft.NET\\Framework64\\v4.0.30319\\csc.exe';
const BUILD = join(ROOT, 'tools', 'cs', 'build');
const STUB = join(ROOT, 'tools', 'cs', 'CphStub.cs');
const HARNESS = join(ROOT, 'tools', 'cs', 'Harness.cs');
// The compiler and the compiled programs get the shell's environment without the API keys
// a developer may have set (in any casing: Windows matches environment names without
// regard to case). Neither needs them.
const CHILD_ENV = Object.fromEntries(Object.entries(process.env).filter(([k]) => !['CHALLONGE_API_KEY', 'STARTGG_API_KEY'].includes(k.toUpperCase())));

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
// The retries cover a stand-in sidecar of [1b] from a run just before, still ending.
rmSync(BUILD, { recursive: true, force: true, maxRetries: 25, retryDelay: 200 });
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
    ...refs.map((x) => `/r:${x}`), ...files], { encoding: 'utf8', env: CHILD_ENV });
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
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
let launcherSource = ''; // ticker-sidecar-start.cs, line ends as \n; [1b] runs a copy of it
let launchWaitMs = 0;    // its LAUNCH_WAIT_MS, read from its text
{
  const { src, path } = prepare('ticker-sidecar-start.cs');
  const withSystem = compile([STUB, path], 'library', join(BUILD, 'ticker-sidecar-start.dll'), ['System.dll']);
  check('ticker-sidecar-start.cs compiles as C# 5 with System.dll added (the References step in its header); it is never run', withSystem.ok && !/warning CS/.test(withSystem.text), withSystem.text);
  const without = compile([STUB, path], 'library', join(BUILD, 'ticker-sidecar-start-nosystem.dll'));
  // The error its header's References step names (the first CS code in that step) is the
  // first error this compile gives, and one of the three a missing System.dll can give.
  const refStep = (/^\/\/\s+3\. THE REFERENCES STEP[\s\S]*?(?=^\/\/\s+4\.)/m.exec(src) || [''])[0];
  const headerCode = (/CS\d{4}/.exec(refStep) || [''])[0];
  const firstError = (/error (CS\d{4})/.exec(without.text) || [])[1] || '';
  check(`ticker-sidecar-start.cs does not compile with mscorlib alone, and its first error is the one its header's References step names (${headerCode || 'none named'}; one of CS0246, CS0234 and CS0103)`,
    !without.ok && /^CS(0246|0234|0103)$/.test(firstError) && firstError === headerCode, JSON.stringify({ headerCode, firstError, text: without.text.slice(0, 300) }));

  // Tally's launcher pattern (row 9.3). The action is never run here: its port probe would
  // reach the guard port of a sidecar running on this machine. So what it says is read
  // from its text.
  const code = src.replace(/^\s*\/\/.*$/gm, ''); // the code, without its comment lines
  const bundle = (/const string BUNDLE = @"([^"]*)";/.exec(src) || [])[1];
  const portLine = (/^.*const int GUARD_PORT = (\d+);.*$/m.exec(src) || []);
  check('ticker-sidecar-start.cs: BUNDLE is a placeholder to edit, not a folder on the author\'s machine',
    bundle === 'C:\\path\\to\\zipper-ticker', String(bundle));
  check('ticker-sidecar-start.cs: it probes the sidecar\'s guard port 7496, and the line that names it says to change it there too when the sidecar is started with ZIPPER_GUARD_PORT',
    portLine[1] === '7496' && /ZIPPER_GUARD_PORT/.test(portLine[0] || ''), portLine[0]);
  const lit = (re) => (re.exec(code) || [''])[0];
  const exists = lit(/File\.Exists\([^;]*\)[\s\S]*?LogWarn\([^;]*;/);
  check('ticker-sidecar-start.cs: before the launch it checks that BUNDLE holds zipper-sidecar.mjs, and the warning when it does not names BUNDLE',
    /zipper-sidecar\.mjs/.test(exists) && /BUNDLE/.test(exists.slice(exists.indexOf('LogWarn'))), exists);
  const already = lit(/if \(PortOpen\(GUARD_PORT\)\)[\s\S]*?return true;/);
  check('ticker-sidecar-start.cs: a port that already answers before the launch starts nothing, and the line says to close the old node process (no /shutdown: the guard is a bare TCP listener)',
    already.indexOf('Process.Start') < 0 && /already answers/.test(already) && /node/.test(already) && /close/i.test(already) && !/shutdown/i.test(already), already);
  const after = code.slice(code.indexOf('Process.Start'));
  check('ticker-sidecar-start.cs: after the launch it probes the port again (CPH.Wait); with no answer it warns and names npm install and start-zipper.bat',
    /CPH\.Wait\(/.test(after) && /PortOpen\(GUARD_PORT\)/.test(after) && /LogWarn\([^;]*npm install[^;]*start-zipper\.bat/.test(after), after.slice(0, 900));
  check('ticker-sidecar-start.cs: the line after a launch that answers says only that the sidecar started and holds its guard port, not that it loaded its config or reached Streamer.bot',
    /LogInfo\([^;]*guard port[^;]*not[^;]*config[^;]*Streamer\.bot/.test(after), after.slice(0, 900));
  // The order of the three steps (review of Sprint 9): the probe for a running sidecar, then
  // the BUNDLE check, then the launch. The checks above find each anywhere in the code.
  const at = (s) => code.indexOf(s);
  check('ticker-sidecar-start.cs: the steps come in order: the probe for a sidecar already running, then the check that BUNDLE holds zipper-sidecar.mjs, then the launch',
    at('if (PortOpen(GUARD_PORT))') >= 0 && at('if (PortOpen(GUARD_PORT))') < at('File.Exists(') && at('File.Exists(') < at('Process.Start(')
      && code.split('Process.Start(').length === 2 && code.split('File.Exists(').length === 2,
    JSON.stringify({ probe: at('if (PortOpen(GUARD_PORT))'), exists: at('File.Exists('), start: at('Process.Start(') }));
  // The wait after the launch is LAUNCH_WAIT_MS on the clock, and every text that names it
  // gives the same figure.
  const waitMs = Number((/const int LAUNCH_WAIT_MS = (\d+);/.exec(code) || [])[1]);
  const flatText = (s) => s.replace(/\s+/g, ' ');
  const header = flatText(src.split('\n').filter((l) => /^\s*\/\//.test(l)).map((l) => l.replace(/^\s*\/\/\s?/, '')).join(' '));
  const readme = flatText(readFileSync(join(ROOT, 'README.md'), 'utf8'));
  // The warning when nothing answers gives the figure from LAUNCH_WAIT_MS, not from the time
  // measured (which could round up past it).
  const noAnswer = lit(/LogWarn\([^;]*nothing answers on port[^;]*;/);
  check('ticker-sidecar-start.cs: the wait after the launch is LAUNCH_WAIT_MS (3000) timed by a Stopwatch, its warning when nothing answers names that figure from LAUNCH_WAIT_MS (not from the time measured), and its header and the README name the same figure ("about 3 seconds")',
    waitMs === 3000 && /var waited = Stopwatch\.StartNew\(\);\s*while \(!up && waited\.ElapsedMilliseconds < LAUNCH_WAIT_MS\)/.test(code)
      && /after about " \+ \(LAUNCH_WAIT_MS \+ 500\) \/ 1000 \+ " seconds/.test(noAnswer) && !/waited/.test(noAnswer)
      && header.includes(`LAUNCH_WAIT_MS (about ${waitMs / 1000} seconds)`) && readme.includes(`within about ${waitMs / 1000} seconds`),
    JSON.stringify({ waitMs, noAnswer }));
  launcherSource = src;
  launchWaitMs = Number.isInteger(waitMs) ? waitMs : 0;
}

// ── Ticker Sidecar Start, run ───────────────────────────────────────────────
// A copy of the action whose BUNDLE and GUARD_PORT read the harness's own folder and port
// (tools/cs/LauncherHarness.cs), so it never probes the sidecar's guard port 7496 and
// launches only a stand-in zipper-sidecar.mjs the suite writes into tools/cs/build. Each
// port is one the system hands out, never 7474, 7495 to 7499 or 8080.
{
  console.log('\n[1b] Ticker Sidecar Start, run on a port and a folder of its own');
  const declBundle = 'const string BUNDLE = @"C:\\path\\to\\zipper-ticker";';
  const declPort = 'const int GUARD_PORT = 7496;';
  const count = (s, part) => s.split(part).length - 1;
  const copyText = launcherSource
    .replace(declBundle, 'static string BUNDLE { get { return LauncherHarness.Bundle; } }')
    .replace(declPort, 'static int GUARD_PORT { get { return LauncherHarness.Port; } }');
  const copyCode = copyText.replace(/^\s*\/\/.*$/gm, '').replace(/\/\/.*$/gm, '');
  const copyOk = count(launcherSource, declBundle) === 1 && count(launcherSource, declPort) === 1 && !copyCode.includes('7496');
  check('the run copy of ticker-sidecar-start.cs replaces BUNDLE and GUARD_PORT (each found once), and its code names port 7496 nowhere', copyOk);
  if (!copyOk) stop('Nothing of Ticker Sidecar Start was run: its copy could not be made safely.');
  const copyPath = join(BUILD, 'ticker-sidecar-start-run.cs');
  writeFileSync(copyPath, copyText.replace('public class CPHInline\n{', 'public class CPHInline\n{\n    public CphStub CPH;'), 'utf8');
  const runExe = join(BUILD, 'ticker-sidecar-start-run.exe');
  const built = compile([STUB, join(ROOT, 'tools', 'cs', 'LauncherHarness.cs'), copyPath], 'exe', runExe, ['System.dll']);
  check('the run copy of ticker-sidecar-start.cs compiles with the launcher harness', built.ok && !/warning CS/.test(built.text), built.text);
  if (!built.ok) stop('Nothing of Ticker Sidecar Start was run: its copy did not compile.');

  const FORBIDDEN = new Set([7474, 7495, 7496, 7497, 7498, 7499, 8080]);
  // A port nothing holds: one the system hands out, then let go.
  const freePort = () => new Promise((res, rej) => {
    const s = createServer();
    s.once('error', rej);
    s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => (FORBIDDEN.has(p) ? freePort().then(res, rej) : res(p))); });
  });
  // A folder for one case. With a stand-in sidecar it writes started.txt when node runs it;
  // given a port, it takes that port after 400 ms, as the sidecar takes its guard port, and
  // ends once something has connected. Either way it ends by itself (in 4 s at most), and
  // writes ended.txt as it does.
  const bundle = (name, { script = false, bindPort = 0 } = {}) => {
    const dir = join(BUILD, `launcher-${name}`);
    mkdirSync(dir, { recursive: true });
    if (script) {
      writeFileSync(join(dir, 'zipper-sidecar.mjs'), [
        "import { writeFileSync } from 'node:fs';",
        "import { createServer } from 'node:net';",
        "process.on('exit', () => { try { writeFileSync(new URL('./ended.txt', import.meta.url), 'ended'); } catch {} });",
        "writeFileSync(new URL('./started.txt', import.meta.url), String(process.pid));",
        bindPort ? `setTimeout(() => { const s = createServer((c) => { c.destroy(); setTimeout(() => process.exit(0), 100); }); s.on('error', () => process.exit(1)); s.listen(${bindPort}, '127.0.0.1'); }, 400);` : 'process.exit(0);',
        'setTimeout(() => process.exit(0), 4000);',
        '',
      ].join('\n'), 'utf8');
    }
    return dir;
  };
  // Until a stand-in sidecar that node ran has ended (so it holds no port and no folder).
  const ended = async (dir) => { for (let i = 0; i < 60 && !existsSync(join(dir, 'ended.txt')); i++) await sleep(100); return existsSync(join(dir, 'ended.txt')); };
  const runLauncher = (port, dir) => new Promise((res) => {
    const out = join(dir, 'result.txt');
    const child = spawn(runExe, [String(port), dir, out], { env: CHILD_ENV, stdio: 'ignore', windowsHide: true });
    const kill = setTimeout(() => child.kill(), 20000);
    child.on('error', (e) => { clearTimeout(kill); res({ error: e.message }); });
    child.on('exit', (code) => {
      clearTimeout(kill);
      if (!existsSync(out)) return res({ error: `ended with code ${code} and no result` });
      const r = {};
      for (const part of readFileSync(out, 'utf8').replace(/\n$/, '').split('\t')) {
        const e = part.indexOf('='), k = part.slice(0, e), v = part.slice(e + 1);
        r[k] = k === 'ret' ? v === 'true' : ['ms', 'warn', 'info'].includes(k) ? Number(v) : k === 'warns' || k === 'infos' ? (v ? v.split('\u001e') : []) : v;
      }
      res(r);
    });
  });
  const started = (dir) => existsSync(join(dir, 'started.txt'));
  const brief = (r) => JSON.stringify(r).slice(0, 700);

  // (a) Something already answers: nothing is started, and the line says what it may be.
  {
    const holder = createServer((c) => c.destroy());
    const port = await new Promise((res) => holder.listen(0, '127.0.0.1', () => res(holder.address().port)));
    const dir = bundle('open', { script: true });
    let r;
    try { r = await runLauncher(port, dir); } finally { holder.close(); }
    await sleep(500);
    check('Ticker Sidecar Start, run: a port that already answers starts nothing (true, at once), and the line says it is most likely a sidecar already running, else another program, and how to move the port (ZIPPER_GUARD_PORT and GUARD_PORT)',
      !FORBIDDEN.has(port) && r.ret === true && r.info === 1 && r.warn === 0 && r.ms < 1500 && !started(dir)
        && /^\[Ticker Sidecar Start\] port \d+ already answers, most likely a Zipper sidecar that is already running, so none was started\./.test(r.infos?.[0] || '')
        && /another program holds that port/.test(r.infos?.[0] || '') && /ZIPPER_GUARD_PORT/.test(r.infos?.[0] || '') && /GUARD_PORT at the top of this action/.test(r.infos?.[0] || ''),
      brief({ port, r, started: started(dir) }));
  }
  // (b) BUNDLE without zipper-sidecar.mjs: one warning that names BUNDLE, before any launch.
  {
    const port = await freePort();
    const dir = bundle('none');
    const r = await runLauncher(port, dir);
    check('Ticker Sidecar Start, run: a BUNDLE without zipper-sidecar.mjs gives false and one warning that names that BUNDLE, at once (no launch, no wait)',
      r.ret === false && r.warn === 1 && r.info === 0 && r.ms < 1500
        && (r.warns?.[0] || '').startsWith(`[Ticker Sidecar Start] zipper-sidecar.mjs was not found in BUNDLE = ${dir}`),
      brief({ port, r }));
  }
  // (c) node starts, and nothing ever takes the port (a sidecar that exits before its guard,
  // as one with no `ws` package does): the wait is LAUNCH_WAIT_MS on the clock, however
  // long each probe takes (at least that long, and well short of a count of 20 probes), and
  // the warning names that figure. The figure comes from LAUNCH_WAIT_MS, not from the time
  // measured, so a slow probe cannot turn "about 3 seconds" into "about 4 seconds".
  {
    const port = await freePort();
    const dir = bundle('silent', { script: true });
    const r = await runLauncher(port, dir);
    await ended(dir);
    const secs = launchWaitMs / 1000;
    check(`Ticker Sidecar Start, run: node is launched and nothing takes the port: false after LAUNCH_WAIT_MS on the clock (${launchWaitMs} to 5000 ms; it took ${r.ms} ms), one warning that says "after about ${secs} seconds", that it may still be starting, and names npm install and start-zipper.bat`,
      launchWaitMs > 0 && started(dir) && r.ret === false && r.warn === 1 && r.info === 0 && r.ms >= launchWaitMs && r.ms <= 5000
        && (r.warns?.[0] || '').startsWith(`[Ticker Sidecar Start] node was launched in ${dir} but nothing answers on port ${port} after about ${secs} seconds. It may still be starting.`)
        && /npm install/.test(r.warns?.[0] || '') && /start-zipper\.bat/.test(r.warns?.[0] || ''),
      brief({ port, r, started: started(dir) }));
  }
  // (d) node starts and takes the port 400 ms later: true as soon as it answers.
  {
    const port = await freePort();
    const dir = bundle('binds', { script: true, bindPort: port });
    const r = await runLauncher(port, dir);
    await ended(dir); // it ends once the launcher's probe has connected
    check(`Ticker Sidecar Start, run: node is launched and takes the port 400 ms later: true as soon as it answers (it took ${r.ms} ms), one info line that says it holds its guard port and no more`,
      started(dir) && r.ret === true && r.info === 1 && r.warn === 0 && r.ms < 2800
        && (r.infos?.[0] || '').startsWith(`[Ticker Sidecar Start] launched node zipper-sidecar.mjs in ${dir}: it started and holds its guard port ${port}.`),
      brief({ port, r, started: started(dir) }));
  }
}

// ── The table, and a few calls only the C# can make ──────────────────────────
const table = loadTickerCases();
console.log('\n[2] tools/cs/ticker-cases.json against the compiled actions');
{
  const constOf = (src, name) => { const m = new RegExp(`const string ${name} = "((?:[^"\\\\]|\\\\.)*)";`).exec(src); return m ? JSON.parse('"' + m[1] + '"') : null; };
  const csEmpty = constOf(sources['Ticker Push'], 'EMPTY_UPDATE');
  check('ticker-push.cs\'s EMPTY_UPDATE is the table\'s emptyUpdate', csEmpty === table.emptyUpdate, String(csEmpty));
  check('ticker-push.cs refuses a payload with the table\'s refusedCode and refusedMessage (row 9.4), and throws nothing for it',
    constOf(sources['Ticker Push'], 'BAD_PAYLOAD_CODE') === table.refusedCode && constOf(sources['Ticker Push'], 'BAD_PAYLOAD_MESSAGE') === table.refusedMessage
      && !/throw new/.test(sources['Ticker Push']));
  const wrongText = BUS.filter((a) => constOf(sources[a.action], 'EXCEPTION_MESSAGE') !== table.exceptionMessages[a.action]).map((a) => a.file);
  check('each bus action\'s EXCEPTION_MESSAGE is the table\'s fixed text for it (row 9.4)', wrongText.length === 0, wrongText.join(', '));
  const listM = /static readonly string\[\] CHAT_ALLOWED = \{([^}]*)\};/.exec(sources['Ticker Command']);
  const csList = listM ? listM[1].split(',').map((s) => s.trim().replace(/^"|"$/g, '')) : null;
  check('ticker-command.cs\'s CHAT_ALLOWED is the table\'s chatAllowed: start, stop, pollNow, status, setTopN (decision 1)',
    JSON.stringify(csList) === JSON.stringify(table.chatAllowed) && JSON.stringify(table.chatAllowed) === JSON.stringify(['start', 'stop', 'pollNow', 'status', 'setTopN']),
    JSON.stringify(csList));
}

const b64 = (text) => Buffer.from(text, 'utf8').toString('base64');
// One Execute() with these arguments: strings, except those named in types, which the
// harness passes as a Boolean ('bool') or an Int32 ('int').
const runLine = (args, types = {}) => {
  const parts = Object.entries(args).map(([k, v]) => `${k}${types[k] ? ':' + types[k] : ''}=${b64(v)}`);
  return parts.length ? '#run64 ' + parts.join('\t') : '(no args)';
};
const unesc = (v) => v.replace(/\\(.)/g, (_, ch) => ({ t: '\t', n: '\n', r: '\r', e: '\u001e' }[ch] || ch));
function readRecords(file) {
  return readFileSync(file, 'utf8').split('\n').filter(Boolean).map((text) => {
    const rec = { g: {} };
    for (const part of text.split('\t')) {
      const e = part.indexOf('='), k = part.slice(0, e), v = part.slice(e + 1);
      if (k === 'ret') rec.ret = v === 'true';
      else if (k === 'threw') rec.threw = v;
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

// Extra calls, after the table's rows, for what the mock cannot stand for. Each is one or
// more Execute() lines (`runs`, 1 unless it says), and its test gets their records.
const oneLine = (s) => s.replace(/[\r\n\t]/g, ' ');
// The warning a run that threw logs: the exception itself, its message included.
const thrownLog = (action, r) => !!r && typeof r.log[0] === 'string'
  && r.log[0].startsWith(`[${action}] ERROR: System.InvalidOperationException: ${oneLine(table.throwMessage)}`);
// The exception's message never reaches a broadcast (row 9.4).
const noStub = (r) => r.bc.every((b) => !b.includes('this run was made to fail'));
const REFUSED_TAIL = '"; chat may send only start, stop, pollNow, status and setTopN, so nothing was relayed';
const EXTRA = {
  'Ticker Push': [
    {
      name: 'writing the stored payload fails (SetGlobalVar made to throw): false, one warning that carries the exception, ticker:error with code exception and the fixed text (not the exception\'s message), the stored payload untouched',
      lines: ['#reset', '#global64 ticker.payload=' + b64(table.storedBefore), '#throwtext64=' + b64(table.throwMessage), '#throwset=ticker.payload', runLine({ tickerPayload: '{"n":9}' })],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && thrownLog('Ticker Push', r)
        && r.bc.length === 1 && r.bc[0] === table.errorWire('exception', table.exceptionMessages['Ticker Push'], 'Ticker Push') && noStub(r) && r.g['ticker.payload'] === table.storedBefore,
    },
    {
      name: 'a refused payload logs one warning that names the code and the payload\'s length, never its text',
      lines: ['#reset', runLine({ tickerPayload: 'abc-secret-text' })],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && /^\[Ticker Push\] refused \(bad-payload\): tickerPayload is not a JSON object \(15 chars\)/.test(r.log[0])
        && !r.log[0].includes('abc-secret-text'),
    },
    {
      name: 'the ticker:error broadcast in the catch block throws as well (every broadcast made to fail): nothing escapes Execute(), which returns false after its one warning (its info line came before the broadcast that threw)',
      lines: ['#reset', '#throwtext64=' + b64(table.throwMessage), '#throwbc=1', runLine({ tickerPayload: '{"n":10}' })],
      test: (r) => r.threw === undefined && r.ret === false && r.warn === 1 && r.info === 1 && thrownLog('Ticker Push', r) && r.log[1] === '[Ticker Push] broadcasting 8 bytes' && r.bc.length === 0,
    },
  ],
  'Ticker Command': [
    {
      name: 'the info line is "[Ticker Command] " and the JSON it broadcast',
      lines: ['#reset', runLine({ command: 'start' })],
      test: (r) => r.ret === true && r.info === 1 && r.log[0] === '[Ticker Command] {"type":"ticker:command","command":"start","value":""}',
    },
    {
      name: 'a refused chat command logs ONE warning that quotes what chat sent, cut to 80 characters and "…", names the five commands chat may send, and broadcasts nothing',
      lines: ['#reset', runLine({ command: '!ticker', commandId: 'x', rawInput: 'setUrl https://' + 'a'.repeat(200) + 'TAIL' })],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && r.bc.length === 0
        && r.log[0] === '[Ticker Command] refused from chat: "' + ('setUrl https://' + 'a'.repeat(200)).slice(0, 80) + '…' + REFUSED_TAIL,
    },
    {
      name: 'a refused chat command\'s warning shows a control character or a line separator as a space, and never cuts an emoji in half',
      lines: ['#reset', runLine({ command: '!ticker', commandId: 'x', rawInput: 'setUrl a\u0001b\u2028c ' + 'e'.repeat(66) + '\u{1F600}tail' })],
      test: (r) => r.ret === false && r.warn === 1 && !/[\u0001\u2028]/.test(r.log[0])
        && r.log[0] === '[Ticker Command] refused from chat: "setUrl a b c ' + 'e'.repeat(66) + '…' + REFUSED_TAIL,
    },
    {
      name: 'a refused rawInput with no command argument (a Stream Deck button that sets only rawInput) logs ONE warning that says it came that way, not from chat, names the five commands, and says to set the command and value arguments instead',
      lines: ['#reset', runLine({ rawInput: 'setCapsText Finals tonight' })],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && r.bc.length === 0
        && r.log[0] === '[Ticker Command] refused from a rawInput with no command argument: "setCapsText Finals tonight"; that way, as from chat, only start, stop, pollNow, status and setTopN are relayed, so nothing was. A Stream Deck or a Set Argument setup should set the command and value arguments instead',
    },
    {
      name: 'the ticker:error broadcast in the catch block throws as well: nothing escapes Execute(), which returns false after its one warning',
      lines: ['#reset', '#throwtext64=' + b64(table.throwMessage), '#throwarg=rawInput', '#throwbc=1', runLine({ command: 'start' })],
      test: (r) => r.threw === undefined && r.ret === false && r.warn === 1 && r.info === 0 && thrownLog('Ticker Command', r) && r.bc.length === 0,
    },
  ],
  'Ticker Announce': [
    {
      name: 'a skipped announcement logs one warning and broadcasts nothing',
      lines: ['#reset', '(no args)'],
      test: (r) => r.ret === false && r.warn === 1 && r.info === 0 && /^\[Ticker Announce\] no text and nothing to compose from/.test(r.log[0]) && r.bc.length === 0,
    },
    {
      name: 'a gift sub that is part of a gift bomb logs one info line that says why it is skipped',
      lines: ['#reset', runLine({ user: 'Gifter', recipientUser: 'R1', fromGiftBomb: 'true', subBombCount: '3' }, { fromGiftBomb: 'bool', subBombCount: 'int' })],
      test: (r) => r.ret === true && r.warn === 0 && r.info === 1 && r.bc.length === 0 && /^\[Ticker Announce\] a gift sub from a gift bomb: skipped/.test(r.log[0]),
    },
    {
      name: 'one Gift Bomb run, then its 3 Gift Subscription runs (fromGiftBomb true): ONE broadcast in all, "Gifter gifted 3 subs!"',
      runs: 4,
      lines: ['#reset', runLine({ user: 'Gifter', gifts: '3', totalGifts: '7' }, { gifts: 'int', totalGifts: 'int' }),
        ...[1, 2, 3].map((n) => runLine({ user: 'Gifter', recipientUser: 'R' + n, fromGiftBomb: 'true', subBombCount: '3', monthsGifted: '1' }, { fromGiftBomb: 'bool', subBombCount: 'int' }))],
      test: (r, rs) => rs.flatMap((x) => x.bc).length === 1 && rs[0].bc[0] === '{"type":"ticker:announce","kind":"sub","text":"Gifter gifted 3 subs!","duration":8}'
        && rs.every((x) => x.ret === true && x.warn === 0),
    },
    {
      name: 'the 3 Gift Subscription runs first, then the Gift Bomb run (the order in a live bomb was not checked): still ONE broadcast',
      runs: 4,
      lines: ['#reset', ...[1, 2, 3].map((n) => runLine({ user: 'Gifter', recipientUser: 'R' + n, fromGiftBomb: 'true', subBombCount: '3' }, { fromGiftBomb: 'bool', subBombCount: 'int' })),
        runLine({ user: 'Gifter', gifts: '3', totalGifts: '7' }, { gifts: 'int', totalGifts: 'int' })],
      test: (r, rs) => rs.flatMap((x) => x.bc).length === 1 && rs[3].bc[0] === '{"type":"ticker:announce","kind":"sub","text":"Gifter gifted 3 subs!","duration":8}',
    },
    {
      name: 'the ticker:error broadcast in the catch block throws as well: nothing escapes Execute(), which returns false after its one warning',
      lines: ['#reset', '#throwtext64=' + b64(table.throwMessage), '#throwarg=kind', '#throwbc=1', runLine({ text: 'x' })],
      test: (r) => r.threw === undefined && r.ret === false && r.warn === 1 && r.info === 0 && thrownLog('Ticker Announce', r) && r.bc.length === 0,
    },
  ],
};
const runsOf = (x) => x.runs || 1;

for (const a of BUS) {
  const rows = table.rows.filter((r) => r.action === a.action);
  const lines = [];
  for (const row of rows) {
    lines.push('#reset');
    if (row.start !== null) lines.push('#global64 ticker.payload=' + b64(row.start));
    if (row.throw) lines.push('#throwtext64=' + b64(table.throwMessage), '#throwarg=' + row.throw);
    lines.push(runLine(row.args, row.types));
  }
  for (const x of EXTRA[a.action]) lines.push(...x.lines);
  const script = join(BUILD, a.exe.replace('.exe', '-script.txt'));
  const out = join(BUILD, a.exe.replace('.exe', '-out.txt'));
  writeFileSync(script, lines.join('\n') + '\n', 'utf8');

  const ran = spawnSync(a.exePath, [script, out], { encoding: 'utf8', env: CHILD_ENV });
  if (ran.error) {
    stop(`The compiled program could not be started: ${ran.error.message}\n(${a.exePath})\n`
      + (existsSync(a.exePath) ? 'The file is still there.' : `The file is gone: an antivirus most likely removed it after it was compiled. Allow the folder ${BUILD} and run again.`));
  }
  if (ran.status !== 0 || !existsSync(out)) stop(`${a.exe} ended with code ${ran.status} and ${existsSync(out) ? 'an' : 'no'} output file.\n${ran.stderr || ran.stdout || ''}`);
  const records = readRecords(out);
  const calls = rows.length + EXTRA[a.action].reduce((n, x) => n + runsOf(x), 0);
  check(`${a.action}: one result for each of the ${calls} calls`, records.length === calls, `${records.length} results`);

  // Each row: what it returns, how many info lines and warnings it logs (rows 9.1, 9.3 and
  // 9.4), what it broadcasts and stores, and that nothing escaped Execute(). A row that
  // throws logs a warning that carries the exception, and its broadcast does not.
  const wrong = [];
  rows.forEach((row, i) => {
    const r = records[i];
    const ok = !!r && r.threw === undefined && r.ret === row.ret && r.warn === row.warn && r.info === row.info
      && (row.broadcast === null ? r.bc.length === 0 : r.bc.length === 1 && r.bc[0] === row.broadcast)
      && (!row.throw || (thrownLog(a.action, r) && noStub(r)))
      && (a.action !== 'Ticker Push' || (row.stored === null ? !('ticker.payload' in r.g) : r.g['ticker.payload'] === row.stored));
    if (!ok) wrong.push(`"${row.name}": wanted ret=${row.ret} info=${row.info} warn=${row.warn} broadcast=${row.broadcast === null ? 'none' : row.broadcast}${a.action === 'Ticker Push' ? ` stored=${row.stored}` : ''}; got ${short(r)}`);
  });
  check(`${a.action}: the compiled action answers every one of the table's ${rows.length} rows as the table says (what it returns, logs, broadcasts and stores)`, wrong.length === 0, wrong.join(' || '));
  let at = rows.length;
  for (const x of EXTRA[a.action]) {
    const rs = records.slice(at, at + runsOf(x));
    at += runsOf(x);
    let good = false;
    try { good = rs.length === runsOf(x) && x.test(rs[0], rs); } catch { good = false; }
    check(`${a.action}: ${x.name}`, good, rs.map(short).join(' | '));
  }
}

console.log(`\n${fail === 0 ? 'ALL GREEN' : 'FAILURES'} — ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);

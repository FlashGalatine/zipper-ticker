// Reads tools/cs/ticker-cases.json and turns each row into what is sent and what must
// come out. Used by verify-cs.mjs (the compiled C#) and by verify.mjs (the mock's copy
// of the actions), so that both run the very same rows. Nothing here decides what an
// action does: that is written out in each row, and the two runs are checked against it.
//
//   const table = loadTickerCases();
//   table.rows    [{ action, name, args, types, start, throw, ret, broadcast, stored, info, warn, refused }]
//                 action    'Ticker Push' | 'Ticker Command' | 'Ticker Announce'
//                 args      every argument as a string
//                 types     { name: 'bool' | 'int' } for the arguments a trigger gives typed
//                 start     the stored payload before the row (Ticker Push), or null
//                 throw     the argument whose read throws, or null
//                 ret       what Execute() returns
//                 broadcast the exact text handed to WebsocketBroadcastJson, or null
//                 stored    the stored payload after the row (Ticker Push), or null
//                 info      how many info lines the run logs
//                 warn      how many warnings the run logs
//                 refused   true for a command chat may not send (the mock counts it)
//   table.typedArgs(row)   the row's arguments with the typed ones as a boolean or a number
//   table.jsonStr(s)       the actions' own JSON string encoder (JsonStr in the C#)
//   table.errorWire(code, message, action)   the ticker:error broadcast for a code, its text
//                 and the exact name of the action that sends it
//   table.chatAllowed, table.storedBefore, table.emptyUpdate, table.refusedCode,
//   table.refusedMessage, table.exceptionMessages, table.throwMessage

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

// JsonStr in the three actions: quotes and backslashes escaped, \b \f \n \r \t by name,
// every other character below a space as \u00xx (lower-case hex), the rest as it is.
export function jsonStr(s) {
  if (s == null) return '""';
  let out = '"';
  for (const c of String(s)) {
    switch (c) {
      case '"': out += '\\"'; break;
      case '\\': out += '\\\\'; break;
      case '\b': out += '\\b'; break;
      case '\f': out += '\\f'; break;
      case '\n': out += '\\n'; break;
      case '\r': out += '\\r'; break;
      case '\t': out += '\\t'; break;
      default:
        out += c < ' ' ? '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0') : c;
    }
  }
  return out + '"';
}

export function loadTickerCases() {
  const table = JSON.parse(readFileSync(join(HERE, 'ticker-cases.json'), 'utf8'));
  // ErrorJson in the actions: { type, code, message, action }, in that order, with action the
  // sending action's exact name, for every code (the owner's ruling of 2026-11-27).
  const ACTIONS = ['Ticker Push', 'Ticker Command', 'Ticker Announce'];
  const errorWire = (code, message, action) => {
    if (!ACTIONS.includes(action)) throw new Error(`ticker-cases.mjs: errorWire needs the sending action's exact name, not ${JSON.stringify(action)}`);
    return '{"type":"ticker:error","code":' + jsonStr(code) + ',"message":' + jsonStr(message) + ',"action":' + jsonStr(action) + '}';
  };
  const thrownWire = (action) => {
    const text = table.exceptionMessages && table.exceptionMessages[action];
    if (typeof text !== 'string' || !text) throw new Error(`ticker-cases.json: exceptionMessages has no text for ${action}`);
    return errorWire('exception', text, action);
  };
  const rows = [];
  const strings = (row, where) => {
    for (const [k, v] of Object.entries(row.args || {})) {
      if (typeof v !== 'string') throw new Error(`ticker-cases.json, ${where} "${row.name}": argument ${k} is not a string`);
    }
    for (const [k, t] of Object.entries(row.types || {})) {
      const v = (row.args || {})[k];
      if (t === 'bool' ? !/^(true|false)$/.test(v) : t === 'int' ? !/^-?\d+$/.test(v) : true) {
        throw new Error(`ticker-cases.json, ${where} "${row.name}": argument ${k} is not a ${t} (${JSON.stringify(v)})`);
      }
    }
    return row.args || {};
  };
  // What a row logs, unless it says otherwise: a run that relays, stores or announces
  // logs one info line; one that does not, one warning.
  const logs = (ret) => (ret ? { info: 1, warn: 0 } : { info: 0, warn: 1 });

  for (const r of table.push) {
    const args = strings(r, 'push');
    const before = r.start === 'empty' ? null : table.storedBefore;
    const out = { action: 'Ticker Push', name: r.name, args, types: r.types || {}, start: before, throw: r.throw || null, stored: before, refused: false };
    if (r.expect === 'accept') {
      if (typeof r.stores !== 'string') throw new Error(`ticker-cases.json, push "${r.name}": an accepted row needs "stores"`);
      Object.assign(out, { ret: true, broadcast: r.stores, stored: r.stores });
    } else if (r.expect === 'sync') Object.assign(out, { ret: true, broadcast: before ?? table.emptyUpdate });
    else if (r.expect === 'refused') Object.assign(out, { ret: false, broadcast: errorWire(table.refusedCode, table.refusedMessage, 'Ticker Push') });
    else if (r.expect === 'thrown') Object.assign(out, { ret: false, broadcast: thrownWire('Ticker Push') });
    else throw new Error(`ticker-cases.json, push "${r.name}": expect must be accept, sync, refused or thrown`);
    if ((r.expect === 'thrown') !== !!r.throw) throw new Error(`ticker-cases.json, push "${r.name}": a thrown row, and only a thrown row, names "throw"`);
    Object.assign(out, logs(out.ret));
    rows.push(out);
  }

  for (const r of table.command) {
    const args = strings(r, 'command');
    const out = { action: 'Ticker Command', name: r.name, args, types: r.types || {}, start: null, throw: r.throw || null, stored: null, refused: !!r.refused };
    if (r.refused && r.relay !== null) throw new Error(`ticker-cases.json, command "${r.name}": a refused row relays nothing`);
    if (r.quiet && r.relay === null) throw new Error(`ticker-cases.json, command "${r.name}": only a row that relays can be quiet`);
    if (r.throw) {
      if (r.relay !== null) throw new Error(`ticker-cases.json, command "${r.name}": a row that throws relays nothing`);
      Object.assign(out, { ret: false, broadcast: thrownWire('Ticker Command') });
    } else if (r.relay === null) Object.assign(out, { ret: false, broadcast: null });
    else Object.assign(out, { ret: true, broadcast: '{"type":"ticker:command","command":' + jsonStr(r.relay.command) + ',"value":' + jsonStr(r.relay.value) + '}' });
    Object.assign(out, logs(out.ret));
    if (r.quiet) out.info = 0;
    rows.push(out);
  }

  for (const r of table.announce) {
    const args = strings(r, 'announce');
    const out = { action: 'Ticker Announce', name: r.name, args, types: r.types || {}, start: null, throw: r.throw || null, stored: null, refused: false };
    if (r.partOfBomb && r.announce !== null) throw new Error(`ticker-cases.json, announce "${r.name}": a run that is part of a gift bomb announces nothing`);
    if (r.throw) {
      if (r.announce !== null) throw new Error(`ticker-cases.json, announce "${r.name}": a row that throws announces nothing`);
      Object.assign(out, { ret: false, broadcast: thrownWire('Ticker Announce') });
    } else if (r.announce === null) Object.assign(out, { ret: !!r.partOfBomb, broadcast: null });
    else {
      const a = r.announce;
      if (!Number.isInteger(a.duration)) throw new Error(`ticker-cases.json, announce "${r.name}": duration is not a whole number`);
      Object.assign(out, { ret: true, broadcast: '{"type":"ticker:announce","kind":' + jsonStr(a.kind) + ',"text":' + jsonStr(a.text) + ',"duration":' + a.duration + '}' });
    }
    // A run that is part of a gift bomb is handled, not failed: one info line.
    Object.assign(out, logs(out.ret));
    rows.push(out);
  }

  // A row's arguments as they go over the bus to the mock: typed ones as a JSON boolean
  // or number, as a Twitch trigger gives them; the rest as strings.
  const typedArgs = (row) => Object.fromEntries(Object.entries(row.args).map(([k, v]) => {
    const t = row.types && row.types[k];
    return [k, t === 'bool' ? v === 'true' : t === 'int' ? Number(v) : v];
  }));

  return {
    rows,
    typedArgs,
    chatAllowed: table.chatAllowed,
    storedBefore: table.storedBefore,
    emptyUpdate: table.emptyUpdate,
    refusedCode: table.refusedCode,
    refusedMessage: table.refusedMessage,
    exceptionMessages: table.exceptionMessages,
    throwMessage: table.throwMessage,
    jsonStr,
    errorWire,
  };
}

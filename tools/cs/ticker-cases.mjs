// Reads tools/cs/ticker-cases.json and turns each row into what is sent and what must
// come out. Used by verify-cs.mjs (the compiled C#) and by verify.mjs (the mock's copy
// of the actions), so that both run the very same rows. Nothing here decides what an
// action does: that is written out in each row, and the two runs are checked against it.
//
//   const table = loadTickerCases();
//   table.rows    [{ action, name, args, start, throw, ret, broadcast, stored }]
//                 action    'Ticker Push' | 'Ticker Command' | 'Ticker Announce'
//                 start     the stored payload before the row (Ticker Push), or null
//                 throw     the argument whose read throws, or null
//                 ret       what Execute() returns
//                 broadcast the exact text handed to WebsocketBroadcastJson, or null
//                 stored    the stored payload after the row (Ticker Push), or null
//   table.jsonStr(s)       the actions' own JSON string encoder (JsonStr in the C#)
//   table.errorWire(msg)   the ticker:error broadcast for a message
//   table.storedBefore, table.emptyUpdate, table.refusedMessage, table.throwMessage

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
  const errorWire = (message) => '{"type":"ticker:error","message":' + jsonStr(message) + '}';
  const rows = [];
  const strings = (row, where) => {
    for (const [k, v] of Object.entries(row.args || {})) {
      if (typeof v !== 'string') throw new Error(`ticker-cases.json, ${where} "${row.name}": argument ${k} is not a string`);
    }
    return row.args || {};
  };

  for (const r of table.push) {
    const args = strings(r, 'push');
    const before = r.start === 'empty' ? null : table.storedBefore;
    const out = { action: 'Ticker Push', name: r.name, args, start: before, throw: r.throw || null, stored: before };
    if (r.expect === 'accept') {
      if (typeof r.stores !== 'string') throw new Error(`ticker-cases.json, push "${r.name}": an accepted row needs "stores"`);
      Object.assign(out, { ret: true, broadcast: r.stores, stored: r.stores });
    } else if (r.expect === 'sync') Object.assign(out, { ret: true, broadcast: before ?? table.emptyUpdate });
    else if (r.expect === 'refused') Object.assign(out, { ret: false, broadcast: errorWire(table.refusedMessage) });
    else if (r.expect === 'thrown') Object.assign(out, { ret: false, broadcast: errorWire(table.throwMessage) });
    else throw new Error(`ticker-cases.json, push "${r.name}": expect must be accept, sync, refused or thrown`);
    if ((r.expect === 'thrown') !== !!r.throw) throw new Error(`ticker-cases.json, push "${r.name}": a thrown row, and only a thrown row, names "throw"`);
    rows.push(out);
  }

  for (const r of table.command) {
    const args = strings(r, 'command');
    const out = { action: 'Ticker Command', name: r.name, args, start: null, throw: r.throw || null, stored: null };
    if (r.throw) {
      if (r.relay !== null) throw new Error(`ticker-cases.json, command "${r.name}": a row that throws relays nothing`);
      Object.assign(out, { ret: false, broadcast: errorWire(table.throwMessage) });
    } else if (r.relay === null) Object.assign(out, { ret: false, broadcast: null });
    else Object.assign(out, { ret: true, broadcast: '{"type":"ticker:command","command":' + jsonStr(r.relay.command) + ',"value":' + jsonStr(r.relay.value) + '}' });
    rows.push(out);
  }

  for (const r of table.announce) {
    const args = strings(r, 'announce');
    const out = { action: 'Ticker Announce', name: r.name, args, start: null, throw: r.throw || null, stored: null };
    if (r.throw) {
      if (r.announce !== null) throw new Error(`ticker-cases.json, announce "${r.name}": a row that throws announces nothing`);
      Object.assign(out, { ret: false, broadcast: errorWire(table.throwMessage) });
    } else if (r.announce === null) Object.assign(out, { ret: false, broadcast: null });
    else {
      const a = r.announce;
      if (!Number.isInteger(a.duration)) throw new Error(`ticker-cases.json, announce "${r.name}": duration is not a whole number`);
      Object.assign(out, { ret: true, broadcast: '{"type":"ticker:announce","kind":' + jsonStr(a.kind) + ',"text":' + jsonStr(a.text) + ',"duration":' + a.duration + '}' });
    }
    rows.push(out);
  }

  return {
    rows,
    storedBefore: table.storedBefore,
    emptyUpdate: table.emptyUpdate,
    refusedMessage: table.refusedMessage,
    throwMessage: table.throwMessage,
    jsonStr,
    errorWire,
  };
}

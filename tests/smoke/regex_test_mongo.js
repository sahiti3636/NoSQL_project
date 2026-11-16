// Loads the sample as raw lines, then parses ONLY inside MongoDB with the contract regexes.
const fs = require('fs');
const C = n => fs.readFileSync('/contracts/' + n, 'utf8').replace(/\n$/, '');
const ENV = C('envelope.regex'), REQ = C('request.regex'), FB = C('request_fallback.regex');
const rows = C('months.tsv').split('\n').slice(1).map(l => l.split('\t'));
const NAMES = rows.map(r => r[0]), NUMS = rows.map(r => r[1]);
const buf = fs.readFileSync('/input/samples/input/edge_sample.log');
const docs = []; let start = 0, n = 0;
for (let i = 0; i <= buf.length; i++) {
  if (i === buf.length || buf[i] === 10) {
    if (i === buf.length && start === i) break;            // trailing newline makes no record
    let s = buf.subarray(start, i).toString('utf8');       // invalid bytes -> U+FFFD
    if (s.endsWith('\r')) s = s.slice(0, -1);
    docs.push({ line_no: ++n, raw: s }); start = i + 1;
  }
}
db = db.getSiblingDB('regex_test'); db.raw.drop(); db.raw.insertMany(docs);
const cap = (v, i) => ({ $arrayElemAt: [v, i] });
db.raw.aggregate([
  { $set: { e: { $regexFind: { input: '$raw', regex: ENV } } } },
  { $set: { c: '$e.captures' } },
  { $set: { mi: { $cond: [{ $eq: ['$e', null] }, -1, { $indexOfArray: [NAMES, cap('$c', 4)] }] } } },
  { $set: { ok: { $gte: ['$mi', 0] } } },
  { $set: { r: { $cond: ['$ok', { $regexFind: { input: cap('$c', 10), regex: REQ } }, null] },
            f: { $cond: ['$ok', { $regexFind: { input: cap('$c', 10), regex: FB } }, null] } } },
  { $sort: { line_no: 1 } },
  { $project: { _id: 0, v: { $cond: ['$ok', [
      'OK', cap('$c', 0),
      { $concat: [cap('$c', 5), '-', { $arrayElemAt: [NUMS, '$mi'] }, '-', cap('$c', 3)] },
      { $toString: { $toInt: cap('$c', 6) } },
      { $cond: ['$r', cap('$r.captures', 0), ''] },
      { $cond: ['$r', cap('$r.captures', 1), cap('$f.captures', 0)] },
      { $cond: ['$r', { $ifNull: [cap('$r.captures', 2), ''] }, ''] },
      cap('$c', 11),
      { $cond: [{ $eq: [cap('$c', 12), '-'] }, '0', cap('$c', 12)] }
    ], ['MALFORMED', '', '', '', '', '', '', '', '']] } } }
]).forEach(d => print(JSON.stringify(d.v)));

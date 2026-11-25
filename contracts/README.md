# contracts/

The single source of truth shared by all four pipelines. **Read these files at runtime; never retype them.**
Full reasoning: the team spec doc, "NASA Log ETL – Shared Contracts Spec".

| File | What it fixes |
| --- | --- |
| `envelope.regex` | A line is valid iff it matches (and the month is in `months.tsv`). 13 groups: host, ident, user, day, month, year, hour, minute, second, tz, request, status, bytes |
| `request.regex` | Splits the request into method (1), resource_path (2), protocol_version (3, optional) |
| `request_fallback.regex` | When `request.regex` does not match: resource_path = group 1, method = protocol = `''` |
| `months.tsv` | Jan..Dec -> 01..12. Any other month name = malformed |
| `outputs.json` | Columns, types and file format of q1/q2/q3/malformed/stats outputs |
| `batch_spec.schema.json` | Controller -> engine message |
| `batch_result.schema.json` | Engine -> controller message |

## Rules that are easy to get wrong

- **Lines:** split each input file on `\n` separately, strip a trailing `\r`, keep a last line with no newline. Never concatenate the files (July ends with the truncated line `alyssa.p`).
- **Bytes:** decode as UTF-8, invalid bytes -> U+FFFD (what Hadoop's `Text` already does).
- **Parsed fields (every engine keeps all nine):** `host`, `log_timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`.
- **Dates:** build `log_date` (`YYYY-MM-DD`), `log_hour` (0-23) and `log_timestamp` (`YYYY-MM-DD HH:MM:SS -0400`: the logged time and zone, unchanged) by slicing regex groups. Never use engine date functions (MongoDB `$dateFromString` converts to UTC and shifts hours by 4).
- **Q2 ties:** `request_count` descending, then `resource_path` in raw UTF-8 byte order (PostgreSQL `COLLATE "C"`, Hadoop `Text`, MongoDB simple collation). Java and Pig string comparison is UTF-16 order, which differs for characters above U+FFFF.
- **Missing parts:** `''`, never NULL. `bytes` `-` -> 0. All byte counts are 64-bit.
- **No trim functions:** the regexes skip spaces themselves.
- **Every valid line goes into every query.** No method or status filter, except Q3's 400-599 error rule.

## Embedding the regex in an engine

Each file holds the raw pattern on one line. Load it with your language's file reading and pass it as a variable; do not paste it by hand. Engines that put the pattern inside a string literal need the backslashes escaped for that language:

- **Hive:** inside a `'...'` HiveQL string every `\` becomes `\\`.
- **Pig:** inside a `'...'` Pig string every `\` becomes `\\`.
- **MongoDB:** pass the pattern string to `$regexFind` from Python (`pymongo`); no extra escaping needed.
- **MapReduce (Java):** `Pattern.compile(Files.readString(...).strip())`.

## Expected answers on the real data

3,461,613 records (Jul95 1,891,715 + Aug95 1,569,898), 1 malformed. Final Q1 = 295 rows, Q2 = 20, Q3 = 1,369.
Exact rows: `../reference/reference_results.json`.

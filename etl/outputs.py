"""Reading and writing the result files of Contract 2 (PostgreSQL COPY text format)."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence

_ESCAPE = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"})
_UNESCAPE = {"t": "\t", "n": "\n", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "\\": "\\"}


def write_tsv(path: Path, rows: Iterable[Sequence]) -> int:
    """Write rows in COPY text format. Returns the number of rows written."""
    n = 0
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write("\t".join(str(v).translate(_ESCAPE) for v in row) + "\n")
            n += 1
    return n


def data_files(path: Path) -> List[Path]:
    """q1.tsv may be a file or a folder of part files (Hive). Skips .crc and other dot files."""
    path = Path(path)
    if path.is_dir():
        return sorted(p for p in path.iterdir() if p.is_file() and not p.name.startswith((".", "_")))
    return [path] if path.is_file() else []


def read_bytes(path: Path) -> bytes:
    """All data of one output, ready to stream into COPY FROM STDIN."""
    return b"".join(p.read_bytes() for p in data_files(path))


def parse_line(line: str) -> List[str]:
    """Split one COPY text line into fields (backslash + any other char = that char)."""
    fields, cur, i = [], [], 0
    while i < len(line):
        ch = line[i]
        if ch == "\\" and i + 1 < len(line):
            nxt = line[i + 1]
            cur.append(_UNESCAPE.get(nxt, nxt))
            i += 2
            continue
        if ch == "\t":
            fields.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    fields.append("".join(cur))
    return fields


def read_rows(path: Path) -> Iterator[List[str]]:
    for line in read_bytes(path).decode("utf-8").split("\n"):
        if line:
            yield parse_line(line)


def fingerprint(query_id: str, rows: List[List[str]]) -> str:
    """SHA-256 of the canonical, sorted rows. error_rate (q3 column 5) is left out on purpose:
    engines print doubles differently, and the database CHECK already ties it to the counts."""
    canon = []
    for r in rows:
        r = list(r)
        if query_id == "q3":
            r = r[:4] + r[5:]
        canon.append("\t".join(_normalise(v) for v in r))
    return hashlib.sha256("\n".join(sorted(canon)).encode("utf-8")).hexdigest()


def _normalise(v: str) -> str:
    """'007' and '7' are the same integer; everything else compares as text."""
    return str(int(v)) if v.lstrip("-").isdigit() else v

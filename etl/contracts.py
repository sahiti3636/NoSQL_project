"""Loads the shared rules from contracts/ so no engine ever retypes them."""
import csv
import json
from functools import lru_cache

from .config import CONTRACTS_DIR


def _read(name: str) -> str:
    return (CONTRACTS_DIR / name).read_text(encoding="utf-8").rstrip("\n")


@lru_cache(maxsize=None)
def envelope_regex() -> str:
    return _read("envelope.regex")


@lru_cache(maxsize=None)
def request_regex() -> str:
    return _read("request.regex")


@lru_cache(maxsize=None)
def request_fallback_regex() -> str:
    return _read("request_fallback.regex")


@lru_cache(maxsize=None)
def months() -> dict:
    """{'Jan': '01', ..., 'Dec': '12'}"""
    with open(CONTRACTS_DIR / "months.tsv", encoding="utf-8") as f:
        return {r["month_name"]: r["month_number"] for r in csv.DictReader(f, delimiter="\t")}


@lru_cache(maxsize=None)
def outputs() -> dict:
    return json.loads((CONTRACTS_DIR / "outputs.json").read_text(encoding="utf-8"))


def output_columns(file_name: str) -> list:
    """Column names of q1.tsv / q2.tsv / q3.tsv / malformed.tsv, in order."""
    return [c["name"] for c in outputs()["files"][file_name]["columns"]]


QUERY_FILES = {"q1": "q1.tsv", "q2": "q2.tsv", "q3": "q3.tsv"}

"""Build a lesson notebook from lessons/src/LNN.py (Databricks source format).

    python tools/build_notebook.py L02 --mode solved    -> build/L02_solved.py
    python tools/build_notebook.py L02 --mode learner   -> notebooks/L02 - <title>.py

Source conventions (lessons/src/LNN.py):
  line 1   # Databricks notebook source
  line 2   # LESSON_TITLE: <title>              (removed from the output)
  cells    separated by "# COMMAND ----------"; markdown cells use "# MAGIC %md"
  blocks   # >>> BLANK P1 / # >>> SOLUTION P1 / # <<< END P1   (ids: W1.. warm-up, P1.. practice)
  {{key}}            in markdown: replaced by METRICS[key] from runs/LNN_latest.json (learner mode)
  {{solution:P1}}    alone on a markdown line: replaced by the SOLUTION P1 code (both modes)

Companion notebooks (lessons/src/LNN.<name>.py, same two header lines) are notebooks that the lesson
calls with dbutils.notebook.run("./<title>"). They are copied as they are (no blocks, no placeholders)
to build/LNN.<name>.py and notebooks/<title>.py, and imported next to the lesson notebook.
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEADER = "# Databricks notebook source"
CELL_SEP = "\n\n# COMMAND ----------\n\n"
SEP_LINE = "# COMMAND ----------"
TITLE_RE = re.compile(r"^# LESSON_TITLE:\s*(.+?)\s*$")
BLOCK_RE = re.compile(r"^\s*# (>>> BLANK|>>> SOLUTION|<<< END) ([A-Z]\d+)\s*$")
SOLUTION_LINE_RE = re.compile(r"^# MAGIC\s*\{\{solution:([A-Z]\d+)\}\}\s*$")
PLACEHOLDER_RE = re.compile(r"\{\{([^{}]+)\}\}")
STANDARD_IDS = ["W1", "W2", "W3", "P1", "P2", "P3", "P4"]

STATE_CELL = """# DBTITLE 1,Lesson state (do not edit)
# METRICS holds the key numbers that the Explained cells quote.
# The validation cells record here which warm-up and practice tasks passed.
METRICS = {}
WARMUP_PASSED = {}
PRACTICE_PASSED = {}"""

CHECK_CELL = """# DBTITLE 1,Build check — every warm-up and practice validation passed
expected = {{"WARMUP_PASSED": {warmup}, "PRACTICE_PASSED": {practice}}}
results = {{"WARMUP_PASSED": WARMUP_PASSED, "PRACTICE_PASSED": PRACTICE_PASSED}}
not_passed = [f"{{name}}[{{k}}]" for name, ids in expected.items() for k in ids if results[name].get(k) is not True]
assert not not_passed, f"These validations did not pass: {{not_passed}}"
print("All validations passed:", expected)"""

EXIT_CELL = """# DBTITLE 1,Build check — return METRICS to the runner
import json
dbutils.notebook.exit(json.dumps(METRICS, default=str))"""


class BuildError(Exception):
    pass


def read_source(lesson):
    path = ROOT / "lessons" / "src" / f"{lesson}.py"
    if not path.exists():
        raise BuildError(f"{path} does not exist")
    return read_source_file(path)


def read_source_file(path):
    lines = path.read_text(encoding="utf-8").replace("\r\n", "\n").split("\n")
    if lines[0] != HEADER:
        raise BuildError(f"{path}: line 1 must be '{HEADER}'")
    m = TITLE_RE.match(lines[1]) if len(lines) > 1 else None
    if not m:
        raise BuildError(f"{path}: line 2 must be '# LESSON_TITLE: <title>'")
    title = m.group(1)
    if re.search(r'[<>:"/\\|?*]', title):
        raise BuildError(f"title {title!r} has characters that are not allowed in file names")
    return title, "\n".join(lines[2:])


def apply_blocks(body, mode):
    """Keep BLANK (learner) or SOLUTION (solved) code; return (text, {id: solution lines})."""
    keep = ">>> BLANK" if mode == "learner" else ">>> SOLUTION"
    out, solutions, state, cur = [], {}, None, None
    for n, line in enumerate(body.split("\n"), start=3):
        m = BLOCK_RE.match(line)
        if m:
            kind, bid = m.groups()
            if kind == ">>> BLANK" and state is None:
                if bid in solutions:
                    raise BuildError(f"line {n}: block {bid} appears twice")
                solutions[bid] = []
            elif kind == ">>> SOLUTION" and state == ">>> BLANK" and bid == cur:
                pass
            elif kind == "<<< END" and state == ">>> SOLUTION" and bid == cur:
                kind = None
            else:
                raise BuildError(f"line {n}: unexpected '{line.strip()}' (blocks must be BLANK -> SOLUTION -> END)")
            state, cur = kind, bid
            continue
        if state is not None and line.strip() == SEP_LINE:
            raise BuildError(f"line {n}: block {cur} crosses a cell boundary")
        if state == ">>> SOLUTION":
            solutions[cur].append(line)
        if state is None or state == keep:
            out.append(line)
    if state is not None:
        raise BuildError(f"block {cur} has no '# <<< END {cur}'")
    return "\n".join(out), solutions


def split_cells(text):
    cells = [c.strip("\n") for c in re.split(r"^# COMMAND ----------$", text, flags=re.M)]
    return [c for c in cells if c.strip()]


def is_markdown(cell):
    lines = [l for l in cell.split("\n") if not l.startswith("# DBTITLE")]
    return bool(lines) and lines[0].startswith("# MAGIC %md")


def code_hash(cells):
    """Hash of the code cells only, so markdown edits do not make a run stale."""
    code = "\n".join(c for c in cells if not is_markdown(c))
    return hashlib.sha256(code.encode("utf-8")).hexdigest()[:16]


def expand_solutions(cell, solutions):
    out = []
    for line in cell.split("\n"):
        m = SOLUTION_LINE_RE.match(line)
        if not m:
            out.append(line)
            continue
        bid = m.group(1)
        if bid not in solutions:
            raise BuildError(f"{{{{solution:{bid}}}}} has no matching '# >>> SOLUTION {bid}' block")
        out.append("# MAGIC ```python")
        out += [f"# MAGIC {l}".rstrip() for l in solutions[bid]]
        out.append("# MAGIC ```")
    return "\n".join(out)


def format_value(v):
    return json.dumps(v) if isinstance(v, (dict, list)) else str(v)


def fill_placeholders(cells, metrics, source):
    missing, out = set(), []
    for cell in cells:
        if is_markdown(cell):
            def sub(m):
                key = m.group(1).strip()
                if key not in metrics:
                    missing.add(key)
                    return m.group(0)
                return format_value(metrics[key])
            cell = PLACEHOLDER_RE.sub(sub, cell)
        out.append(cell)
    if missing:
        raise BuildError(f"placeholders with no value in {source}: {sorted(missing)}")
    return out


def build(lesson, mode, allow_stale=False):
    title, body = read_source(lesson)
    body, solutions = apply_blocks(body, mode)
    cells = [expand_solutions(c, solutions) for c in split_cells(body)]

    lacking = [i for i in STANDARD_IDS if i not in solutions]
    if lacking:
        print(f"WARNING: {lesson} has no practice/warm-up block for {lacking}")
    bad = [i for i in solutions if i[0] not in "WP"]
    if bad:
        raise BuildError(f"block ids must start with W (warm-up) or P (practice): {bad}")

    # The state cell goes before the first code cell, so every validation cell can use it
    first_code = next((i for i, c in enumerate(cells) if not is_markdown(c)), len(cells))
    cells.insert(first_code, STATE_CELL)

    if mode == "solved":
        warmup = sorted(i for i in solutions if i.startswith("W"))
        practice = sorted(i for i in solutions if i.startswith("P"))
        cells += [CHECK_CELL.format(warmup=warmup, practice=practice), EXIT_CELL]
        out_path = ROOT / "build" / f"{lesson}_solved.py"
    else:
        latest = ROOT / "runs" / f"{lesson}_latest.json"
        if not latest.exists():
            raise BuildError(f"{latest} does not exist: run tools/run_lesson.py {lesson} first")
        record = json.loads(latest.read_text(encoding="utf-8"))
        solved_hash = lesson_code_hash(lesson)
        if record.get("code_hash") != solved_hash and not allow_stale:
            raise BuildError(
                f"{latest} was produced by different code (run {record.get('code_hash')}, source now {solved_hash}). "
                f"Run tools/run_lesson.py {lesson} again, or pass --allow-stale.")
        cells = fill_placeholders(cells, record["metrics"], latest.relative_to(ROOT))
        out_path = ROOT / "notebooks" / f"{lesson} - {title}.py"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(HEADER + "\n" + CELL_SEP.join(cells) + "\n", encoding="utf-8", newline="\n")
    return out_path, title


def build_cells_only(lesson):
    """The cells of the solved notebook, used for code_hash."""
    _, body = read_source(lesson)
    body, solutions = apply_blocks(body, "solved")
    return [expand_solutions(c, solutions) for c in split_cells(body)]


def companion_sources(lesson):
    """[(name, title, cells)] for every lessons/src/LNN.<name>.py."""
    out = []
    for path in sorted((ROOT / "lessons" / "src").glob(f"{lesson}.*.py")):
        title, body = read_source_file(path)
        out.append((path.stem.split(".", 1)[1], title, split_cells(body)))
    return out


def build_companions(lesson, mode):
    """Write the companion notebooks; return [(local path, workspace title)]."""
    built = []
    for name, title, cells in companion_sources(lesson):
        out_path = ROOT / "build" / f"{lesson}.{name}.py" if mode == "solved" else ROOT / "notebooks" / f"{title}.py"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(HEADER + "\n" + CELL_SEP.join(cells) + "\n", encoding="utf-8", newline="\n")
        built.append((out_path, title))
    return built


def lesson_code_hash(lesson):
    """code_hash of the solved lesson plus all companion notebooks."""
    cells = build_cells_only(lesson)
    for _, _, comp_cells in companion_sources(lesson):
        cells += comp_cells
    return code_hash(cells)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("lesson", help="lesson id, e.g. L02")
    p.add_argument("--mode", choices=["solved", "learner"], required=True)
    p.add_argument("--allow-stale", action="store_true",
                   help="learner mode: use runs/LNN_latest.json even if the code changed since that run")
    args = p.parse_args()
    try:
        path, _ = build(args.lesson, args.mode, args.allow_stale)
        companions = build_companions(args.lesson, args.mode)
    except BuildError as e:
        sys.exit(f"BUILD FAILED: {e}")
    for p in [path] + [c for c, _ in companions]:
        print(f"built {p.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

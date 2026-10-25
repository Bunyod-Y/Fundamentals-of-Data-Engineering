"""Build a lesson notebook from _authoring/LNN.py (Databricks source format).

    python _authoring/build_notebook.py L03 --mode solved    -> _authoring/build/L03_solved.py (+ test harness)
    python _authoring/build_notebook.py L03 --mode learner   -> Databricks - Learning/L03 - <title>.ipynb

Source conventions (_authoring/LNN.py):
  line 1   # Databricks notebook source
  line 2   # LESSON_TITLE: <title>              (removed from the output)
  cells    separated by "# COMMAND ----------"; first line "# DBTITLE 1,<title>"; markdown cells use "# MAGIC %md"
  blocks   # >>> BLANK P1 / # >>> SOLUTION P1 / # <<< END P1   (ids: W1.. warm-up, P1.. practice)
  {{key}}            in markdown: replaced by METRICS[key] from _authoring/LNN.json (learner mode)
  {{solution:P1}}    alone on a markdown line: replaced by the SOLUTION P1 code (both modes)
  # EXPECT_FAILURE: <key>
           first code line of a cell that must fail (for example a stream that stops). Learner: a note to
           read the error and go on. Solved: the cell runs in the harness notebook, and
           METRICS["<key>.errors"] gets the real error classes (the build fails if the cell does not fail).

One lesson is one learner notebook: an .ipynb like the hand-written lessons, without test plumbing. Lines
that start with METRICS[, WARMUP_PASSED[ or PRACTICE_PASSED[ are removed from it, so they may only record
values: one line, and no work (streams, writes, file landing) on the right side.
"""
import argparse
import hashlib
import html
import json
import re
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LESSONS = ROOT / "_authoring"                    # sources, specs and verified run results (LNN.json)
NOTEBOOKS = ROOT / "Databricks - Learning"       # learner notebooks (what you study)
BUILD = LESSONS / "build"                        # solved builds and run history (git-ignored)
HEADER = "# Databricks notebook source"
CELL_SEP = "\n\n# COMMAND ----------\n\n"
SEP_LINE = "# COMMAND ----------"
TITLE_RE = re.compile(r"^# LESSON_TITLE:\s*(.+?)\s*$")
BLOCK_RE = re.compile(r"^\s*# (>>> BLANK|>>> SOLUTION|<<< END) ([A-Z]\d+)\s*$")
SOLUTION_LINE_RE = re.compile(r"^# MAGIC\s*\{\{solution:([A-Z]\d+)\}\}\s*$")
PLACEHOLDER_RE = re.compile(r"\{\{([^{}]+)\}\}")
EXPECT_RE = re.compile(r"^# EXPECT_FAILURE: (\S+)\s*$")
RECORD_RE = re.compile(r"^\s*(?:(?:METRICS|WARMUP_PASSED|PRACTICE_PASSED)\[[^\]]*\]\s*,\s*)*"
                       r"(?:METRICS|WARMUP_PASSED|PRACTICE_PASSED)\[[^\]]*\]\s*=(?!=)(.*)$")
TEST_LINE_RE = re.compile(r"^\s*(METRICS|WARMUP_PASSED|PRACTICE_PASSED)\[")
UNSAFE_RHS_RE = re.compile(r"\b(?:load|land|run)_\w*\(|spark\.sql\(|\.write|toTable|awaitTermination|dbutils\.")
STANDARD_IDS = ["W1", "W2", "W3", "P1", "P2", "P3", "P4"]
HARNESS_NAME = "_expect_failure_harness"

STATE_CELL = """# DBTITLE 1,Test state (solved build only)
import json
METRICS = {{}}
WARMUP_PASSED = {{}}
PRACTICE_PASSED = {{}}

def run_expecting_failure(key, code):
    \"\"\"Run code that must fail in the harness notebook; return its error classes ("none" if it did not fail).\"\"\"
    error_file = VOL + "/_expect_failure/" + key + ".json"   # VOL is defined by the lesson setup
    try:
        dbutils.notebook.run("./{harness}", 900, {{"code": code, "error_file": error_file}})
    except Exception:
        pass                                   # the harness run fails when the code fails
    return ", ".join(json.loads(dbutils.fs.head(error_file))["error_classes"]) or "none\""""

CHECK_CELL = """# DBTITLE 1,Build check — every warm-up and practice validation passed
expected = {{"WARMUP_PASSED": {warmup}, "PRACTICE_PASSED": {practice}}}
results = {{"WARMUP_PASSED": WARMUP_PASSED, "PRACTICE_PASSED": PRACTICE_PASSED}}
not_passed = [f"{{name}}[{{k}}]" for name, ids in expected.items() for k in ids if results[name].get(k) is not True]
assert not not_passed, f"These validations did not pass: {{not_passed}}"
print("All validations passed:", expected)"""

EXIT_CELL = """# DBTITLE 1,Build check — return METRICS to the runner
import json
dbutils.notebook.exit(json.dumps(METRICS, default=str))"""

# A failed stream fails the notebook cell even inside try/except, so expected failures run in this child
HARNESS = r'''# Databricks notebook source
# DBTITLE 1,Run code that is expected to fail (test builds only)
import json, re
dbutils.widgets.text("code", "")
dbutils.widgets.text("error_file", "")
error_file = dbutils.widgets.get("error_file")
try:
    exec(dbutils.widgets.get("code"), {"spark": spark, "dbutils": dbutils, "display": display})
except Exception as e:
    classes = list(dict.fromkeys(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))))
    dbutils.fs.put(error_file, json.dumps({"error_classes": classes or [type(e).__name__]}), overwrite=True)
    raise
dbutils.fs.put(error_file, json.dumps({"error_classes": []}), overwrite=True)
'''


class BuildError(Exception):
    pass


def read_source(lesson):
    path = LESSONS / f"{lesson}.py"
    if not path.exists():
        raise BuildError(f"{path} does not exist")
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


def split_title(cell):
    """(title, other lines) of a source cell."""
    lines = cell.split("\n")
    if lines[0].startswith("# DBTITLE 1,"):
        return lines[0][len("# DBTITLE 1,"):], lines[1:]
    return "", lines


def is_markdown(cell):
    _, lines = split_title(cell)
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
        if any("{{" in l for l in solutions[bid]):
            # Code with {{...}} (for example job dynamic value references) would be read as placeholders in a
            # markdown cell, so it is shown as an HTML <pre> block with the braces written as entities
            esc = lambda s: html.escape(s, quote=False).replace("{", "&#123;").replace("}", "&#125;")
            out.append("# MAGIC <pre><code>" + esc(solutions[bid][0] if solutions[bid] else ""))
            out += [f"# MAGIC {esc(l)}".rstrip() for l in solutions[bid][1:]]
            out.append("# MAGIC </code></pre>")
            continue
        out.append("# MAGIC ```python")
        out += [f"# MAGIC {l}".rstrip() for l in solutions[bid]]
        out.append("# MAGIC ```")
    return "\n".join(out)


def apply_expect_failure(cell, mode):
    title, lines = split_title(cell)
    marker = next((m for m in map(EXPECT_RE.match, lines) if m), None)
    if not marker:
        return cell
    key = marker.group(1)
    body = [l for l in lines if not EXPECT_RE.match(l)]
    head = [f"# DBTITLE 1,{title}"] if title else []
    if mode == "learner":
        return "\n".join(head + ["# Expected to fail: read the error class in the message, then run the next cell."] + body)
    code = "\n".join(body)
    return "\n".join(head + [
        f'METRICS["{key}.errors"] = run_expecting_failure("{key}", {code!r})',
        f'print("{key} failed with:", METRICS["{key}.errors"])',
        f'assert METRICS["{key}.errors"] != "none", "{key} was expected to fail, but it did not"'])


def strip_test_lines(cell, index):
    """Learner mode: remove the lines that only record METRICS / *_PASSED values."""
    out = []
    for line in cell.split("\n"):
        if not TEST_LINE_RE.match(line):
            out.append(line)
            continue
        m = RECORD_RE.match(line)
        if not m:
            raise BuildError(f"cell {index}: '{line.strip()}' uses a test value; only one-line records are allowed")
        rhs = m.group(1)
        if any(rhs.count(a) != rhs.count(b) for a, b in ("()", "[]", "{}")):
            raise BuildError(f"cell {index}: '{line.strip()}' continues on the next line; keep records on one line")
        bad = UNSAFE_RHS_RE.findall(rhs)
        if bad:
            raise BuildError(f"cell {index}: '{line.strip()}' does work on the right side ({bad}); "
                             "store the result in a variable first, then record the variable")
    return "\n".join(out)


def check_compiles(cell, index):
    """Learner code cells must still be valid Python after the test lines are removed."""
    _, lines = split_title(cell)
    code = "\n".join(lines)
    if "<FILL_IN>" in code or code.lstrip().startswith("%"):
        return
    try:
        compile(code, f"cell {index}", "exec")
    except SyntaxError as e:
        raise BuildError(f"cell {index} is not valid Python in the learner notebook: {e}") from None


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


def to_ipynb(cells, lesson, title):
    """Jupyter notebook in the same shape as the Databricks export of the hand-written lessons."""
    nb_cells = []
    for i, cell in enumerate(cells):
        cell_title, lines = split_title(cell)
        markdown = bool(lines) and lines[0].startswith("# MAGIC %md")
        if markdown:
            lines = [l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lines[1:]]
        source = [l + "\n" for l in lines]
        if source:
            source[-1] = source[-1][:-1]
        meta = {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {}, "inputWidgets": {},
            "nuid": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{lesson}/{i}")),    # stable across builds
            "showTitle": bool(cell_title), "tableResultSettingsMap": {}, "title": cell_title}}
        if markdown:
            nb_cells.append({"cell_type": "markdown", "metadata": meta, "source": source})
        else:
            nb_cells.append({"cell_type": "code", "execution_count": None, "metadata": meta,
                             "outputs": [], "source": source})
    nb = {"cells": nb_cells,
          "metadata": {"application/vnd.databricks.v1+notebook": {
              "language": "python", "notebookMetadata": {"pythonIndentUnit": 4},
              "notebookName": f"{lesson} - {title}", "widgets": {}},
              "language_info": {"name": "python"}},
          "nbformat": 4, "nbformat_minor": 0}
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


def build(lesson, mode, allow_stale=False):
    title, body = read_source(lesson)
    body, solutions = apply_blocks(body, mode)
    cells = [apply_expect_failure(expand_solutions(c, solutions), mode) for c in split_cells(body)]

    lacking = [i for i in STANDARD_IDS if i not in solutions]
    if lacking:
        print(f"WARNING: {lesson} has no practice/warm-up block for {lacking}")
    bad = [i for i in solutions if i[0] not in "WP"]
    if bad:
        raise BuildError(f"block ids must start with W (warm-up) or P (practice): {bad}")

    BUILD.mkdir(parents=True, exist_ok=True)
    if mode == "solved":
        # The state cell goes before the first code cell, so every validation cell can use it
        first_code = next((i for i, c in enumerate(cells) if not is_markdown(c)), len(cells))
        cells.insert(first_code, STATE_CELL.format(lesson=lesson.lower(), harness=HARNESS_NAME))
        warmup = sorted(i for i in solutions if i.startswith("W"))
        practice = sorted(i for i in solutions if i.startswith("P"))
        cells += [CHECK_CELL.format(warmup=warmup, practice=practice), EXIT_CELL]
        (BUILD / f"{HARNESS_NAME}.py").write_text(HARNESS, encoding="utf-8", newline="\n")
        out_path = BUILD / f"{lesson}_solved.py"
        out_path.write_text(HEADER + "\n" + CELL_SEP.join(cells) + "\n", encoding="utf-8", newline="\n")
        return out_path, title

    latest = LESSONS / f"{lesson}.json"
    if not latest.exists():
        raise BuildError(f"{latest} does not exist: run _authoring/run_lesson.py {lesson} first")
    record = json.loads(latest.read_text(encoding="utf-8"))
    solved_hash = code_hash(build_cells_only(lesson))
    if record.get("code_hash") != solved_hash and not allow_stale:
        raise BuildError(
            f"{latest} was produced by different code (run {record.get('code_hash')}, source now {solved_hash}). "
            f"Run _authoring/run_lesson.py {lesson} again, or pass --allow-stale.")
    cells = [strip_test_lines(c, i) for i, c in enumerate(cells)]
    for i, c in enumerate(cells):
        if not is_markdown(c):
            check_compiles(c, i)
    cells = fill_placeholders(cells, record["metrics"], latest.relative_to(ROOT))
    out_path = NOTEBOOKS / f"{lesson} - {title}.ipynb"
    out_path.write_text(to_ipynb(cells, lesson, title), encoding="utf-8", newline="\n")
    return out_path, title


def build_cells_only(lesson):
    """The cells of the solved notebook before test wrapping, used for code_hash."""
    _, body = read_source(lesson)
    body, solutions = apply_blocks(body, "solved")
    return [expand_solutions(c, solutions) for c in split_cells(body)]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("lesson", help="lesson id, e.g. L03")
    p.add_argument("--mode", choices=["solved", "learner"], required=True)
    p.add_argument("--allow-stale", action="store_true",
                   help="learner mode: use _authoring/LNN.json even if the code changed since that run")
    args = p.parse_args()
    try:
        path, _ = build(args.lesson, args.mode, args.allow_stale)
    except BuildError as e:
        sys.exit(f"BUILD FAILED: {e}")
    print(f"built {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

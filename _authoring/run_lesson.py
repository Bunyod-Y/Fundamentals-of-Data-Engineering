"""Run a lesson's solved notebook on serverless and save the results.

    python _authoring/run_lesson.py L02             build solved, import, run once
    python _authoring/run_lesson.py L02 --twice     run twice and diff METRICS (keys starting with "nd." are ignored)
    python _authoring/run_lesson.py L02 --publish   build the learner notebook (.ipynb) into "Databricks - Learning/" (no run)

Results: _authoring/build/runs/LNN_<UTC timestamp>.json for every run; _authoring/LNN.json only after a successful run.
Test builds are imported to <home>/_lesson_build, outside the Git folder (<home> from `databricks current-user me`).
Learner notebooks reach the workspace through git: commit, push, then Pull in the workspace Git folder.
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_notebook  # noqa: E402

ROOT = build_notebook.ROOT
BUILD_DIR = "_lesson_build"          # workspace folder for test builds, relative to the user's home
POLL_SECONDS = 15
RUN_TIMEOUT_SECONDS = 3600
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}  # a failed notebook ends in INTERNAL_ERROR
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


class Cli:
    def __init__(self, profile):
        self.exe = shutil.which("databricks")
        if not self.exe:
            sys.exit("databricks CLI not found on PATH (restart the terminal after installing it)")
        self.profile = profile

    def __call__(self, *args, parse=True):
        cmd = [self.exe, *args, "--profile", self.profile, "-o", "json"]
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
        if r.returncode != 0:
            raise RuntimeError(f"`databricks {' '.join(args)}` failed (exit {r.returncode}):\n{r.stderr.strip()}")
        return json.loads(r.stdout) if parse and r.stdout.strip() else r.stdout


def build_dir(cli):
    return f"/Users/{cli('current-user', 'me')['userName']}/{BUILD_DIR}"


def import_notebook(cli, local_path, workspace_path):
    cli("workspace", "mkdirs", workspace_path.rsplit("/", 1)[0], parse=False)
    cli("workspace", "import", workspace_path, "--file", str(local_path),
        "--format", "SOURCE", "--language", "PYTHON", "--overwrite", parse=False)
    print(f"imported {local_path.relative_to(ROOT)} -> {workspace_path}")


def run_once(cli, lesson, notebook_path, code_hash):
    started = datetime.now(timezone.utc)
    submit = {
        "run_name": f"{lesson} solved build",
        "timeout_seconds": RUN_TIMEOUT_SECONDS,
        # No cluster spec: the task runs on serverless compute
        "tasks": [{"task_key": "lesson", "notebook_task": {"notebook_path": notebook_path, "source": "WORKSPACE"}}],
    }
    run_id = cli("jobs", "submit", "--no-wait", "--json", json.dumps(submit))["run_id"]
    print(f"submitted run {run_id}")

    last = None
    while True:
        run = cli("jobs", "get-run", str(run_id))
        state = run["state"]["life_cycle_state"]
        if state != last:
            print(f"  {datetime.now():%H:%M:%S} {state}")
            last = state
        if state in TERMINAL_STATES:
            break
        time.sleep(POLL_SECONDS)

    task = run["tasks"][0]
    output = cli("jobs", "get-run-output", str(task["run_id"]))
    record = {
        "lesson": lesson,
        "started_utc": started.isoformat(timespec="seconds"),
        "run_id": run_id,
        "task_run_id": task["run_id"],
        "life_cycle_state": state,
        "result_state": run["state"].get("result_state"),
        "state_message": run["state"].get("state_message"),
        "run_duration_s": round(run.get("run_duration", 0) / 1000),
        "code_hash": code_hash,
        # Compute fields present on the task. Serverless: no cluster fields, but an
        # effective_performance_target (a serverless-only setting per `jobs submit --help`)
        "task_compute": {k: task[k] for k in ("existing_cluster_id", "new_cluster", "job_cluster_key",
                                               "cluster_instance", "environment_key", "compute",
                                               "effective_performance_target") if k in task},
        "metrics": None,
    }
    nb = output.get("notebook_output", {})
    if nb.get("truncated"):
        record["error"] = "notebook_output was truncated (over 5 MB): METRICS is incomplete"
    elif "result" in nb:
        record["metrics"] = json.loads(nb["result"])
    for k in ("error", "error_trace"):
        if output.get(k):
            record[k] = ANSI_RE.sub("", output[k])  # the notebook trace comes with terminal colour codes

    ok = record["result_state"] == "SUCCESS" and isinstance(record["metrics"], dict)
    runs = build_notebook.BUILD / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    out = runs / f"{lesson}_{started:%Y%m%dT%H%M%SZ}.json"
    out.write_text(json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8", newline="\n")
    if ok:
        shutil.copyfile(out, build_notebook.LESSONS / f"{lesson}.json")
    print(f"{'SUCCESS' if ok else 'FAILED'}: {record['result_state']} in {record['run_duration_s']} s -> "
          f"{out.relative_to(ROOT)}{' (and _authoring/' + lesson + '.json)' if ok else ''}")
    if not ok:
        print(record.get("state_message") or "")
        print(record.get("error") or "")
        print(record.get("error_trace") or "")
    return ok, record


def diff_metrics(a, b):
    keys = sorted(k for k in set(a) | set(b) if not k.startswith("nd."))
    return [(k, a.get(k, "<missing>"), b.get(k, "<missing>")) for k in keys if a.get(k, "<missing>") != b.get(k, "<missing>")]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("lesson", help="lesson id, e.g. L02")
    p.add_argument("--twice", action="store_true", help="run two times and diff METRICS")
    p.add_argument("--publish", action="store_true",
                   help='build the learner notebook (.ipynb) into "Databricks - Learning/" (no run)')
    p.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile (default: DEFAULT)")
    args = p.parse_args()

    if args.publish:
        try:
            path, _ = build_notebook.build(args.lesson, "learner")
        except build_notebook.BuildError as e:
            sys.exit(f"FAILED: {e}")
        print(f"built {path.relative_to(ROOT)}")
        print("Next: commit and push, then Pull in the workspace Git folder.")
        return

    cli = Cli(args.profile)
    try:
        target = build_dir(cli)
        path, _ = build_notebook.build(args.lesson, "solved")
        print(f"built {path.relative_to(ROOT)}")
        code_hash = build_notebook.code_hash(build_notebook.build_cells_only(args.lesson))
        notebook = f"{target}/{args.lesson}_solved"
        import_notebook(cli, path, notebook)
        # The harness sits next to the solved notebook, so run_expecting_failure finds it with "./"
        harness = build_notebook.BUILD / f"{build_notebook.HARNESS_NAME}.py"
        import_notebook(cli, harness, f"{target}/{build_notebook.HARNESS_NAME}")

        results = []
        for i in range(2 if args.twice else 1):
            if args.twice:
                print(f"--- run {i + 1} of 2")
            ok, record = run_once(cli, args.lesson, notebook, code_hash)
            if not ok:
                sys.exit(1)
            results.append(record["metrics"])
    except (build_notebook.BuildError, RuntimeError) as e:
        sys.exit(f"FAILED: {e}")

    print("METRICS:", json.dumps(results[-1], indent=2, default=str))
    if args.twice:
        diffs = diff_metrics(*results)
        ignored = sorted(k for k in set(results[0]) | set(results[1]) if k.startswith("nd."))
        print(f"ignored nd. keys: {ignored}")
        if diffs:
            print("METRICS DIFFER between the two runs:")
            for k, a, b in diffs:
                print(f"  {k}: run 1 = {a!r}, run 2 = {b!r}")
            sys.exit(1)
        print("METRICS identical in both runs (nd. keys ignored)")


if __name__ == "__main__":
    main()

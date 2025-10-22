# gitver.py
import json, os, subprocess, datetime, pathlib

def _git(cmd):
    try:
        return subprocess.check_output(["git"] + cmd, stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        return ""

def _env(k): return os.getenv(k, "")

def calc():
    # приоритет: ENV -> git
    d = _env("GIT_DESCRIBE") or _git(["describe", "--tags", "--dirty", "--always"])
    b = _env("GIT_BRANCH")   or _git(["rev-parse", "--abbrev-ref", "HEAD"])
    s = _env("GIT_SHA")      or _git(["rev-parse", "--short", "HEAD"])
    t = _env("BUILD_AT")     or datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"describe": d, "branch": b, "sha": s, "build_at": t}

def write_version_json(path="version.json"):
    p = pathlib.Path(path)
    p.write_text(json.dumps(calc(), ensure_ascii=False, indent=2), encoding="utf-8")

def banner():
    v = calc()
    tail = []
    if v.get("branch"): tail.append(v["branch"])
    if v.get("sha"):    tail.append(f"@{v['sha']}")
    tail_str = " [" + " ".join(tail) + "]" if tail else ""
    built = f" built {v['build_at']}" if v.get("build_at") else ""
    return f"{v.get('describe','')}{tail_str}{built}"

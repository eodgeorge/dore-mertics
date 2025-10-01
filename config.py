
#!/usr/bin/env python3
import argparse, csv, os, re
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
import requests

API_VERSION = "7.1"

# ---------- time helpers ----------
def utc(dt_str):
    if not dt_str:
        return None
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)

def fmtiso(dt):
    return dt.astimezone(timezone.utc).isoformat()

def day_key(dt_utc):
    return dt_utc.date().isoformat()  # YYYY-MM-DD

def human_dur(seconds: int) -> str:
    if seconds is None:
        return ""
    neg = seconds < 0
    s = abs(int(seconds))
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if d: parts.append(f"{d}d")
    if h or d: parts.append(f"{h}h")
    if m or h or d: parts.append(f"{m}m")
    parts.append(f"{s}s")
    out = " ".join(parts)
    return f"-{out}" if neg else out

# ---------- ADO client ----------
class ADO:
    def __init__(self, org, project, pat=None):
        self.org = org
        self.project = project
        self.base = f"https://dev.azure.com/{org}/{project}"
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        bearer = os.getenv("SYSTEM_ACCESSTOKEN") or os.getenv("AZDO_BEARER")
        if bearer:
            self.session.headers["Authorization"] = f"Bearer {bearer}"
        elif pat:
            self.session.auth = ("", pat)
        else:
            raise SystemExit("Set SYSTEM_ACCESSTOKEN (in pipeline) or provide --pat / AZDO_PAT")

    def _get(self, path, params=None, base=None):
        url = f"{self.base}{path}" if base is None else f"{base}{path}"
        r = self.session.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def list_definitions(self, name=None):
        params = {"api-version": API_VERSION}
        if name:
            params["name"] = name
        return self._get("/_apis/build/definitions", params).get("value", [])

    def get_definition(self, def_id: int):
        return self._get(f"/_apis/build/definitions/{def_id}", {"api-version": API_VERSION})

    def list_builds(self, def_ids, min_time, max_time, branch=None, top=200):
        params = {
            "api-version": API_VERSION,
            "$top": top,
            "minTime": min_time.isoformat(),
            "maxTime": max_time.isoformat(),
            "queryOrder": "finishTimeDescending",
            "definitions": ",".join(str(i) for i in def_ids),
        }
        if branch and branch.strip():
            params["branchName"] = branch.strip()
        builds = []
        cont = None
        while True:
            if cont:
                params["continuationToken"] = cont
            j = self._get("/_apis/build/builds", params)
            builds.extend(j.get("value", []))
            cont = j.get("continuationToken")
            if not cont:
                break
        return builds

    def get_timeline(self, build_id):
        return self._get(f"/_apis/build/builds/{build_id}/timeline", {"api-version": API_VERSION})

    def get_ado_commit(self, repo_id, sha):
        return self._get(f"/_apis/git/repositories/{repo_id}/commits/{sha}", {"api-version": API_VERSION})

# ---------- pipeline helpers ----------
def resolve_definition_ids(ado_client, names_csv):
    pats = [s.strip().lower() for s in (names_csv or "").split(",") if s.strip()]
    if not pats:
        return []
    defs = ado_client.list_definitions()
    out = []
    for d in defs:
        n = (d.get("name") or "").lower()
        if any(p in n for p in pats):
            out.append(d["id"])
    return sorted(set(out))

# ---------- GitHub repo helpers ----------
def extract_owner_repo(repo: dict) -> str | None:
    if not repo:
        return None
    props = repo.get("properties") or {}
    for k in ("fullName", "full_name", "repoFullName", "repositoryFullName"):
        v = props.get(k)
        if v and "/" in v:
            return v
    api_url = props.get("apiUrl") or repo.get("url")
    if api_url:
        m = re.search(r"/repos/([^/]+/[^/]+)", api_url)
        if m:
            return m.group(1)
    clone = props.get("cloneUrl")
    if clone:
        path = urlparse(clone).path.strip("/")
        if path.endswith(".git"):
            path = path[:-4]
        if "/" in path:
            return path
    for k in ("name", "id"):
        v = repo.get(k)
        if v and "/" in v:
            return v
    return None

def github_commit_api(owner_repo: str, sha: str, props: dict) -> str:
    api_url = (props or {}).get("apiUrl")
    if api_url:
        base = api_url.rstrip("/")
        if re.search(r"/repos/[^/]+/[^/]+$", base):
            return f"{base}/commits/{sha}"
        m = re.search(r"^(https?://[^/]+)/repos/([^/]+/[^/]+)", base)
        if m:
            return f"{m.group(1)}/repos/{m.group(2)}/commits/{sha}"
    return f"https://api.github.com/repos/{owner_repo}/commits/{sha}"

def get_commit_time(build, ado_client, gh_token=None, gh_repo_override=None, verbose=False):
    sha = build.get("sourceVersion")
    if not sha:
        if verbose: print(f"[LEAD] build {build.get('id')} has no sourceVersion")
        return None
    repo = build.get("repository") or {}
    rtype = (repo.get("type") or "").lower()

    # Azure Repos
    if "tfs" in rtype or "azure" in rtype:
        rid = repo.get("id")
        if not rid:
            if verbose: print(f"[LEAD] build {build.get('id')} missing Azure Repos id")
            return None
        try:
            c = ado_client.get_ado_commit(rid, sha)
            dt = (c.get("author", {}) or {}).get("date") or (c.get("committer", {}) or {}).get("date")
            return utc(dt) if dt else None
        except requests.HTTPError as e:
            if verbose: print(f"[LEAD] ADO commit lookup failed for {sha}: {e}")
            return None

    # GitHub / GHES
    if "github" in rtype or gh_repo_override:
        owner_repo = gh_repo_override or extract_owner_repo(repo)
        if not owner_repo:
            if verbose:
                props = list((repo.get("properties") or {}).keys())
                print(f"[LEAD] cannot determine GitHub repo for build {build.get('id')} (props={props})")
            return None
        api_url = github_commit_api(owner_repo, sha, repo.get("properties") or {})
        headers = {"Accept": "application/vnd.github+json"}
        if gh_token:
            headers["Authorization"] = f"Bearer {gh_token}"
        try:
            r = requests.get(api_url, headers=headers, timeout=60)
            if r.status_code == 200:
                j = r.json().get("commit", {})
                dt = (j.get("author", {}) or {}).get("date") or (j.get("committer", {}) or {}).get("date")
                return utc(dt) if dt else None
            if verbose:
                print(f"[LEAD] GitHub API {r.status_code} for {api_url}")
            return None
        except requests.RequestException as e:
            if verbose: print(f"[LEAD] GitHub lookup failed: {e}")
            return None

    if verbose:
        print(f"[LEAD] unknown repo type '{rtype}' for build {build.get('id')}")
    return None

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="DORA via Build Timelines (slot swap as prod deploy).")
    ap.add_argument("--org", required=True)
    ap.add_argument("--project", required=True)
    ap.add_argument("--pipeline-ids", help="CSV build definition IDs")
    ap.add_argument("--pipeline-names", help="CSV names or substrings to match (auto-resolve IDs)")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--branch", default="refs/heads/main", help="Branch ref to filter (set empty to disable)")
    ap.add_argument("--pat", help="Fallback PAT (optional)")
    ap.add_argument("--github-token", help="GitHub token for commit timestamps (optional)")
    ap.add_argument("--github-repo", help='Override owner/repo for commit lookup, e.g. "org/app-repo"')
    ap.add_argument("--app", help="Logical application name for CSVs (optional)")
    ap.add_argument("--out-prefix", default="", help="Prefix for output filenames, e.g. 'email-queue-'")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = ap.parse_args()

    # env fallbacks
    if not args.github_token:
        args.github_token = os.getenv("GITHUB_TOKEN")
    if not args.github_repo:
        args.github_repo = os.getenv("GITHUB_REPO")

    APP = args.app or ""
    OUT = args.out_prefix or ""

    ado = ADO(args.org, args.project, args.pat or os.getenv("AZDO_PAT"))

    if args.pipeline_ids and args.pipeline_ids.strip():
        defs = [int(x) for x in args.pipeline_ids.split(",") if x.strip()]
    elif args.pipeline_names and args.pipeline_names.strip():
        defs = resolve_definition_ids(ado, args.pipeline_names)
        if not defs:
            raise SystemExit(f"No pipeline definitions matched names: {args.pipeline_names}")
    else:
        raise SystemExit("Provide --pipeline-ids or --pipeline-names")

    # Pretty names for CLI traces
    def_names = {}
    for did in defs:
        try:
            dd = ado.get_definition(did)
            def_names[did] = dd.get("name")
        except Exception:
            pass

    until = datetime.now(timezone.utc) + timedelta(minutes=5)
    since = until - timedelta(days=args.days)

    builds = ado.list_builds(defs, since, until, branch=args.branch)
    deployments, failures = [], []

    for b in builds:
        bid = b.get("id")
        timeline = ado.get_timeline(bid)
        recs = timeline.get("records", [])

        def has_job(sub):
            sub = sub.lower()
            return next((r for r in recs if r.get("type") == "Job" and sub in (r.get("name","").lower())), None)

        def has_stage(name_sub):
            name_sub = name_sub.lower()
            return next((r for r in recs if r.get("type") == "Stage" and name_sub in (r.get("name","").lower())), None)

        swap = has_job("swap")
        validate_swap = has_job("validate")
        rollback = has_job("rollback")
        _ = has_stage("deploylive")  # optional

        # Successful production deployment = swap job succeeded
        if swap and (swap.get("result") or "").lower() == "succeeded":
            dep_time = utc(swap.get("finishTime"))
            if dep_time:
                deployments.append({"buildId": bid, "when": dep_time})

                # Console trace (useful in logs)
                repo = b.get("repository") or {}
                def_id = (b.get("definition") or {}).get("id")
                def_name = def_names.get(def_id)
                if args.verbose:
                    print(f"[DEPLOY] def={def_id}({def_name}) build={bid} when={fmtiso(dep_time)} "
                          f"repo.type={repo.get('type')} repo.name={repo.get('name')} sha={b.get('sourceVersion')}")

            # Post-deploy failures for CFR/MTTR
            failed_change, fail_t = False, None
            if validate_swap and (validate_swap.get("result") or "").lower() == "failed":
                failed_change, fail_t = True, utc(validate_swap.get("finishTime"))
            elif rollback and (rollback.get("result") or "").lower() == "succeeded":
                failed_change, fail_t = True, utc(rollback.get("finishTime"))
            if failed_change and fail_t:
                failures.append({"buildId": bid, "when": fail_t})

    # ----- Deployment Frequency (daily) -----
    daily = {}  # date -> [deploy times...]
    for d in deployments:
        k = day_key(d["when"])
        daily.setdefault(k, []).append(d["when"])
    # Normalize each day’s list (sort for first/last)
    for k in daily:
        daily[k] = sorted(daily[k])

    # ----- Change Failure Rate -----
    cfr_pct = (len(failures) / len(deployments) * 100.0) if deployments else 0.0

    # ----- Lead Time for Changes (per deployment) -----
    run_base_url = f"https://dev.azure.com/{ado.org}/{ado.project}/_build/results?buildId="
    lead_rows = []
    dep_by_build = {d["buildId"]: d for d in deployments}
    for b in builds:
        bid = b.get("id")
        dep = dep_by_build.get(bid)
        if not dep:
            continue
        ct = get_commit_time(
            b, ado_client=ado,
            gh_token=args.github_token,
            gh_repo_override=args.github_repo,
            verbose=args.verbose
        )
        if ct and dep["when"]:
            secs = int((dep["when"] - ct).total_seconds())
            lead_rows.append({
                "app": APP,
                "buildId": bid,
                "commit": b.get("sourceVersion"),
                "commitTimeUtc": fmtiso(ct),
                "deployTimeUtc": fmtiso(dep["when"]),
                "leadTimeSeconds": secs,
                "leadTimeHours": round(secs/3600, 3),
                "leadTimeHuman": human_dur(secs),
                "runUrl": run_base_url + str(bid)
            })

    # ----- MTTR (Failed Deployment Recovery Time) -----
    successes_sorted = sorted(deployments, key=lambda x: x["when"])
    mttr_rows = []
    for f in sorted(failures, key=lambda x: x["when"]):
        next_success = next((s for s in successes_sorted if s["when"] > f["when"]), None)
        if next_success:
            secs = int((next_success["when"] - f["when"]).total_seconds())
            mttr_rows.append({
                "app": APP,
                "failedBuildId": f["buildId"],
                "failedAtUtc": fmtiso(f["when"]),
                "restoredBuildId": next_success["buildId"],
                "restoredAtUtc": fmtiso(next_success["when"]),
                "mttrSeconds": secs,
                "mttrHours": round(secs/3600, 3),
                "mttrHuman": human_dur(secs),
                "failedRunUrl": run_base_url + str(f["buildId"]),
                "restoredRunUrl": run_base_url + str(next_success["buildId"]),
            })

    # ---------- WRITE THE 4 CSVs (daily frequency) ----------
    window_start = since.astimezone(timezone.utc).isoformat()
    window_end   = until.astimezone(timezone.utc).isoformat()

    # 1) Deployment Frequency (daily)
    with open(f"{OUT}deployment_frequency.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "app","date","deployments","firstDeployAtUtc","lastDeployAtUtc","weekday"
        ])
        w.writeheader()
        for day in sorted(daily.keys()):  # chronological
            times = daily[day]
            first = times[0]
            last = times[-1]
            weekday = datetime.fromisoformat(day).strftime("%a")
            w.writerow({
                "app": APP,
                "date": day,
                "deployments": len(times),
                "firstDeployAtUtc": fmtiso(first),
                "lastDeployAtUtc": fmtiso(last),
                "weekday": weekday
            })

    # 2) Lead Time for Changes (per deployment)
    lead_rows_sorted = sorted(lead_rows, key=lambda r: r["deployTimeUtc"])
    with open(f"{OUT}lead_time_for_changes.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "app","buildId","runUrl","commit","commitTimeUtc","deployTimeUtc",
            "leadTimeSeconds","leadTimeHours","leadTimeHuman"
        ])
        w.writeheader()
        for r in lead_rows_sorted:
            w.writerow(r)

    # 3) Change Failure Rate (window summary)
    with open(f"{OUT}change_failure_rate.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "app","windowStartUtc","windowEndUtc","totalDeployments","failedChanges","changeFailureRatePct"
        ])
        w.writeheader()
        w.writerow({
            "app": APP,
            "windowStartUtc": window_start,
            "windowEndUtc": window_end,
            "totalDeployments": len(successes_sorted),
            "failedChanges": len(failures),
            "changeFailureRatePct": round(cfr_pct, 2)
        })

    # 4) Failed Deployment Recovery Time (MTTR rows)
    mttr_rows_sorted = sorted(mttr_rows, key=lambda r: r["failedAtUtc"])
    with open(f"{OUT}failed_deployment_recovery_time.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "app","failedBuildId","failedAtUtc","failedRunUrl",
            "restoredBuildId","restoredAtUtc","restoredRunUrl",
            "mttrSeconds","mttrHours","mttrHuman"
        ])
        w.writeheader()
        for r in mttr_rows_sorted:
            w.writerow(r)

    # Console summary
    def fmt_avg(sec_list):
        if not sec_list: return "n/a"
        avg = sum(sec_list)/len(sec_list)
        h=int(avg//3600); m=int((avg%3600)//60); s=int(avg%60)
        return f"{h}h {m}m {s}s"

    print("\n=== DORA SUMMARY (Timeline-based) ===")
    print(f"App: {APP or '(n/a)'} | Window: {args.days} days | Pipelines: {defs} | Branch: {args.branch or '(no filter)'}")
    print(f"Deployment Frequency (days reported): {len(daily)}")
    print(f"Change Failure Rate: {cfr_pct:.1f}%  (deployments={len(successes_sorted)}, failures={len(failures)})")
    print(f"Lead Time avg: {fmt_avg([r['leadTimeSeconds'] for r in lead_rows])}  (n={len(lead_rows)})")
    print(f"MTTR avg: {fmt_avg([r['mttrSeconds'] for r in mttr_rows])}  (n={len(mttr_rows)})")
    print(f"Artifacts: {OUT}deployment_frequency.csv, {OUT}lead_time_for_changes.csv, {OUT}change_failure_rate.csv, {OUT}failed_deployment_recovery_time.csv")

if __name__ == "__main__":
    main()

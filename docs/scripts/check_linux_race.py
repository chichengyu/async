"""Actually execute Linux race tests using an existing WSL/local toolchain.

Run from Windows with Python 3 and WSL docker-desktop. Requires an existing
Linux Go root and a working C compiler (possibly a wrapper with a local
sysroot). This script does not install tools or modify system configuration.
Set ASYNC_LINUX_GO_ROOT and ASYNC_LINUX_CC to their absolute Linux paths.
Optional package arguments replace ./...; count1/audit/race are fixed and
short is enabled by default.
ASYNC_LINUX_RACE_NONSHORT=1 disables short; ASYNC_LINUX_RACE_FOLDER selects
a separate evidence directory below .gotmp. The manifest records source
hashes before execution; never attribute later source changes to this run.
ASYNC_LINUX_RACE_RUN optionally selects a Go test regular expression; the
manifest preserves it so a selected run cannot be confused with a full suite.
"""
from pathlib import Path
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time

root=Path(__file__).resolve().parents[2]
linux_root="/mnt/host/"+root.drive[0].lower()+root.as_posix()[2:]
go_root=os.environ.get("ASYNC_LINUX_GO_ROOT")
compiler=os.environ.get("ASYNC_LINUX_CC")
if not go_root or not compiler:
 raise SystemExit("Set ASYNC_LINUX_GO_ROOT and ASYNC_LINUX_CC to existing Linux tool paths")
folder=root/".gotmp"/os.environ.get("ASYNC_LINUX_RACE_FOLDER","linux_race_actual")
folder.mkdir(parents=True,exist_ok=True)
packages=sys.argv[1:] or ["./..."]
short=os.environ.get("ASYNC_LINUX_RACE_NONSHORT")!="1"
quote=shlex.quote
environment={"GOROOT":go_root,"PATH":go_root+"/bin:/usr/bin:/bin",
 "GOTOOLCHAIN":"local","GOCACHE":linux_root+"/.gotmp/linux_go_race_cache",
 "GOTMPDIR":linux_root+"/.gotmp","GOMAXPROCS":"2","CGO_ENABLED":"1","CC":compiler}
prefix="cd "+quote(linux_root)+" && "+" ".join(k+"="+quote(v) for k,v in environment.items())+" "
def linux(command):
 return subprocess.check_output(["wsl","-d","docker-desktop","--","sh","-c",prefix+command],text=True).strip()
expected=linux("go list "+" ".join(map(quote,packages))).splitlines()
flags=["go","test","-race","-tags=audit","-p=1","-count=1","-timeout=1800s","-json"]
if short:flags.append("-short")
pattern=os.environ.get("ASYNC_LINUX_RACE_RUN")
if pattern:flags.extend(["-run",pattern])
command=prefix+" ".join(map(quote,flags+packages))
sources={}
for path in root.rglob("*.go"):
 relative=path.relative_to(root)
 if any(part.startswith(".") for part in relative.parts):continue
 sources[relative.as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
for package in packages:
 if package.startswith("./.gotmp/"):
  for path in (root/package).rglob("*.go"):
   sources[path.relative_to(root).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
manifest={"platform":"linux/amd64","race":True,"short":short,"count":1,"tags":["audit"],
 "run_pattern":pattern,"requested":packages,"expected_packages":expected,"go_version":linux("go version"),
 "uname":linux("uname -a"),"compiler":compiler,"source_sha256":sources,"command":command,
 "started_unix":time.time()}
report=folder/"manifest.json"
report.write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print("Executing Linux race tests:",", ".join(packages),flush=True)
started=time.monotonic()
with (folder/"events.jsonl").open("w",encoding="utf-8") as output, (folder/"stderr.log").open("w",encoding="utf-8") as stderr:
 result=subprocess.run(["wsl","-d","docker-desktop","--","sh","-c",command],stdout=output,stderr=stderr,text=True)
outcomes=[];failures=[];executed_tests=[]
for line in (folder/"events.jsonl").read_text(encoding="utf-8").splitlines():
 event=json.loads(line)
 if event["Action"]=="fail":failures.append(event)
 if event["Action"]=="run" and event.get("Test"):
  executed_tests.append({"package":event["Package"],"test":event["Test"]})
 if not event.get("Test") and event["Action"] in ("pass","fail","skip"):
  outcomes.append({key:event[key] for key in ("Package","Action","Elapsed") if key in event})
selection_executed=not pattern or bool(executed_tests)
passed=result.returncode==0 and not failures and selection_executed and sorted(item["Package"] for item in outcomes)==sorted(expected) and all(item["Action"] in ("pass","skip") for item in outcomes)
manifest.update(exit_code=result.returncode,elapsed_seconds=round(time.monotonic()-started,3),packages=outcomes,failures=failures,executed_tests=executed_tests,selection_executed=selection_executed,verified=passed)
report.write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(f"exit={result.returncode} verified={passed}; manifest: {report}",flush=True)
if not passed:print((folder/"stderr.log").read_text(encoding="utf-8")[-6000:])
raise SystemExit(not passed)

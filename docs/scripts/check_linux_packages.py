"""Cross-compile and actually execute Go short tests in an existing WSL distro.

Run from Windows with Go 1.25+ and Python 3 on PATH:
    python docs/scripts/check_linux_packages.py
Optional package arguments replace ./...; no distro or system packages are
installed. Requires the existing docker-desktop WSL distro and its host mounts.
Tests use CGO_ENABLED=0 and therefore do not claim Linux race coverage.
ASYNC_LINUX_GO_ROOT may name an existing Linux Go root for tests invoking go;
ASYNC_LINUX_AUDIT_FOLDER selects a folder name under .gotmp for separate runs.
"""
from pathlib import Path
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[2]
folder = root / '.gotmp' / os.environ.get('ASYNC_LINUX_AUDIT_FOLDER', 'linux_short_actual')
folder.mkdir(parents=True, exist_ok=True)
linux_root = '/mnt/host/' + root.drive[0].lower() + root.as_posix()[2:]
env = os.environ.copy()
env.update(GOOS='linux', GOARCH='amd64', CGO_ENABLED='0', GOMAXPROCS='2')
requested = sys.argv[1:] or ['./...']
packages = subprocess.check_output(['go','list',*requested],cwd=root,env=env,text=True).splitlines()
# Run the longer root integration package after the smaller packages.
packages.sort(key=lambda package: (package == 'github.com/chichengyu/async', package))
records = []
manifest = folder / 'manifest.json'
metadata = {
    'platform':'linux/amd64', 'race':False, 'short':True, 'tags':['audit'],
    'requested':requested, 'expected_packages':packages,
    'windows_go_version':subprocess.check_output(['go','version'],cwd=root,env=env,text=True).strip(),
    'linux_uname':subprocess.check_output(['wsl','-d','docker-desktop','--','uname','-a'],text=True).strip(),
}

def save():
    manifest.write_text(json.dumps({**metadata, 'packages':records},indent=2),encoding='utf-8')

for index, package in enumerate(packages):
    name = package.removeprefix('github.com/chichengyu/async').strip('/').replace('/','_') or 'root'
    binary = folder / (name + '.test')
    log = folder / (name + '.log')
    # A no-test package must not accidentally execute an old binary.
    binary.unlink(missing_ok=True)
    print(f'[{index+1}/{len(packages)}] compile {package}',flush=True)
    built = subprocess.run(['go','test','-c','-tags=audit','-o',str(binary),package],
        cwd=root,env=env,capture_output=True,text=True,encoding='utf-8')
    if built.returncode:
        log.write_text(built.stdout+built.stderr,encoding='utf-8')
        records.append({'package':package,'compile_exit':built.returncode,'log':log.name})
        save();print(built.stdout+built.stderr,flush=True);raise SystemExit(built.returncode)
    if not binary.exists():
        records.append({'package':package,'no_test_binary':True});save();continue
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    linux_binary = linux_root + '/' + binary.relative_to(root).as_posix()
    linux_go = os.environ.get('ASYNC_LINUX_GO_ROOT')
    runtime_env = 'GOMAXPROCS=2'
    if linux_go:
        runtime_env += ' GOROOT=' + shlex.quote(linux_go) + ' PATH=' + shlex.quote(linux_go + '/bin:/usr/bin:/bin') + ' GOTOOLCHAIN=local'
        cache = root / '.gotmp/linux_go_cache'
        cache.mkdir(parents=True, exist_ok=True)
        runtime_env += ' GOCACHE=' + shlex.quote(linux_root + '/.gotmp/linux_go_cache')
    command = 'cd ' + shlex.quote(linux_root) + ' && ' + runtime_env + ' ' + shlex.quote(linux_binary)
    command += ' -test.short=true -test.count=1 -test.timeout=900s -test.v=true'
    print(f'[{index+1}/{len(packages)}] execute {package}',flush=True)
    started = time.monotonic()
    with log.open('w',encoding='utf-8') as output:
        run = subprocess.run(['wsl','-d','docker-desktop','--','sh','-c',command],
            cwd=root,stdout=output,stderr=subprocess.STDOUT,text=True)
    elapsed = time.monotonic()-started
    records.append({'package':package,'compile_exit':0,'run_exit':run.returncode,
        'binary_sha256':digest,'elapsed_seconds':round(elapsed,3),'log':log.name})
    save()
    print(f'[{index+1}/{len(packages)}] exit={run.returncode} elapsed={elapsed:.3f}s {package}',flush=True)
    if run.returncode:
        print(log.read_text(encoding='utf-8')[-6000:],flush=True)
        raise SystemExit(run.returncode)
executed = sum('run_exit' in record for record in records)
print(f'Executed {executed} Linux test binaries across {len(records)} packages; manifest: {manifest}',flush=True)

"""Real HN snapshot, cross-SDK crashes, commit-gap replay, schedules, offline SQL.

Run with a Python environment containing deoos 0.8.0, duckdb and boto3. The Node
example must resolve deoos 0.8.0 and @duckdb/node-api. Only local RustFS is used.
The first public API responses are cached unchanged for the remaining cells;
evidence distinguishes upstream requests from replay, rather than calling all
four cells independent live-source runs.
"""
import argparse, collections, datetime, hashlib, http.server, importlib.util, json
import os, pathlib, socket, subprocess, sys, tempfile, threading, time
import urllib.request, uuid
import boto3, duckdb
from botocore.exceptions import ClientError

ROOT = pathlib.Path(__file__).resolve().parents[1]
HANDLER = 'hacker-news.collect.v1'
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--endpoint', default='http://127.0.0.1:19000')
parser.add_argument('--python', default=sys.executable)
parser.add_argument('--node', default=os.environ.get('DEOOS_TEST_NODE', 'node'))
parser.add_argument('--python-example', default=str(ROOT / 'python/hacker-news/hacker_news.py'))
parser.add_argument('--node-example', default=str(ROOT / 'typescript/hacker-news/hacker_news.mjs'))
args = parser.parse_args()
spec = importlib.util.spec_from_file_location('hacker_news_example', args.python_example)
example = importlib.util.module_from_spec(spec); spec.loader.exec_module(example)


def wait(predicate, timeout=180, process=None, log=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process and process.poll() is not None:
            raise AssertionError(f'process exited {process.returncode}: {log.read_text()}')
        try:
            result = predicate()
            if result: return result
        except (OSError, urllib.error.URLError): pass
        time.sleep(.1)
    raise AssertionError('timed out waiting for behavioral condition')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); return sock.getsockname()[1]


class Source:
    def __init__(self):
        self.cache, self.requests, self.upstream, self.ids = {}, [], [], []
        self.lock, self.injected = threading.Lock(), False
        source = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused): pass
            def do_GET(self):
                path = self.path.removeprefix('/v0')
                with source.lock:
                    if source.ids and path == f'/item/{source.ids[20]}.json':
                        if not source.injected:
                            source.injected = True
                            source.requests.append(dict(path=path, status=503, kind='injected'))
                            self.send_response(503); self.end_headers(); return
                    kind = 'replay' if path in source.cache else 'upstream'
                    if path not in source.cache:
                        with urllib.request.urlopen('https://hacker-news.firebaseio.com/v0' + path, timeout=30) as response:
                            raw = response.read(); value = json.loads(raw)
                        source.cache[path] = raw
                        source.upstream.append(dict(path=path, sha256=hashlib.sha256(raw).hexdigest(), at=time.time()))
                        if path == '/topstories.json': source.ids = value[:100]
                    raw = source.cache[path]
                    source.requests.append(dict(path=path, status=200, kind=kind, at=time.time()))
                self.send_response(200); self.send_header('Content-Type', 'application/json')
                self.end_headers(); self.wfile.write(raw)
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_port}/v0'
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
    def stop(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
    def counts(self, start):
        return dict(collections.Counter(row['path'] for row in self.requests[start:]))


PY_WORKER = '''import importlib.util,json,os,pathlib,time
spec=importlib.util.spec_from_file_location('hn',os.environ['HN_EXAMPLE'])
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
c=m.create_client()
def barrier(ctx,name):
 pathlib.Path(os.environ['HN_READY']).write_text(json.dumps({'task_id':ctx.task['id'],'step':name,'stage':os.environ['HN_STAGE']}))
 print(json.dumps({'barrier':name,'task_id':ctx.task['id']}),flush=True)
 while not pathlib.Path(os.environ['HN_RELEASE']).exists():time.sleep(.05)
def collect(ctx,inputs):
 original=ctx.step;stores=0
 def step(name,function,*a,**kw):
  nonlocal stores
  def callback():
   result=function()
   if os.environ['HN_STAGE']=='gap' and name==os.environ['HN_GAP']:barrier(ctx,name)
   return result
  result=original(name,callback,*a,**kw)
  if name.startswith('store-'):
   stores+=1
   if os.environ['HN_STAGE']=='checkpoint' and stores==20:barrier(ctx,name)
  return result
 ctx.step=step
 return m.collect(ctx,inputs)
try:
 while True:
  try:
   if c.run_once({'hacker-news.collect.v1':collect}):break
  except Exception as error:print(json.dumps({'retry_error':str(error)}),flush=True)
  time.sleep(.1)
finally:c.close()
'''
JS_WORKER = '''import {writeFileSync,existsSync} from 'node:fs';
import {pathToFileURL} from 'node:url';import {setTimeout as delay} from 'node:timers/promises';
const m=await import(pathToFileURL(process.env.HN_EXAMPLE));const c=m.create_client();
async function barrier(ctx,name){writeFileSync(process.env.HN_READY,JSON.stringify({task_id:ctx.task.id,step:name,stage:process.env.HN_STAGE}));console.log(JSON.stringify({barrier:name,task_id:ctx.task.id}));while(!existsSync(process.env.HN_RELEASE))await delay(50);}
async function collect(ctx,inputs){const original=ctx.step.bind(ctx);let stores=0;
ctx.step=async(name,fn,...rest)=>{const result=await original(name,async()=>{const result=await fn();if(process.env.HN_STAGE==='gap'&&name===process.env.HN_GAP)await barrier(ctx,name);return result;},...rest);
if(name.startsWith('store-')&&++stores===20&&process.env.HN_STAGE==='checkpoint')await barrier(ctx,name);return result;};return m.collect(ctx,inputs);}
while(true){try{if(await c.runOnce({'hacker-news.collect.v1':collect}))break;}catch(error){console.log(JSON.stringify({retry_error:String(error)}));}await delay(100);}
'''


def rows(database):
    with duckdb.connect(str(database), read_only=True) as conn:
        payloads = {int(identifier): json.loads(payload) for identifier, payload in conn.execute('SELECT id,payload FROM stories ORDER BY id').fetchall()}
        links = conn.execute('SELECT task_id,story_id,rank FROM collections ORDER BY task_id,rank').fetchall()
    hashes = {str(identifier): hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest() for identifier, value in payloads.items()}
    return payloads, links, hashes


def seed(database, source):
    identifier = source.ids[0]
    item = json.loads(source.cache[f'/item/{identifier}.json'])
    payload = dict(item if item is not None else {'id': identifier}, _preserve=True)
    raw = json.dumps(payload)
    with duckdb.connect(str(database)) as conn:
        conn.execute('CREATE TABLE stories(id BIGINT PRIMARY KEY,payload JSON NOT NULL)')
        conn.execute('CREATE TABLE collections(task_id VARCHAR NOT NULL,story_id BIGINT NOT NULL,rank INTEGER NOT NULL,PRIMARY KEY(task_id,story_id))')
        conn.execute('INSERT INTO stories VALUES (?, ?)', [identifier, raw])
    return identifier, raw


def preserved(database, seeded):
    with duckdb.connect(str(database), read_only=True) as conn:
        assert conn.execute('SELECT payload FROM stories WHERE id=?', [seeded[0]]).fetchone()[0] == seeded[1]


def verify(database, source, tasks, seeded, baseline=None):
    ids = source.ids
    payloads, links, hashes = rows(database)
    assert set(payloads) == set(ids) and len(links) == 100 * len(tasks)
    expected = {identifier: json.loads(source.cache[f'/item/{identifier}.json']) for identifier in ids}
    expected[seeded[0]] = json.loads(seeded[1])
    assert payloads == expected, 'stored JSON differs from captured public API payloads'
    for task in tasks:
        selected = [(int(identifier), rank) for owner, identifier, rank in links if owner == task]
        assert selected == list(zip(ids, range(1, 101))), selected
    if baseline: assert all(hashes[key] == value for key, value in baseline.items())
    return hashes


bucket = 'deoos-hn-' + uuid.uuid4().hex[:20]
os.environ.update(AWS_ACCESS_KEY_ID='local-development', AWS_SECRET_ACCESS_KEY='local-development-only-secret',
                  AWS_REGION='us-east-1', AWS_ENDPOINT=args.endpoint, AWS_ALLOW_HTTP='true',
                  DEOOS_STORAGE_BUCKET=bucket, DEOOS_STORAGE_PROVIDER='s3', LEASE_MS='2000')
os.environ.pop('AWS_SESSION_TOKEN', None)
s3 = boto3.client('s3', endpoint_url=os.environ['AWS_ENDPOINT'], region_name='us-east-1')
report = dict(started=datetime.datetime.now(datetime.timezone.utc).isoformat(), bucket=bucket, cases=[], schedules=[], cleaned=False,
              source_policy='Each upstream public response captured once; subsequent source requests replay its unchanged bytes.')
procs, clients, created, source = [], [], False, None
evidence = ROOT / 'outputs/evidence/hacker-news.json'
evidence.parent.mkdir(parents=True, exist_ok=True)
try:
    s3.create_bucket(Bucket=bucket); created = True; source = Source()
    with urllib.request.urlopen(source.url + '/topstories.json', timeout=30) as response: json.load(response)
    with urllib.request.urlopen(source.url + f'/item/{source.ids[0]}.json', timeout=30) as response: json.load(response)
    with tempfile.TemporaryDirectory(prefix='deoos-hn-') as scratch:
        scratch = pathlib.Path(scratch).resolve()
        py_wrapper, js_wrapper = scratch / 'worker.py', scratch / 'worker.mjs'
        py_wrapper.write_text(PY_WORKER); js_wrapper.write_text(JS_WORKER)
        def command(sdk, *arguments):
            return [args.python, args.python_example, *arguments] if sdk == 'python' else [args.node, args.node_example, *arguments]
        def cli(sdk, *arguments):
            result = subprocess.run(command(sdk, *arguments), capture_output=True, text=True, timeout=180, env=os.environ.copy())
            assert result.returncode == 0, result.stdout + result.stderr
            return json.loads(result.stdout)
        def worker(sdk, stage, label):
            ready, release, log = (scratch / f'{label}.{suffix}' for suffix in ['ready', 'release', 'log'])
            env = dict(os.environ, HN_EXAMPLE=args.python_example if sdk == 'python' else args.node_example,
                       HN_READY=str(ready), HN_RELEASE=str(release), HN_STAGE=stage,
                       HN_GAP=f'store-{source.ids[20]}' if source.ids else '')
            cmd = [args.python, str(py_wrapper)] if sdk == 'python' else [args.node, str(js_wrapper)]
            with log.open('w') as output: process = subprocess.Popen(cmd, env=env, stdout=output, stderr=subprocess.STDOUT)
            procs.append(process); wait(ready.exists, process=process, log=log)
            return process, json.loads(ready.read_text()), release, log
        def reclaim_delay():
            # The barrier worker renews its lease. A snapshot taken before the
            # kill can expire while the server still has a newer expiry.
            deadline = time.monotonic() + 30
            last = None
            while time.monotonic() < deadline:
                last = c.inspect(task_id)
                if last.get('status') != 'running' or last.get('expires_at', 0) <= time.time() * 1000:
                    time.sleep(.2)
                    return
                time.sleep(.1)
            raise AssertionError(f'lease did not expire: status={last.get("status")} expires_at={last.get("expires_at")}')
        def resume(sdk):
            # One claim can lose the race with a renewed lease or a transient
            # storage 503. The task stays queued while attempts remain.
            deadline = time.monotonic() + 180
            detail = ''
            while time.monotonic() < deadline:
                result = subprocess.run(command(sdk, 'work', '--once'), capture_output=True, text=True, timeout=180, env=os.environ.copy())
                detail = result.stdout + result.stderr
                state = c.inspect(task_id)
                if state['status'] == 'completed':
                    return state
                if state['status'] in ('failed', 'cancelled') and state.get('attempts', 0) >= 5:
                    raise AssertionError(detail + '\n' + json.dumps({key: state.get(key) for key in ('status', 'error', 'attempts')}))
                if state['status'] == 'running' and state.get('expires_at', 0) > time.time() * 1000:
                    reclaim_delay()
                else:
                    time.sleep(.3)
            raise AssertionError('resume did not complete: ' + detail)
        for mode in ['library', 'server']:
            for sdk in ['python', 'typescript']:
                opposite = 'typescript' if sdk == 'python' else 'python'
                label = mode + '-' + sdk; started = time.monotonic()
                os.environ.update(DEOOS_MODE=mode, EXECUTION_PREFIX='hacker-news/' + label + '/' + uuid.uuid4().hex,
                                  ENGINE_TOKEN='local-hn-test-only-token')
                server = None
                if mode == 'server':
                    port = free_port(); os.environ.update(ENGINE_URL=f'http://127.0.0.1:{port}', ENGINE_BIND=f'127.0.0.1:{port}')
                    log = scratch / f'{label}-server.log'
                    binary = os.environ.get('ENGINE_BINARY', str(ROOT / '.deoos/release/bin/deoos-server'))
                    with log.open('w') as output: server = subprocess.Popen([binary], env=os.environ.copy(), stdout=output, stderr=subprocess.STDOUT)
                    procs.append(server); wait(lambda: urllib.request.urlopen(os.environ['ENGINE_URL'] + '/health', timeout=1).status == 200, process=server, log=log)
                c = example.create_client(); clients.append(c)
                database = scratch / f'{label}.duckdb'; task_id = label; request_start = len(source.requests)
                seeded = seed(database, source)
                cli(sdk, 'submit', '--id', task_id, '--database', str(database), '--source', source.url, '--count', '100')
                first, barrier, _, _ = worker(sdk, 'checkpoint', label + '-20')
                state = c.inspect(task_id); ids = list(source.ids); assert len(ids) == len(set(ids)) == 100
                assert barrier['step'] == f'store-{ids[19]}' and barrier['step'] in state['steps']
                payloads, links, first_hashes = rows(database)
                assert set(payloads) == set(ids[:20]) and len(links) == 20
                preserved(database, seeded)
                first_counts = source.counts(request_start); first.kill(); first.wait(); reclaim_delay()
                second, gap, _, retry_log = worker(opposite, 'gap', label + '-21')
                state = c.inspect(task_id)
                assert gap['step'] == f'store-{ids[20]}' and gap['step'] not in state['steps']
                assert f'fetch-{ids[20]}' in state['steps']
                payloads, links, gap_hashes = rows(database)
                assert set(payloads) == set(ids[:21]) and len(links) == 21
                preserved(database, seeded)
                assert all(gap_hashes[key] == value for key, value in first_hashes.items())
                gap_counts = source.counts(request_start)
                assert all(gap_counts[path] == count for path, count in first_counts.items())
                second.kill(); second.wait(); reclaim_delay()
                final = resume(sdk)
                assert final['output'] == dict(task_id=task_id, stories=100, database=str(database)), final['output']
                hashes = verify(database, source, [task_id], seeded, gap_hashes)
                preserved(database, seeded)
                counts = source.counts(request_start)
                assert all(counts[path] == count for path, count in gap_counts.items())
                assert counts['/topstories.json'] == 1
                assert all(counts[f'/item/{identifier}.json'] == (2 if label == 'library-python' and rank == 20 else 1) for rank, identifier in enumerate(ids))
                assert final['attempts'] == (4 if label == 'library-python' else 3)
                inspected = cli(opposite, 'inspect', '--id', task_id); assert inspected['status'] == 'completed'
                report['cases'].append(dict(mode=mode, starting_sdk=sdk, resume_sdk=opposite, final_sdk=sdk,
                    snapshot_ids=ids, faultpoints=[barrier, gap], status=final['status'], attempts=final['attempts'],
                    first20_hashes=first_hashes, committed_gap_hashes=gap_hashes, final_hashes=hashes,
                    source_counts=counts, retry_log=retry_log.read_text(), seconds=round(time.monotonic() - started, 3)))
                report['cases'][-1]['seed_preserved'] = dict(story_id=seeded[0], raw_sha256=hashlib.sha256(seeded[1].encode()).hexdigest())
                print(json.dumps(dict(case=label, status='passed', attempts=final['attempts'])), flush=True)
                # A held first occurrence proves skip-overlap; the next occurrence runs in the other SDK.
                scheduled = scratch / f'{label}-scheduled.duckdb'; anchor = int(time.time() * 1000)
                schedule_seed = seed(scheduled, source)
                cli(sdk, 'schedule', '--id', 'hn-interval', '--database', str(scheduled), '--source', source.url,
                    '--count', '100', '--interval-ms', '1000', '--first-due-ms', str(anchor))
                process, held, release, schedule_log = worker(sdk, 'checkpoint', label + '-schedule')
                time.sleep(1.1)
                assert c.request('/claim', dict(worker='overlap-probe', handlers=[HANDLER]))['task'] is None
                skipped = c.inspect_schedule('hn-interval'); assert skipped['next_due_ms'] > anchor
                _, _, saved = rows(scheduled); release.touch(); process.wait(timeout=180)
                assert process.returncode == 0, schedule_log.read_text()
                first_task = c.inspect(held['task_id']); assert first_task['status'] == 'completed'
                time.sleep(max(0, (c.inspect_schedule('hn-interval')['next_due_ms'] - time.time() * 1000) / 1000) + .1)
                cli(opposite, 'work', '--once'); c.pause_schedule('hn-interval')
                _, links, _ = rows(scheduled); tasks = sorted(set(owner for owner, _, _ in links)); assert len(tasks) == 2
                hashes = verify(scheduled, source, tasks, schedule_seed, saved)
                preserved(scheduled, schedule_seed)
                occurrences = [c.inspect(task) for task in tasks]
                assert all(task['status'] == 'completed' and task['output']['stories'] == 100 for task in occurrences)
                due = sorted(task['schedule']['scheduled_at'] for task in occurrences)
                assert due[1] - due[0] >= 1000 and all((value - anchor) % 1000 == 0 for value in due)
                report['schedules'].append(dict(mode=mode, starting_sdk=sdk, tasks=tasks,
                    scheduled_at=[task['schedule']['scheduled_at'] for task in occurrences], overlap_probe='no claim while first run held',
                    first_due_ms=anchor, interval_ms=1000, per_run_counts={task: 100 for task in tasks},
                    stable_hashes=hashes, skipped_next_due=skipped['next_due_ms']))
                c.close(); clients.remove(c)
                if server: server.terminate(); server.wait(timeout=10)
        source.stop()
        report['source_requests'], report['actual_api_requests'], report['snapshot_ids'] = source.requests, source.upstream, source.ids
        assert len(source.upstream) == 101 and source.injected
        # All workers, engines and source HTTP listeners are stopped before these CLI SQL calls.
        assert all(process.poll() is not None for process in procs)
        offline = []
        for sdk in ['python', 'typescript']:
            result = cli(sdk, 'query', '--database', str(scratch / 'library-python.duckdb'))
            assert result['collections'] == [dict(task_id='library-python', stories=100)], result
            assert [str(row['id']) for row in result['sample']] == list(map(str, source.ids[:10]))
            assert all(row['title'] is None or isinstance(row['title'], str) for row in result['sample'])
            offline.append(dict(sdk=sdk, output=result))
        report['offline_queries'] = offline; report['success'] = True
except BaseException as error:
    report['success'], report['error'] = False, repr(error)
    raise
finally:
    for process in procs:
        if process.poll() is None: process.kill(); process.wait()
    for client in clients: client.close()
    if source and source.thread.is_alive(): source.stop()
    if source:
        report['source_requests'], report['actual_api_requests'], report['snapshot_ids'] = source.requests, source.upstream, source.ids
    if created:
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket):
            objects = [{'Key': item['Key']} for item in page.get('Contents', [])]
            if objects: assert not s3.delete_objects(Bucket=bucket, Delete={'Objects': objects}).get('Errors')
        s3.delete_bucket(Bucket=bucket)
        try: s3.head_bucket(Bucket=bucket)
        except ClientError as error: assert error.response['ResponseMetadata']['HTTPStatusCode'] == 404
        else: raise AssertionError('test bucket still exists')
        report['cleaned'] = True
    report['processes_stopped'] = all(process.poll() is not None for process in procs)
    report['finished'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    evidence.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(success=report.get('success'), cleaned=report['cleaned'], evidence=str(evidence))), flush=True)

import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {atomic, encode, read, rpc, sleep} from '../src/common.mjs';
import {readMetadata} from '../src/metadata.mjs';

const run = path.resolve('.runs', new Date().toISOString().replaceAll(':', '-'));
fs.mkdirSync(run, {recursive: true});
const token = randomUUID(), children = [], evidence = {node: process.version, run, actions: [], checks: []};
let url, coordinator, port = 0;
function launch(script, args, name) {
  const log = fs.createWriteStream(path.join(run, `${name}.log`), {flags: 'a'});
  const child = spawn(process.execPath, [script, ...args], {env: {...process.env, MAPREDUCE_TOKEN: token}, stdio: ['ignore', 'pipe', 'pipe']});
  child.stdout.pipe(log, {end: false}); child.stderr.pipe(log, {end: false});
  child.once('close', () => log.end()); children.push(child); return child;
}
async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise(resolve => child.once('close', resolve)); child.kill('SIGKILL'); await closed;
}
async function until(probe, label, timeout = 20000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const value = await probe(); if (value) return value; await sleep(30); }
  throw new Error(`timeout: ${label}`);
}
const call = (route, method, data) => rpc(`${url}${route}`, token, method, data);
async function startCoordinator() {
  coordinator = launch('src/coordinator.mjs', ['--data', path.join(run, 'coordinator'), '--port', String(port), '--lease', '1500'], 'coordinator');
  let buffer = '';
  const ready = new Promise((resolve, reject) => {
    coordinator.stdout.on('data', chunk => { buffer += chunk; const line = buffer.split('\n')[0]; try { const value = JSON.parse(line); if (value.ready) resolve(value); } catch {} });
    coordinator.once('exit', code => reject(new Error(`coordinator exited ${code}`)));
    setTimeout(() => reject(new Error('coordinator readiness timeout')), 5000).unref();
  });
  const state = await ready; port = state.port; url = `http://127.0.0.1:${port}`;
  const health = await call('/health'); assert.equal(health.pid, coordinator.pid);
  evidence.actions.push({doctor: health, port});
}
function worker(id) { return launch('src/worker.mjs', ['--url', url, '--id', id, '--data', path.join(run, id)], id); }
async function submit(module, keys, splits = 3, reducers = 3) {
  const {id} = await call('/jobs', 'POST', {module, keys, splits, reducers});
  evidence.actions.push({submit: {id, module, count: keys.length, splits, reducers}}); return id;
}
async function done(id) {
  return until(async () => { const job = await call(`/jobs/${id}`); if (job.status === 'failed') throw new Error(JSON.stringify(job)); return job.status === 'done' && job; }, `job ${id}`);
}
function sorted(pairs) { return pairs.sort((a, b) => a[0].localeCompare(b[0])); }
function countOracle(keys) {
  const counts = new Map();
  for (const [, text] of keys) for (const word of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) counts.set(word, (counts.get(word) ?? 0) + 1);
  return sorted([...counts]);
}
async function compare(module, keys, expected) {
  const job = await done(await submit(module, keys)); assert.deepEqual(job.output, expected);
  evidence.checks.push({case: module, id: job.id, output: job.output}); return job;
}
try {
  assert.throws(() => encode({nested: [Infinity]}), /nonfinite/);
  assert.throws(() => encode({nested: [NaN]}), /nonfinite/);
  const legacyId = randomUUID(), legacyToken = randomUUID();
  const corruptRoot = path.join(run, 'corrupt-coordinator');
  atomic(path.join(corruptRoot, 'jobs', `${randomUUID()}.json`), {id: randomUUID()});
  const corrupt = launch('src/coordinator.mjs', ['--data', corruptRoot, '--port', '0'], 'corrupt-coordinator');
  assert.notEqual(await new Promise(resolve => corrupt.once('exit', resolve)), 0, 'corrupt accepted metadata must fail startup');
  evidence.checks.push({case: 'corrupt durable metadata refuses startup', pid: corrupt.pid});
  const legacyJob = {id: legacyId, module: 'wordcount', reducers: 1, status: 'map', tasks: [{index: 0, status: 'running', attempts: 1, worker: 'legacy-worker', token: legacyToken, until: Date.now() + 60000}], events: [{type: 'submitted'}]};
  atomic(path.join(run, 'coordinator', 'inputs', legacyId, '0.json'), [['legacy', 'legacy legacy']]);
  atomic(path.join(run, 'coordinator', 'state.json'), {jobs: {[legacyId]: legacyJob}});
  await startCoordinator();
  assert.equal((await call(`/jobs/${legacyId}`)).tasks[0].status, 'pending');
  await assert.rejects(call(`/jobs/${legacyId}/complete`, 'POST', {phase: 'map', index: 0, token: legacyToken, output: []}), e => e.status === 409);
  await assert.rejects(rpc(`${url}/health`, 'incorrect'), e => e.status === 401);
  await assert.rejects(call('/jobs', 'POST', {module: '../bad', keys: []}), e => e.status === 400);
  await assert.rejects(call('/jobs', 'POST', {module: 'wordcount', keys: [[7, 'bad']]}), e => e.status === 400);
  await assert.rejects(call('/jobs', 'POST', null), e => e.status === 400);
  await assert.rejects(call('/claim', 'POST', null), e => e.status === 400);
  const nonfinite = await fetch(`${url}/jobs`, {method:'POST', headers:{authorization:`Bearer ${token}`}, body:'{"module":"wordcount","keys":[["x",1e400]]}'});
  assert.equal(nonfinite.status, 400, 'nonfinite JSON input must not be persisted as null');
  await assert.rejects(call('/jobs/__proto__'), e => e.status === 404);
  const competitor = launch('src/coordinator.mjs', ['--data', path.join(run, 'coordinator'), '--port', '0'], 'competing-coordinator');
  const competingExit = await new Promise(resolve => competitor.once('exit', resolve)); assert.notEqual(competingExit, 0);
  const peers = [worker('worker-0'), worker('worker-1'), worker('worker-2')];
  assert.deepEqual((await done(legacyId)).output, [['legacy', 2]]);
  assert.equal(read(path.join(run, 'coordinator', 'state.json')).jobs[legacyId].tasks[0].token, legacyToken, 'legacy source remains preserved');
  evidence.checks.push({case: 'legacy running-state import and stale lease fencing', id: legacyId});
  const keys = [['doc-a', 'Alpha beta alpha'], ['doc-a', 'beta gamma'], ['doc-b', 'ALPHA '.repeat(1000)], ['empty', '']];
  const base = await compare('wordcount', keys, countOracle(keys));
  const readings = [['duplicate', {station: 'north', temperature: -8}], ['duplicate', {station: 'north', temperature: -2}], ['x', {station: 'south', temperature: 30}], ['y', {station: 'south', temperature: 19}]];
  const temperatures = new Map();
  for (const [, value] of readings) temperatures.set(value.station, Math.max(temperatures.get(value.station) ?? -Infinity, value.temperature));
  await compare('max-temperature', readings, sorted([...temperatures]));
  const index = new Map();
  for (const [document, text] of keys) for (const word of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) { if (!index.has(word)) index.set(word, new Set()); index.get(word).add(document); }
  await compare('inverted-index', keys, sorted([...index].map(([word, docs]) => [word, [...docs].sort()])));
  for (const module of ['wordcount', 'max-temperature', 'inverted-index']) await compare(module, [], []);
  const invalidId = await submit('max-temperature', [['invalid', {station: 'north', temperature: 'warm'}]], 1, 1);
  const failed = await until(async () => { const j = await call(`/jobs/${invalidId}`); return j.status === 'failed' && j; }, 'bounded invalid job failure');
  assert.equal(failed.tasks[0].attempts, 4);
  assert.equal(failed.events.filter(e => e.type === 'error').length, 4);
  evidence.checks.push({case: 'bounded deterministic module failure', id: invalidId, attempts: failed.tasks[0].attempts});
  for (const peer of peers) await stop(peer);

  const crashKeys = [['a', 'one one'], ['b', 'two'], ['c', 'one three']];
  const crashId = await submit('delayed-count', crashKeys, 3, 2);
  await assert.rejects(call(`/jobs/${crashId}/shuffle/0/0`), e => e.status === 400);
  const victim = worker('victim');
  const assigned = await until(async () => { const j = await call(`/jobs/${crashId}`); return j.tasks.find(t => t.status === 'running' && t.worker === 'victim'); }, 'victim assigned');
  await stop(victim); evidence.actions.push({killedWorker: victim.pid, assigned});
  const survivors = [worker('replacement-0'), worker('replacement-1')];
  const recovered = await done(crashId); assert.deepEqual(recovered.output, countOracle(crashKeys));
  assert(recovered.maps.some(t => t.attempts >= 2)); assert(recovered.events.some(e => e.type === 'expired'));
  await assert.rejects(call(`/jobs/${crashId}/complete`, 'POST', {phase: 'map', index: assigned.index, token: assigned.token, output: []}), e => e.status === 409);
  evidence.checks.push({case: 'worker death and stale lease', id: crashId, events: recovered.events});
  for (const peer of survivors) await stop(peer);

  const restartId = await submit('delayed-count', crashKeys, 3, 2);
  const restartWorker = worker('restart-worker');
  const old = await until(async () => { const j = await call(`/jobs/${restartId}`); return j.tasks.find(t => t.status === 'running'); }, 'restart lease');
  await stop(coordinator); evidence.actions.push({killedCoordinator: coordinator.pid, old});
  const contenders = Array.from({length:4}, (_, i) => launch('src/coordinator.mjs', ['--data', path.join(run, 'coordinator'), '--port', '0'], `recovery-contender-${i}`));
  const outcomes = await Promise.all(contenders.map(child => new Promise((resolve, reject) => {
    let text = '';
    child.stdout.on('data', chunk => {
      text += chunk;
      try { if (JSON.parse(text.split('\n')[0]).ready) resolve({ready:true, pid:child.pid}); } catch {}
    });
    child.once('exit', code => resolve({ready:false, pid:child.pid, code}));
    setTimeout(() => reject(new Error('recovery contender readiness timeout')), 5000).unref();
  })));
  assert.equal(outcomes.filter(item => item.ready).length, 1, 'simultaneous recovery must have exactly one coordinator');
  evidence.checks.push({case:'concurrent recovery has one owner', outcomes});
  for (const child of contenders) await stop(child);
  await startCoordinator();
  await assert.rejects(call(`/jobs/${restartId}/complete`, 'POST', {phase: 'map', index: old.index, token: old.token, output: []}), e => e.status === 409);
  assert.deepEqual((await call(`/jobs/${legacyId}`)).output, [['legacy', 2]], 'authoritative metadata must override stale legacy lease after restart');
  const restartPeer = worker('restart-peer');
  const restarted = await done(restartId); assert.deepEqual(restarted.output, countOracle(crashKeys));
  assert(restarted.maps.some(t => t.attempts >= 2));
  evidence.checks.push({case: 'coordinator restart fences old attempt', id: restartId, output: restarted.output});
  await stop(restartWorker); await stop(restartPeer);

  const acceptedMap = base.maps[0];
  const duplicate = await call(`/jobs/${base.id}/complete`, 'POST', {phase: 'map', index: acceptedMap.index, token: acceptedMap.accepted});
  assert.deepEqual(duplicate, {accepted: true, duplicate: true});
  const acceptedReduce = base.tasks[0];
  assert.deepEqual(await call(`/jobs/${base.id}/complete`, 'POST', {phase: 'reduce', index: acceptedReduce.index, token: acceptedReduce.accepted}), duplicate);
  const snapshot = readMetadata(path.join(run, 'coordinator'));
  for (const job of Object.values(snapshot.jobs)) {
    if (job.status === 'failed') continue;
    const publications = job.events.filter(e => e.type === 'published').map(e => e.token);
    assert.equal(new Set(publications).size, publications.length);
    assert.equal(publications.length, job.maps.length + job.tasks.length);
    for (const task of [...job.maps, ...job.tasks]) assert(fs.existsSync(path.join(run, 'coordinator', 'attempts', `${task.accepted}.json`)));
    assert.deepEqual(read(path.join(run, 'coordinator', 'results', `${job.id}.json`)), (await call(`/jobs/${job.id}`)).output);
  }
  const assignedWorkers = new Set(base.events.filter(e => e.type === 'assigned').map(e => e.worker));
  assert(assignedWorkers.size >= 2, 'maps must run across independent workers');
  const localFiles = fs.readdirSync(run, {recursive: true}).map(String).filter(file => !file.startsWith('coordinator'));
  assert(localFiles.some(file => file.endsWith('input.json')));
  assert(localFiles.some(file => /shuffle-\d+\.json$/.test(file)));
  assert(localFiles.some(file => file.endsWith('output.json')));
  evidence.workerArtifacts = localFiles.filter(file => file.endsWith('.json'));
  evidence.checks.push({case: 'idempotent publication and persisted artifacts', jobs: Object.keys(snapshot.jobs).length, workers: [...assignedWorkers]});
  evidence.status = 'passed';
} catch (error) { evidence.status = 'failed'; evidence.error = error.stack; process.exitCode = 1; }
finally {
  for (const child of children) await stop(child);
  evidence.cleanup = children.every(c => c.exitCode !== null || c.signalCode !== null);
  atomic(path.join(run, 'evidence.json'), evidence);
  assert(fs.existsSync(path.join(run, 'evidence.json')));
  console.log(JSON.stringify({status: evidence.status, checks: evidence.checks.length, cleanup: evidence.cleanup, evidence: path.join(run, 'evidence.json'), error: evidence.error}, null, 2));
}

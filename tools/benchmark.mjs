import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import assert from 'node:assert/strict';
import {spawn, execFileSync} from 'node:child_process';
import {randomUUID, createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {performance} from 'node:perf_hooks';
import {atomic, read, rpc, sleep} from '../src/common.mjs';

export const workload = Object.freeze({version: 1, seed: 421, records: [256, 2048], workers: [1, 2, 4], skews: [0, 0.9], splits: 8, reducers: 4, wordsPerRecord: 16, vocabulary: 128, repeats: 5, jobs: 6, rates: [0.5, 2, 8], sweepSeconds: 10, stabilitySeconds: 120, outstandingLimit: 32, deadlineMs: 60000, leaseMs: 10000});

export function dataset(records, skew) {
  let seed = workload.seed;
  const random = () => { seed = (Math.imul(1664525, seed) + 1013904223) >>> 0; return seed / 4294967296; };
  return Array.from({length: records}, (_, i) => [`doc-${i}`, Array.from({length: workload.wordsPerRecord}, () => random() < skew ? 'hot' : `word${Math.floor(random() * workload.vocabulary)}`).join(' ')]);
}

export function oracle(keys) {
  const counts = new Map();
  for (const [, text] of keys) for (const word of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) counts.set(word, (counts.get(word) ?? 0) + 1);
  return [...counts].sort((a, b) => a[0].localeCompare(b[0]));
}

export function distribution(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const q = p => sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)];
  return {count: sorted.length, min: sorted[0], p50: q(.5), p95: q(.95), p99: q(.99), max: sorted.at(-1)};
}

async function until(probe, label, timeout = workload.deadlineMs) {
  const end = performance.now() + timeout;
  while (performance.now() < end) { const value = await probe(); if (value) return value; await sleep(20); }
  throw Object.assign(new Error(`deadline: ${label}`), {timedOut: true});
}

function proc(pid) {
  if (process.platform !== 'linux') return null;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, 'utf8').split(') ')[1].split(' ');
    const memory = fs.readFileSync(`/proc/${pid}/status`, 'utf8').match(/^VmRSS:\s+(\d+)/m);
    return {cpuTicks: Number(stat[11]) + Number(stat[12]), rssBytes: Number(memory?.[1] ?? 0) * 1024};
  } catch (error) { if (error.code === 'ENOENT' || error.code === 'ESRCH') return null; throw error; }
}

function freeDisk() {
  const stat = fs.statfsSync('.');
  assert(stat.bavail * stat.bsize >= 10 * 1024 ** 3, 'benchmark requires 10 GiB free disk');
}

async function trial(root, cell, profile = false) {
  freeDisk();
  fs.mkdirSync(root, {recursive: true});
  const token = randomUUID(), children = [], logs = [], resourceSamples = [], samples = [];
  const result = {cell, passed: false, offered: 0, started: 0, finished: 0, failed: 0, rejected: 0, timedOut: 0, generatorDeclined: 0, samples, resourceSamples};
  let timer, url;
  function launch(script, args, name) {
    const log = fs.createWriteStream(path.join(root, `${name}.log`)); logs.push(log);
    const flags = profile ? ['--cpu-prof', `--cpu-prof-dir=${root}`] : [];
    const child = spawn(process.execPath, [...flags, ...(profile ? ['tools/profile-entry.mjs'] : []), script, ...args], {env: {...process.env, MAPREDUCE_TOKEN: token}, stdio: ['ignore', 'pipe', 'pipe']});
    child.stdout.pipe(log, {end: false}); child.stderr.pipe(log, {end: false});
    children.push({child, name}); return child;
  }
  async function stop(child) {
    if (child.exitCode !== null || child.signalCode !== null) return;
    await new Promise((resolve, reject) => {
      const bound = setTimeout(() => reject(new Error(`owned PID ${child.pid} did not exit`)), 5000);
      child.once('close', () => { clearTimeout(bound); resolve(); });
      child.kill(profile ? 'SIGINT' : 'SIGKILL');
    });
  }
  const call = (route, method, data) => rpc(`${url}${route}`, token, method, data);
  const keys = dataset(cell.records, cell.skew), expected = oracle(keys);
  result.inputBytes = Buffer.byteLength(JSON.stringify(keys));
  result.inputSha256 = createHash('sha256').update(JSON.stringify(keys)).digest('hex');
  result.expectedSha256 = createHash('sha256').update(JSON.stringify(expected)).digest('hex');
  let victim;
  try {
    const coordinator = launch('src/coordinator.mjs', ['--data', path.join(root, 'coordinator'), '--port', '0', '--lease', String(workload.leaseMs)], 'coordinator');
    const ready = await new Promise((resolve, reject) => {
      let text = '';
      const bound = setTimeout(() => reject(new Error('coordinator readiness deadline')), 5000);
      coordinator.once('error', reject);
      coordinator.once('exit', code => reject(new Error(`coordinator exited ${code}`)));
      coordinator.stdout.on('data', chunk => { text += chunk; try { const state = JSON.parse(text.split('\n')[0]); if (state.ready) { clearTimeout(bound); resolve(state); } } catch {} });
    });
    url = `http://127.0.0.1:${ready.port}`;
    assert.equal((await call('/health')).pid, coordinator.pid);
    for (let i = 0; i < cell.workers; i++) {
      const worker = launch('src/worker.mjs', ['--url', url, '--id', `bench-${i}`, '--data', path.join(root, `worker-${i}`)], `worker-${i}`);
      if (!i) victim = worker;
    }
    const capture = () => resourceSamples.push({atMs: performance.now(), generator: proc(process.pid), processes: children.map(({child, name}) => ({name, pid: child.pid, ...proc(child.pid)}))});
    capture(); timer = setInterval(capture, 100);
    const runJob = async (scheduled, index, measured = true) => {
      const sample = {index, scheduledMs: scheduled, startedMs: performance.now()};
      if (measured) { result.started++; samples.push(sample); }
      try {
        const {id} = await call('/jobs', 'POST', {module: cell.fault ? 'delayed-count' : 'wordcount', keys, splits: workload.splits, reducers: workload.reducers});
        sample.id = id;
        if (cell.fault) {
          const assigned = await until(async () => (await call(`/jobs/${id}`)).tasks.find(t => t.worker === 'bench-0' && t.status === 'running'), 'victim assignment');
          sample.fault = {atMs: performance.now(), pid: victim.pid, token: assigned.token};
          await stop(victim);
        }
        const job = await until(async () => {
          const state = await call(`/jobs/${id}`);
          if (state.status === 'failed') throw new Error(state.error ?? 'job failed');
          return state.status === 'done' && state;
        }, id);
        assert.deepEqual(job.output, expected);
        const durable = read(path.join(root, 'coordinator', 'results', `${id}.json`));
        assert.deepEqual(durable, expected);
        sample.finishedMs = performance.now(); sample.latencyMs = sample.finishedMs - scheduled;
        sample.dispatchLagMs = sample.startedMs - scheduled;
        sample.shuffleBytes = job.maps.reduce((bytes, task) => bytes + fs.statSync(path.join(root, 'coordinator', 'attempts', `${task.accepted}.json`)).size, 0);
        sample.attempts = [...job.maps, ...job.tasks].reduce((n, task) => n + task.attempts, 0);
        sample.publications = job.events.filter(event => event.type === 'published').length;
        assert.equal(sample.publications, workload.splits + workload.reducers);
        sample.oracle = true; sample.outcome = 'finished';
        if (cell.fault) { assert(sample.attempts > sample.publications); sample.recoveryMs = sample.finishedMs - sample.fault.atMs; }
        if (measured) result.finished++;
      } catch (error) {
        sample.finishedMs = performance.now(); sample.error = error.stack;
        sample.outcome = error.timedOut || error.name === 'TimeoutError' ? 'timedOut' : error.status >= 400 && error.status < 500 ? 'rejected' : 'failed';
        if (measured) result[sample.outcome]++;
        else throw error;
      }
    };
    if (!cell.fault) await runJob(performance.now(), -1, false);
    result.startMs = performance.now();
    if (cell.rate) {
      const pending = new Set();
      const count = Math.ceil(cell.rate * cell.seconds);
      for (let i = 0; i < count; i++) {
        const scheduled = result.startMs + i * 1000 / cell.rate;
        await sleep(Math.max(0, scheduled - performance.now())); result.offered++;
        if (pending.size >= workload.outstandingLimit) { result.generatorDeclined++; samples.push({index: i, scheduledMs: scheduled, outcome: 'generatorDeclined'}); continue; }
        const promise = runJob(scheduled, i).finally(() => pending.delete(promise)); pending.add(promise);
      }
      result.offeringEndMs = result.startMs + cell.seconds * 1000;
      await Promise.all(pending);
    } else {
      for (let i = 0; i < cell.jobs; i++) { result.offered++; await runJob(performance.now(), i); }
      result.offeringEndMs = performance.now();
    }
    result.endMs = performance.now(); capture();
    result.latencyMs = distribution(samples.filter(s => s.oracle).map(s => s.latencyMs));
    result.dispatchLagMs = distribution(samples.filter(s => s.startedMs !== undefined).map(s => s.dispatchLagMs ?? s.startedMs - s.scheduledMs));
    result.elapsedSeconds = (result.endMs - result.startMs) / 1000;
    result.jobsPerSecond = result.finished / result.elapsedSeconds;
    result.usefulInputBytesPerSecond = result.finished * result.inputBytes / result.elapsedSeconds;
    assert.equal(result.offered, result.finished + result.failed + result.rejected + result.timedOut + result.generatorDeclined);
    result.passed = result.finished > 0 && !result.failed && !result.rejected && !result.timedOut && !result.generatorDeclined;
  } catch (error) { result.error = error.stack; }
  finally {
    clearInterval(timer);
    result.cleanupErrors = [];
    for (const {child} of children) try { await stop(child); } catch (error) { result.cleanupErrors.push(error.message); }
    for (const log of logs) await new Promise(resolve => log.end(resolve));
    if (profile && fs.readdirSync(root).filter(file => file.endsWith('.cpuprofile')).length !== children.length) result.cleanupErrors.push('missing child CPU profiles');
    result.cleanup = children.every(({child}) => child.exitCode !== null || child.signalCode !== null) && !result.cleanupErrors.length;
    result.passed &&= result.cleanup;
    atomic(path.join(root, 'result.json'), result);
  }
  return result;
}

async function main() {
  const mode = process.argv[2] ?? 'smoke';
  assert(['smoke', 'benchmark', 'profile'].includes(mode), 'mode must be smoke, benchmark or profile');
  assert(mode === 'smoke' || process.platform === 'linux', 'measurement requires isolated Linux runner and /proc resource evidence');
  const root = path.resolve('.runs', `benchmark-${mode}-${new Date().toISOString().replaceAll(':', '-')}-${randomUUID().slice(0, 8)}`);
  fs.mkdirSync(root, {recursive: true});
  const source = execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim();
  const summary = {passed: false, mode, source, dirty: !!execFileSync('git', ['status', '--porcelain'], {encoding: 'utf8'}).trim(), environment: {node: process.version, platform: process.platform, release: os.release(), cpus: os.cpus(), memoryBytes: os.totalmem(), clockTicksPerSecond: process.platform === 'linux' ? Number(execFileSync('getconf', ['CLK_TCK'], {encoding: 'utf8'})) : null}, workload, policy: 'Fresh coordinator/store per cell; one untimed warm job; six sequential measured jobs; scheduled arrivals for rates; Linux /proc samples every 100ms. Profiles excluded from comparison. Latency includes submit, execution, durable result and up to 20ms observation delay.', trials: []};
  const run = async cell => {
    const result = await trial(path.join(root, `trial-${summary.trials.length}`), cell, mode === 'profile');
    summary.trials.push(result); atomic(path.join(root, 'summary.json'), summary);
    console.log(JSON.stringify({cell, passed: result.passed, finished: result.finished, evidence: root}));
    if (!result.cleanup || result.error) throw new Error(result.error ?? 'cleanup failed');
    return result;
  };
  try {
    if (mode !== 'benchmark') await run({records: mode === 'smoke' ? 16 : 2048, skew: .9, workers: 2, jobs: mode === 'smoke' ? 1 : workload.jobs});
    else {
      for (let repeat = 0; repeat < workload.repeats; repeat++) for (const records of workload.records) for (const skew of workload.skews) {
        const order = repeat % 2 ? [...workload.workers].reverse() : workload.workers;
        for (const workers of order) await run({records, skew, workers, jobs: workload.jobs, repeat});
      }
      const sweeps = [];
      for (const rate of workload.rates) sweeps.push(await run({records: 256, skew: .9, workers: 4, rate, seconds: workload.sweepSeconds}));
      const stable = sweeps.filter(s => s.passed && s.dispatchLagMs.p99 < 100 && s.latencyMs.p99 < 5000);
      if (stable.length) await run({records: 256, skew: .9, workers: 4, rate: stable.at(-1).cell.rate, seconds: workload.stabilitySeconds, stability: true});
      else summary.stabilityBlocked = 'No offered rate met zero-loss, dispatch-p99 <100ms and job-p99 <5s envelope; retain sweep evidence and expand lower rates.';
      await run({records: 16, skew: .9, workers: 2, jobs: 1, fault: 'worker-loss'});
    }
    summary.passed = summary.trials.every(t => t.passed) && !summary.stabilityBlocked;
  } catch (error) { summary.error = error.stack; }
  finally { atomic(path.join(root, 'summary.json'), summary); }
  if (!summary.passed) process.exitCode = 1;
  console.log(JSON.stringify({passed: summary.passed, evidence: path.join(root, 'summary.json')}));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();

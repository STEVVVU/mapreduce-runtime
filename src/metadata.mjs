import fs from 'node:fs';
import path from 'node:path';
import {ensure, read} from './common.mjs';

const idPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export function metadataFile(root, id) {
  ensure(typeof id === 'string' && idPattern.test(id), 'invalid durable job ID');
  return path.join(root, 'jobs', `${id}.json`);
}

export function readMetadata(root) {
  const legacy = path.join(root, 'state.json');
  const state = fs.existsSync(legacy) ? read(legacy) : {jobs: {}};
  ensure(state && state.jobs && typeof state.jobs === 'object' && !Array.isArray(state.jobs), 'invalid legacy job registry');
  for (const [id, job] of Object.entries(state.jobs)) {
    metadataFile(root, id);
    ensure(job && job.id === id, 'legacy job ID mismatch');
  }
  const directory = path.join(root, 'jobs');
  if (fs.existsSync(directory)) for (const file of fs.readdirSync(directory)) {
    if (!file.endsWith('.json')) continue;
    const id = file.slice(0, -5);
    metadataFile(root, id);
    const job = read(path.join(directory, file));
    ensure(job && job.id === id, 'durable job ID mismatch');
    state.jobs[id] = job;
  }
  return state;
}

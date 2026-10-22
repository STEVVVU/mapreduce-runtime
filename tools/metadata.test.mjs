import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {atomic} from '../src/common.mjs';
import {metadataFile, readMetadata} from '../src/metadata.mjs';

test('authoritative job snapshots survive partial legacy import without stale resurrection', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rillmap-metadata-'));
  const first = randomUUID(), second = randomUUID();
  try {
    atomic(path.join(root, 'state.json'), {jobs: {[first]: {id: first, attempt: 1}, [second]: {id: second, attempt: 1}}});
    atomic(metadataFile(root, first), {id: first, attempt: 2});
    fs.writeFileSync(`${metadataFile(root, second)}.orphan.tmp`, '{partial');
    assert.equal(readMetadata(root).jobs[first].attempt, 2);
    assert.equal(readMetadata(root).jobs[second].attempt, 1);
    atomic(metadataFile(root, second), {id: second, attempt: 3});
    assert.equal(readMetadata(root).jobs[second].attempt, 3);
    assert.equal(JSON.parse(fs.readFileSync(path.join(root, 'state.json'))).jobs[first].attempt, 1);
  } finally { fs.rmSync(root, {recursive: true}); }
});

test('metadata fails closed on unsafe names, mismatched IDs and corrupt accepted snapshots', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'rillmap-metadata-')), id = randomUUID();
  try {
    assert.throws(() => metadataFile(root, '../escape'), /invalid durable job ID/);
    atomic(metadataFile(root, id), {id: randomUUID()});
    assert.throws(() => readMetadata(root), /ID mismatch/);
    fs.writeFileSync(metadataFile(root, id), '{partial');
    assert.throws(() => readMetadata(root), SyntaxError);
  } finally { fs.rmSync(root, {recursive: true}); }
});

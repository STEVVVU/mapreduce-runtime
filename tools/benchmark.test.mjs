import {test} from 'node:test';
import assert from 'node:assert/strict';
import {dataset, oracle, distribution} from './benchmark.mjs';

test('frozen data is deterministic, changes skew and preserves all tokens', () => {
  assert.deepEqual(dataset(16, .9), dataset(16, .9));
  assert.notDeepEqual(dataset(16, .9), dataset(16, 0));
  assert.equal(oracle(dataset(16, .9)).reduce((sum, [, count]) => sum + count, 0), 16 * 16);
});

test('nearest-rank tails preserve failures as separate counts', () => {
  assert.deepEqual(distribution([5, 1, 2, 4, 3]), {count: 5, min: 1, p50: 3, p95: 5, p99: 5, max: 5});
  assert.equal(distribution([]), null);
});

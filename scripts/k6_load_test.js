// k6 is a JavaScript load testing tool. Each "virtual user" (VU) runs the
// default exported function in a loop for the duration of the test.
// Install: brew install k6
// Run:     k6 run scripts/k6_load_test.js

import http from 'k6/http';
import { check, sleep } from 'k6';

// BASE_URL can be overridden at runtime:
//   k6 run -e BASE_URL=http://staging.example.com scripts/k6_load_test.js
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// options controls how the test runs.
// stages ramps VUs up and down — this is more realistic than hitting the
// server with full concurrency from the first second. It also lets you see
// exactly which VU count causes latency to degrade.
export const options = {
  stages: [
    { duration: '30s', target: 10 },  // ramp from 0 → 10 VUs over 30s
    { duration: '1m',  target: 10 },  // hold at 10 VUs for 1 minute
    { duration: '30s', target: 30 },  // ramp up to 30 VUs — watch metrics here
    { duration: '1m',  target: 30 },  // hold at 30 VUs
    { duration: '30s', target: 0  },  // ramp down
  ],

  // thresholds are pass/fail criteria for the whole test.
  // If any threshold is breached, k6 exits with a non-zero code — useful for CI gates.
  // These measure the synchronous HTTP layer, not the async saga.
  thresholds: {
    http_req_failed:   ['rate<0.01'],   // less than 1% of requests error
    http_req_duration: ['p(95)<3000'],  // 95% of HTTP responses under 3s
  },
};

// setup() runs exactly once before any VU starts.
// Its return value is passed as the first argument to every default() call.
// Use it for expensive one-time work — here, creating the test user and
// fetching a token so every VU shares the same auth without each one
// hammering the user endpoint.
export function setup() {
  const headers = { 'Content-Type': 'application/json' };

  http.post(`${BASE_URL}/v1/users`, JSON.stringify({
    name: 'k6 user',
    email: 'k6@shippy.test',
    company: 'k6',
    password: 'secret123',
  }), { headers });

  const authRes = http.post(`${BASE_URL}/auth`, JSON.stringify({
    email: 'k6@shippy.test',
    password: 'secret123',
  }), { headers });

  const token = authRes.json('token');
  if (!token) {
    throw new Error(`failed to get token — is the stack running? response: ${authRes.body}`);
  }

  return { token };
}

// default() is the VU loop — every VU runs this repeatedly for the test duration.
// __VU  = which VU this is (1-based)
// __ITER = how many times this VU has run the loop (0-based)
// Together they give each iteration a unique identifier without needing uuid.
export default function (data) {
  const headers = {
    'Content-Type': 'application/json',
    'x-token': data.token,
  };

  // --- create consignment ---
  const createRes = http.post(
    `${BASE_URL}/v1/consignments`,
    JSON.stringify({
      description: `k6 shipment ${__VU}-${__ITER}`,
      weight: 100,  // keep weight small so vessel capacity doesn't exhaust
      containers: [{ customer_id: `cust-${__VU}`, user_id: `user-${__VU}` }],
    }),
    { headers },
  );

  // check() records a pass/fail but does NOT stop the test on failure.
  // Failed checks show up in the summary at the end and increment
  // the checks_failed counter. Compare to thresholds which fail the whole run.
  const created = check(createRes, {
    'create: status 201': r => r.status === 201,
    'create: has id':     r => r.json('id') !== null,
  });

  if (!created) {
    // Skip confirm if create failed — no point hammering a broken endpoint.
    sleep(1);
    return;
  }

  const id = createRes.json('id');

  // --- confirm consignment ---
  // This kicks off the async SAGA. The HTTP response comes back immediately
  // once the outbox event is written — the actual saga completion happens
  // in the background and is measured by Prometheus, not k6.
  const confirmRes = http.post(
    `${BASE_URL}/v1/consignments/confirm/${id}`,
    null,
    { headers },
  );

  check(confirmRes, {
    'confirm: status 202': r => r.status === 202,
  });

  sleep(1);
}

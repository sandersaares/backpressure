import http from 'k6/http';

export const options = {
  // Avoid the extreme hammering if the server starts rejecting us.
  minIterationDuration: '10ms',

  scenarios: {
    contacts: {
      executor: 'constant-arrival-rate',
      duration: '60s',
      rate: 110,
      timeUnit: '1s',
      preAllocatedVUs: 4,
      maxVUs: 100,
    },
  },
};

export default function () {
  let params = {
    // Reasonable SLA - anything longer than this is failed request.
    timeout: "500ms",
  };
  http.get('http://10.0.0.8:3000/', params);
}

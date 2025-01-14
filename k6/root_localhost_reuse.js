import http from 'k6/http';

export const options = {
  // Avoid the extreme hammering if the server starts rejecting us.
  minIterationDuration: '10ms',
};

export default function () {
  let params = {
    // Reasonable SLA - anything longer than this is failed request.
    timeout: "500ms",
  };
  http.get('http://127.0.0.1:3000/', params);
}

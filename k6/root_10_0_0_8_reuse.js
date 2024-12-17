import http from 'k6/http';

export const options = {
  // Avoid the extreme hammering if the server starts rejecting us.
  minIterationDuration: '10ms',
};

export default function () {
  let params = {
    timeout: "1s",
  };
  http.get('http://10.0.0.8:3000/', params);
}

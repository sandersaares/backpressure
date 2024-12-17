import http from 'k6/http';

export const options = {
  timeout: "5s",
};

export default function () {
  http.get('http://10.0.0.8:3000/');
}

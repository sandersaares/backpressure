import http from 'k6/http';

export const options = {
};

export default function () {
  http.get('http://10.0.0.8:3000/');
}

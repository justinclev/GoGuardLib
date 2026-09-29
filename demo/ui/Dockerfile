# syntax=docker/dockerfile:1
FROM node:22-alpine AS build
WORKDIR /app
# A network that re-signs HTTPS with a company root certificate: put that certificate
# in demo/certs/ (see the README there). With none present this changes nothing.
COPY --from=certs . /extra-certs/
ARG NPM_CONFIG_REGISTRY
COPY package*.json ./
RUN cat /extra-certs/*.crt > /extra-ca.pem 2>/dev/null || true; \
    if [ -s /extra-ca.pem ]; then export NODE_EXTRA_CA_CERTS=/extra-ca.pem; fi; \
    npm ci --no-audit --no-fund ${NPM_CONFIG_REGISTRY:+--registry=$NPM_CONFIG_REGISTRY}
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist/goguard-demo/browser /usr/share/nginx/html
EXPOSE 80

# Corporate root certificates for the demo build

If `docker compose up --build` fails with `x509: certificate signed by unknown authority`,
your network re-signs HTTPS traffic with a company root certificate that the host trusts
but the build containers do not.

Put that certificate here as a PEM file ending in `.crt` (for example `company-root.crt`),
then rebuild with `docker compose build --no-cache`. Files here are ignored by git: never
commit a certificate.

Only the root (or intermediate) **CA** certificate is needed. Never a private key.

Getting it:

- **Windows:** run `certmgr.msc`, open *Trusted Root Certification Authorities*, find your
  company's root, right-click, *All Tasks*, *Export*, choose *Base-64 encoded X.509 (.CER)*,
  and rename the file to `.crt`.
- **macOS:** Keychain Access, find the root, export as `.cer`, then
  `openssl x509 -inform der -in root.cer -out company-root.crt`.
- **Any system:** `openssl s_client -showcerts -connect proxy.golang.org:443 </dev/null`
  and save the *last* certificate block (between `BEGIN CERTIFICATE` and
  `END CERTIFICATE`) to a `.crt` file.

If public package registries are blocked outright, point the build at your internal mirrors:

```sh
GOPROXY=https://artifactory.example/api/go/go NPM_CONFIG_REGISTRY=https://artifactory.example/api/npm/npm/ \
  docker compose build
```

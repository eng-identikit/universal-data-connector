#!/usr/bin/env node
/**
 * Generates the OPC UA client identity (self-signed certificate + private key) to share
 * between replicas, e.g. as a Kubernetes Secret:
 *
 *   node src/tools/opcua-certificate.js <out-dir> [--uri urn:udc:Universal-Data-Connector] [--days 1825]
 *   kubectl create secret generic udc-opcua \
 *     --from-file=certificate.pem=<out-dir>/certificate.pem \
 *     --from-file=private_key.pem=<out-dir>/private_key.pem
 *
 * Then set backend.opcua.existingSecret (Helm) or OPCUA_CERTIFICATE_FILE,
 * OPCUA_PRIVATE_KEY_FILE and OPCUA_APPLICATION_URI (the same --uri).
 */
const fs = require('fs');
const os = require('os');
const path = require('path');
const { OPCUACertificateManager } = require('node-opcua');

function parseArgs(argv) {
  const args = { out: null, uri: 'urn:udc:Universal-Data-Connector', days: 365 * 5, subject: '/CN=Universal Data Connector/O=UDC' };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--uri') args.uri = argv[++i];
    else if (a === '--days') args.days = parseInt(argv[++i], 10);
    else if (a === '--subject') args.subject = argv[++i];
    else if (a === '-h' || a === '--help') args.help = true;
    else if (!args.out) args.out = a;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.out || !args.uri || !(args.days > 0)) {
    console.log('Usage: node src/tools/opcua-certificate.js <out-dir> [--uri <applicationUri>] [--days <n>] [--subject </CN=...>]');
    process.exit(args.help ? 0 : 1);
  }
  const out = path.resolve(args.out);
  fs.mkdirSync(out, { recursive: true });
  const certificateFile = path.join(out, 'certificate.pem');
  const keyFile = path.join(out, 'private_key.pem');
  if (fs.existsSync(certificateFile) || fs.existsSync(keyFile)) {
    console.error(`${out} already holds certificate.pem or private_key.pem: not overwritten`);
    process.exit(1);
  }

  const pki = fs.mkdtempSync(path.join(os.tmpdir(), 'udc-pki-'));
  try {
    const manager = new OPCUACertificateManager({ rootFolder: pki, name: '' });
    await manager.initialize();
    await manager.createSelfSignedCertificate({
      subject: args.subject,
      startDate: new Date(),
      dns: [],
      validity: args.days,
      applicationUri: args.uri,
      outputFile: certificateFile
    });
    fs.copyFileSync(manager.privateKey, keyFile);
    fs.chmodSync(keyFile, 0o600);
    await manager.dispose?.();
  } finally {
    fs.rmSync(pki, { recursive: true, force: true });
  }
  console.log(`Certificate: ${certificateFile}`);
  console.log(`Private key: ${keyFile}`);
  console.log(`Application URI: ${args.uri}  (set it as OPCUA_APPLICATION_URI / backend.opcua.applicationUri)`);
}

main().catch((error) => {
  console.error(`Failed: ${error.message}`);
  process.exit(1);
});

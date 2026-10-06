#!/usr/bin/env node

/**
 * Universal Data Connector - Test Runner
 * Avvia automaticamente il server e poi esegue tutti i test
 * 
 * Usage:
 *   node tests/run-tests.js             # Avvia server + tutti i test
 *   node tests/run-tests.js --no-server # Solo test (server già in esecuzione)
 *   node tests/run-tests.js --host=myhost --port=3000
 *
 * Prerequisiti:
 *   npm install
 */

const { spawn, exec } = require('child_process');
const path = require('path');
const axios = require('axios').default;

const TESTS = [
  { name: 'Drivers',       script: 'tests/test-drivers.js',        requiresServer: false },
  { name: 'Connectors',    script: 'tests/test-connectors.js',     requiresServer: false },
  { name: 'Fieldbus',      script: 'tests/test-fieldbus.js',       requiresServer: false },
  { name: 'Mapping',       script: 'tests/test-mapping.js',        requiresServer: false },
  { name: 'Storage',       script: 'tests/test-storage.js',        requiresServer: false },
  { name: 'UDC Full Suite',script: 'tests/test-udc.js',            requiresServer: true  },
  { name: 'Dynamic Config',script: 'tests/test-dynamic-config.js', requiresServer: true  },
];

async function waitForServer(baseUrl, maxWait = 15000) {
  const interval = 500;
  const maxAttempts = maxWait / interval;
  
  for (let i = 0; i < maxAttempts; i++) {
    try {
      await axios.get(`${baseUrl}/api/status`);
      return true;
    } catch {
      await new Promise(resolve => setTimeout(resolve, interval));
    }
  }
  
  return false;
}

async function runScript(script) {
  return new Promise((resolve) => {
    const proc = spawn(process.execPath, [script], {
      cwd: path.resolve(__dirname, '..'),
      stdio: 'inherit'
    });
    
    proc.on('exit', code => resolve(code === 0));
  });
}

async function main() {
  const args = process.argv.slice(2);
  const noServer = args.includes('--no-server');
  const hostArg  = args.find(a => a.startsWith('--host='));
  const portArg  = args.find(a => a.startsWith('--port='));
  
  const host    = hostArg ? hostArg.split('=')[1] : 'localhost';
  const port    = portArg ? portArg.split('=')[1] : '3000';
  const baseUrl = `http://${host}:${port}`;
  
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║         Universal Data Connector - Test Runner                ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log(`\nTarget server: ${baseUrl}`);
  console.log(`Start time:    ${new Date().toISOString()}\n`);
  
  let serverProcess = null;
  
  // ---- Check or start server ----
  let serverAvailable = false;
  
  // Check if already running
  try {
    await axios.get(`${baseUrl}/api/status`);
    serverAvailable = true;
    console.log('✓ Server already running\n');
  } catch {
    if (noServer) {
      console.warn('⚠ Server not running and --no-server flag set. Skipping server-dependent tests.\n');
    } else {
      console.log('Starting UDC server...');
      serverProcess = spawn(process.execPath, ['src/server.js'], {
        cwd: path.resolve(__dirname, '..'),
        stdio: 'inherit',
        detached: false
      });
      
      serverAvailable = await waitForServer(baseUrl);
      
      if (serverAvailable) {
        console.log('✓ Server started\n');
      } else {
        console.warn('⚠ Server did not start in time. Skipping server-dependent tests.\n');
      }
    }
  }
  
  // ---- Run tests ----
  const results = {};
  const startTime = Date.now();
  
  for (const test of TESTS) {
    if (test.requiresServer && !serverAvailable) {
      console.log(`\n⊘ ${test.name} (skipped - server unavailable)`);
      results[test.name] = null;
      continue;
    }
    
    console.log(`\n▶ Running: ${test.name}`);
    results[test.name] = await runScript(test.script);
  }
  
  // ---- Stop server if we started it ----
  if (serverProcess) {
    serverProcess.kill('SIGTERM');
    console.log('\n✓ Server stopped');
  }
  
  // ---- Print summary ----
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║                      OVERALL SUMMARY                           ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log(`\nDuration: ${duration}s\n`);
  
  let passed = 0, failed = 0, skipped = 0;
  
  for (const [name, ok] of Object.entries(results)) {
    if (ok === null) {
      console.log(`  ⊘ ${name.padEnd(20)} SKIPPED`);
      skipped++;
    } else if (ok) {
      console.log(`  ✓ ${name.padEnd(20)} PASSED`);
      passed++;
    } else {
      console.log(`  ✗ ${name.padEnd(20)} FAILED`);
      failed++;
    }
  }
  
  console.log(`\nTotal: ${passed + failed + skipped}, ✓ ${passed}, ✗ ${failed}, ⊘ ${skipped}`);
  
  if (failed > 0) {
    console.log('\n❌ SOME TEST SUITES FAILED');
    process.exit(1);
  } else {
    console.log('\n✓ ALL TEST SUITES PASSED! 🎉');
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});

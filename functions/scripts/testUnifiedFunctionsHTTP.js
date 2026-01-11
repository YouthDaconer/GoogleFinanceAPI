/**
 * HTTP Test Script for Unified Cloud Functions
 * 
 * Tests actual HTTP calls to the emulator
 * Run while emulator is running: node scripts/testUnifiedFunctionsHTTP.js
 */

const http = require('http');

const BASE_URL = 'http://127.0.0.1:5001/portafolio-inversiones/us-central1';

/**
 * Makes a POST request to a Cloud Function
 */
function callFunction(functionName, data) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({ data });
    
    const options = {
      hostname: '127.0.0.1',
      port: 5001,
      path: `/portafolio-inversiones/us-central1/${functionName}`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(body);
          resolve({ status: res.statusCode, data: parsed });
        } catch (e) {
          resolve({ status: res.statusCode, data: body });
        }
      });
    });

    req.on('error', (e) => reject(e));
    req.write(postData);
    req.end();
  });
}

/**
 * Test results tracker
 */
const results = {
  passed: 0,
  failed: 0,
  tests: [],
};

function logResult(testName, passed, details = '') {
  const status = passed ? '✅' : '❌';
  console.log(`   ${status} ${testName}${details ? `: ${details}` : ''}`);
  results.tests.push({ testName, passed, details });
  if (passed) results.passed++;
  else results.failed++;
}

/**
 * Run all tests
 */
async function runTests() {
  console.log('\n' + '='.repeat(60));
  console.log('🧪 HTTP Tests for Unified Cloud Functions');
  console.log('='.repeat(60));

  // Test 1: portfolioOperations - sin autenticación
  console.log('\n📦 Testing portfolioOperations...');
  try {
    const result = await callFunction('portfolioOperations', {
      action: 'createAsset',
      payload: { name: 'AAPL' },
    });
    
    // Debe retornar error de autenticación
    const isAuthError = result.data?.error?.status === 'UNAUTHENTICATED' ||
                        result.data?.error?.message?.includes('Autenticación') ||
                        result.data?.error?.message?.includes('autenticación');
    
    if (isAuthError) {
      logResult('portfolioOperations - auth check', true, 'UNAUTHENTICATED');
    } else {
      logResult('portfolioOperations - auth check', false, JSON.stringify(result.data).substring(0, 100));
    }
  } catch (e) {
    logResult('portfolioOperations - connection', false, e.message);
  }

  // Test 2: settingsOperations - sin autenticación
  console.log('\n⚙️ Testing settingsOperations...');
  try {
    const result = await callFunction('settingsOperations', {
      action: 'addCurrency',
      payload: { code: 'EUR' },
    });
    
    const isAuthError = result.data?.error?.status === 'UNAUTHENTICATED' ||
                        result.data?.error?.message?.includes('Autenticación');
    
    if (isAuthError) {
      logResult('settingsOperations - auth check', true, 'UNAUTHENTICATED');
    } else {
      logResult('settingsOperations - auth check', false, JSON.stringify(result.data).substring(0, 100));
    }
  } catch (e) {
    logResult('settingsOperations - connection', false, e.message);
  }

  // Test 3: accountOperations - sin autenticación
  console.log('\n🏦 Testing accountOperations...');
  try {
    const result = await callFunction('accountOperations', {
      action: 'addPortfolioAccount',
      payload: { name: 'Test' },
    });
    
    const isAuthError = result.data?.error?.status === 'UNAUTHENTICATED' ||
                        result.data?.error?.message?.includes('Autenticación');
    
    if (isAuthError) {
      logResult('accountOperations - auth check', true, 'UNAUTHENTICATED');
    } else {
      logResult('accountOperations - auth check', false, JSON.stringify(result.data).substring(0, 100));
    }
  } catch (e) {
    logResult('accountOperations - connection', false, e.message);
  }

  // Test 4: queryOperations - sin autenticación
  console.log('\n🔍 Testing queryOperations...');
  try {
    const result = await callFunction('queryOperations', {
      action: 'getHistoricalReturns',
      payload: { currency: 'USD' },
    });
    
    const isAuthError = result.data?.error?.status === 'UNAUTHENTICATED' ||
                        result.data?.error?.message?.includes('Autenticación');
    
    if (isAuthError) {
      logResult('queryOperations - auth check', true, 'UNAUTHENTICATED');
    } else {
      logResult('queryOperations - auth check', false, JSON.stringify(result.data).substring(0, 100));
    }
  } catch (e) {
    logResult('queryOperations - connection', false, e.message);
  }

  // Test 5: healthCheck - público
  console.log('\n❤️ Testing healthCheck (public)...');
  try {
    const result = await callFunction('healthCheck', {});
    
    if (result.status === 200 || result.data?.status === 'healthy' || result.data?.result?.status === 'healthy') {
      logResult('healthCheck - public endpoint', true, `status=${result.data?.result?.status || result.data?.status || 'OK'}`);
    } else {
      logResult('healthCheck - public endpoint', false, JSON.stringify(result).substring(0, 100));
    }
  } catch (e) {
    logResult('healthCheck - connection', false, e.message);
  }

  // Test 6: Legacy createAsset - comparar con portfolioOperations
  console.log('\n📜 Testing legacy functions...');
  try {
    const result = await callFunction('createAsset', {
      portfolioAccount: 'test',
      name: 'AAPL',
    });
    
    const isAuthError = result.data?.error?.status === 'UNAUTHENTICATED' ||
                        result.data?.error?.message?.includes('Autenticación');
    
    if (isAuthError) {
      logResult('createAsset (legacy) - same behavior', true, 'UNAUTHENTICATED');
    } else {
      logResult('createAsset (legacy) - same behavior', false, JSON.stringify(result.data).substring(0, 100));
    }
  } catch (e) {
    logResult('createAsset (legacy) - connection', false, e.message);
  }

  // Print summary
  console.log('\n' + '='.repeat(60));
  console.log(`📊 Test Summary: ${results.passed} passed, ${results.failed} failed`);
  console.log('='.repeat(60) + '\n');
  
  if (results.failed > 0) {
    console.log('Failed tests:');
    results.tests
      .filter(t => !t.passed)
      .forEach(t => console.log(`  ❌ ${t.testName}: ${t.details}`));
    console.log('');
  }

  process.exit(results.failed > 0 ? 1 : 0);
}

// Run tests
runTests().catch(e => {
  console.error('❌ Error running tests:', e.message);
  console.log('\n⚠️  Asegúrate de que el emulador esté corriendo:');
  console.log('   cd src/GoogleFinanceAPI && npx firebase emulators:start --only functions\n');
  process.exit(1);
});

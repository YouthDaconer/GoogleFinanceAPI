/**
 * STORY-036: Test Script for /risk-metrics endpoint
 * 
 * Ejecutar con: node tests/testRiskMetricsEndpoint.js
 * Requiere: Usuario válido en Firebase
 */

const BASE_URL = process.env.API_URL || 'http://localhost:5001/portafolio-inversiones/us-central1/app';

async function testEndpoint(name, url, expectedStatus = 200) {
  try {
    console.log(`\n🧪 Test: ${name}`);
    console.log(`   URL: ${url}`);
    
    const start = Date.now();
    const response = await fetch(url);
    const duration = Date.now() - start;
    
    const data = await response.json();
    
    const passed = response.status === expectedStatus;
    
    console.log(`   Status: ${response.status} (expected ${expectedStatus}) ${passed ? '✅' : '❌'}`);
    console.log(`   Duration: ${duration}ms`);
    
    if (data.success !== undefined) {
      console.log(`   Success: ${data.success}`);
    }
    if (data.error) {
      console.log(`   Error: ${data.error}`);
    }
    if (data.metadata) {
      console.log(`   Metadata:`, JSON.stringify(data.metadata, null, 2).substring(0, 200));
    }
    
    return { passed, data, status: response.status };
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { passed: false, error: error.message };
  }
}

async function runTests() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  STORY-036: Testing /risk-metrics Endpoint');
  console.log('═══════════════════════════════════════════════════');
  console.log(`Base URL: ${BASE_URL}`);
  
  const results = [];
  
  // Test 1: Missing userId should return 400
  results.push(await testEndpoint(
    'Missing userId returns 400',
    `${BASE_URL}/risk-metrics`,
    400
  ));
  
  // Test 2: Valid request with period=YTD
  // Nota: Reemplazar USER_ID con un ID real para tests en ambiente real
  const TEST_USER_ID = process.env.TEST_USER_ID || 'test-user-id';
  
  results.push(await testEndpoint(
    'Valid request with YTD period',
    `${BASE_URL}/risk-metrics?userId=${TEST_USER_ID}&period=YTD&currency=USD`,
    200
  ));
  
  // Test 3: Valid request with 1Y period
  results.push(await testEndpoint(
    'Valid request with 1Y period',
    `${BASE_URL}/risk-metrics?userId=${TEST_USER_ID}&period=1Y&currency=USD`,
    200
  ));
  
  // Test 4: Invalid period should return 400
  results.push(await testEndpoint(
    'Invalid period returns 400',
    `${BASE_URL}/risk-metrics?userId=${TEST_USER_ID}&period=INVALID`,
    400
  ));
  
  // Test 5: Multi-account aggregation
  results.push(await testEndpoint(
    'Multi-account request',
    `${BASE_URL}/risk-metrics?userId=${TEST_USER_ID}&period=YTD&accountIds=acc1,acc2`,
    200
  ));
  
  // Summary
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  Summary');
  console.log('═══════════════════════════════════════════════════');
  
  const passed = results.filter(r => r.passed).length;
  const total = results.length;
  
  console.log(`\n  Tests passed: ${passed}/${total}`);
  
  if (passed === total) {
    console.log('  ✅ All tests passed!');
  } else {
    console.log('  ❌ Some tests failed');
  }
}

runTests().catch(console.error);

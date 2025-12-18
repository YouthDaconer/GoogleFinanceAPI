/**
 * STORY-036: Test Script for /closed-positions endpoint
 * 
 * Ejecutar con: node tests/testClosedPositionsEndpoint.js
 * Requiere: Usuario válido en Firebase
 */

const BASE_URL = process.env.API_URL || 'http://localhost:5001/portafolio-inversiones/us-central1/app';

async function testEndpoint(name, url, expectedStatus = 200, validate = null) {
  try {
    console.log(`\n🧪 Test: ${name}`);
    console.log(`   URL: ${url}`);
    
    const start = Date.now();
    const response = await fetch(url);
    const duration = Date.now() - start;
    
    const data = await response.json();
    
    let passed = response.status === expectedStatus;
    let validationResult = null;
    
    if (passed && validate) {
      validationResult = validate(data);
      passed = passed && validationResult.passed;
    }
    
    console.log(`   Status: ${response.status} (expected ${expectedStatus}) ${response.status === expectedStatus ? '✅' : '❌'}`);
    console.log(`   Duration: ${duration}ms`);
    
    if (data.success !== undefined) {
      console.log(`   Success: ${data.success}`);
    }
    if (data.pagination) {
      console.log(`   Pagination: page ${data.pagination.page}/${data.pagination.totalPages}, items: ${data.pagination.totalItems}`);
    }
    if (data.summary) {
      console.log(`   Summary: ${data.summary.totalTrades} trades, P&L: ${data.summary.totalRealizedPnL}`);
    }
    if (validationResult && !validationResult.passed) {
      console.log(`   Validation failed: ${validationResult.message}`);
    }
    if (data.error) {
      console.log(`   Error: ${data.error}`);
    }
    
    return { passed, data, status: response.status };
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return { passed: false, error: error.message };
  }
}

async function runTests() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  STORY-036: Testing /closed-positions Endpoint');
  console.log('═══════════════════════════════════════════════════');
  console.log(`Base URL: ${BASE_URL}`);
  
  const results = [];
  const TEST_USER_ID = process.env.TEST_USER_ID || 'test-user-id';
  
  // Test 1: Missing userId should return 400
  results.push(await testEndpoint(
    'Missing userId returns 400',
    `${BASE_URL}/closed-positions`,
    400
  ));
  
  // Test 2: Valid request
  results.push(await testEndpoint(
    'Valid request returns positions',
    `${BASE_URL}/closed-positions?userId=${TEST_USER_ID}`,
    200,
    (data) => ({
      passed: Array.isArray(data.positions),
      message: 'positions should be an array'
    })
  ));
  
  // Test 3: Pagination
  results.push(await testEndpoint(
    'Pagination works',
    `${BASE_URL}/closed-positions?userId=${TEST_USER_ID}&page=1&pageSize=5`,
    200,
    (data) => ({
      passed: data.pagination && data.pagination.pageSize === 5,
      message: 'pageSize should be 5'
    })
  ));
  
  // Test 4: Sorting
  results.push(await testEndpoint(
    'Sorting by realizedPnL desc',
    `${BASE_URL}/closed-positions?userId=${TEST_USER_ID}&sortBy=realizedPnL&sortOrder=desc`,
    200
  ));
  
  // Test 5: Summary includes byAccount
  results.push(await testEndpoint(
    'Summary includes byAccount breakdown',
    `${BASE_URL}/closed-positions?userId=${TEST_USER_ID}`,
    200,
    (data) => ({
      passed: data.summary && Array.isArray(data.summary.byAccount),
      message: 'summary.byAccount should be an array'
    })
  ));
  
  // Test 6: Filter by date range
  const startDate = '2025-01-01';
  const endDate = '2025-12-31';
  results.push(await testEndpoint(
    'Filter by date range',
    `${BASE_URL}/closed-positions?userId=${TEST_USER_ID}&startDate=${startDate}&endDate=${endDate}`,
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

/**
 * STORY-036: Test Script for /news/batch endpoint
 * 
 * Ejecutar con: node tests/testNewsBatchEndpoint.js
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
    if (data.metadata) {
      console.log(`   Symbols requested: ${data.metadata.symbolsRequested}`);
      console.log(`   Symbols successful: ${data.metadata.symbolsSuccessful}`);
      console.log(`   Total news: ${data.metadata.totalNews}`);
    }
    if (data.newsBySymbol) {
      const symbols = Object.keys(data.newsBySymbol);
      console.log(`   Symbols returned: ${symbols.join(', ')}`);
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
  console.log('  STORY-036: Testing /news/batch Endpoint');
  console.log('═══════════════════════════════════════════════════');
  console.log(`Base URL: ${BASE_URL}`);
  
  const results = [];
  
  // Test 1: Missing symbols should return 400
  results.push(await testEndpoint(
    'Missing symbols returns 400',
    `${BASE_URL}/news/batch`,
    400
  ));
  
  // Test 2: Empty symbols should return 400
  results.push(await testEndpoint(
    'Empty symbols returns 400',
    `${BASE_URL}/news/batch?symbols=`,
    400
  ));
  
  // Test 3: Valid request with multiple symbols
  results.push(await testEndpoint(
    'Multiple symbols returns news',
    `${BASE_URL}/news/batch?symbols=AAPL,MSFT,GOOGL`,
    200,
    (data) => ({
      passed: data.success && data.newsBySymbol && Object.keys(data.newsBySymbol).length > 0,
      message: 'Should return newsBySymbol object'
    })
  ));
  
  // Test 4: Consolidated news is sorted by date
  results.push(await testEndpoint(
    'Consolidated news is returned',
    `${BASE_URL}/news/batch?symbols=AAPL,MSFT`,
    200,
    (data) => ({
      passed: data.success && Array.isArray(data.consolidated),
      message: 'Should return consolidated array'
    })
  ));
  
  // Test 5: Limit parameter works
  results.push(await testEndpoint(
    'Limit parameter respected',
    `${BASE_URL}/news/batch?symbols=AAPL&limit=2`,
    200,
    (data) => {
      if (!data.success || !data.newsBySymbol?.AAPL) {
        return { passed: true, message: 'No news to validate limit' };
      }
      return {
        passed: data.newsBySymbol.AAPL.news.length <= 2,
        message: `Expected max 2 news, got ${data.newsBySymbol.AAPL.news.length}`
      };
    }
  ));
  
  // Test 6: Max 10 symbols
  const manySymbols = 'AAPL,MSFT,GOOGL,AMZN,META,NVDA,TSLA,JPM,V,MA,DIS,NFLX';
  results.push(await testEndpoint(
    'Max 10 symbols enforced',
    `${BASE_URL}/news/batch?symbols=${manySymbols}`,
    200,
    (data) => ({
      passed: data.metadata && data.metadata.symbolsRequested <= 10,
      message: `Expected max 10 symbols, got ${data.metadata?.symbolsRequested}`
    })
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

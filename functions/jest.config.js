/**
 * Jest Configuration for GoogleFinanceAPI Backend
 * 
 * @module jest.config
 * @see docs/stories/53.story.md (SCALE-CORE-001)
 */

module.exports = {
  // Environment
  testEnvironment: 'node',
  
  // Test file patterns - automated tests in __tests__ directories
  testMatch: [
    '**/__tests__/**/*.test.js',
    '**/__tests__/**/*.spec.js',
  ],
  
  // Ignore the manual test files in /tests directory
  testPathIgnorePatterns: [
    '/node_modules/',
    '/tests/', // Manual test scripts - not automated
  ],
  
  // Coverage configuration
  collectCoverageFrom: [
    'utils/**/*.js',
    'services/**/*.js',
    '!**/node_modules/**',
    '!**/__tests__/**',
    '!**/tests/**',
  ],
  
  coverageThreshold: {
    global: {
      statements: 50,
      branches: 50,
      functions: 50,
      lines: 50,
    },
  },
  
  coverageReporters: ['text', 'json', 'html'],
  coverageDirectory: './coverage',
  
  // Timeouts
  testTimeout: 30000,
  
  // Clear mocks between tests
  clearMocks: true,
  
  // Verbose output
  verbose: true,
};

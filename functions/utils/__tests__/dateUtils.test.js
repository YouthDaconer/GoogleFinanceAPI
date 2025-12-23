/**
 * Tests para dateUtils.js
 * 
 * @module __tests__/utils/dateUtils.test
 * @see docs/stories/53.story.md (SCALE-CORE-001)
 */

const {
  TRADING_DAYS_PER_YEAR,
  VALID_PERIODS,
  getPeriodStartDate,
  formatDateToISO,
  getDateRangeForPeriod,
  getPeriodLabel,
  daysBetween,
  calendarToTradingDays,
  parseAsLocalDate,
  isValidPeriod,
} = require('../../utils/dateUtils');

// ============================================================================
// Tests
// ============================================================================

describe('dateUtils', () => {
  // ==========================================================================
  // Constants
  // ==========================================================================

  describe('constants', () => {
    it('should have correct TRADING_DAYS_PER_YEAR', () => {
      expect(TRADING_DAYS_PER_YEAR).toBe(252);
    });

    it('should have all VALID_PERIODS', () => {
      expect(VALID_PERIODS).toEqual(['YTD', '1M', '3M', '6M', '1Y', '2Y', 'ALL']);
    });
  });

  // ==========================================================================
  // getPeriodStartDate
  // ==========================================================================

  describe('getPeriodStartDate', () => {
    it('should return January 1st for YTD', () => {
      const result = getPeriodStartDate('YTD');
      const now = new Date();
      
      expect(result.getMonth()).toBe(0); // January
      expect(result.getDate()).toBe(1);
      expect(result.getFullYear()).toBe(now.getFullYear());
    });

    it('should return 1 month ago for 1M', () => {
      const result = getPeriodStartDate('1M');
      const now = new Date();
      const expectedMonth = now.getMonth() - 1;
      
      // Handle year boundary
      if (expectedMonth < 0) {
        expect(result.getMonth()).toBe(11); // December
        expect(result.getFullYear()).toBe(now.getFullYear() - 1);
      } else {
        expect(result.getMonth()).toBe(expectedMonth);
        expect(result.getFullYear()).toBe(now.getFullYear());
      }
    });

    it('should return 3 months ago for 3M', () => {
      const result = getPeriodStartDate('3M');
      const now = new Date();
      
      // Calculate expected date
      const expected = new Date(now.getFullYear(), now.getMonth() - 3, now.getDate());
      
      expect(result.getMonth()).toBe(expected.getMonth());
      expect(result.getFullYear()).toBe(expected.getFullYear());
    });

    it('should return 6 months ago for 6M', () => {
      const result = getPeriodStartDate('6M');
      const now = new Date();
      
      const expected = new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
      
      expect(result.getMonth()).toBe(expected.getMonth());
      expect(result.getFullYear()).toBe(expected.getFullYear());
    });

    it('should return 1 year ago for 1Y', () => {
      const result = getPeriodStartDate('1Y');
      const now = new Date();
      
      expect(result.getFullYear()).toBe(now.getFullYear() - 1);
      expect(result.getMonth()).toBe(now.getMonth());
    });

    it('should return 2 years ago for 2Y', () => {
      const result = getPeriodStartDate('2Y');
      const now = new Date();
      
      expect(result.getFullYear()).toBe(now.getFullYear() - 2);
      expect(result.getMonth()).toBe(now.getMonth());
    });

    it('should return year 2000 for ALL', () => {
      const result = getPeriodStartDate('ALL');
      
      expect(result.getFullYear()).toBe(2000);
      expect(result.getMonth()).toBe(0); // January
      expect(result.getDate()).toBe(1);
    });

    it('should default to YTD for invalid period', () => {
      const result = getPeriodStartDate('INVALID');
      const now = new Date();
      
      expect(result.getMonth()).toBe(0); // January (YTD)
      expect(result.getDate()).toBe(1);
      expect(result.getFullYear()).toBe(now.getFullYear());
    });
  });

  // ==========================================================================
  // formatDateToISO
  // ==========================================================================

  describe('formatDateToISO', () => {
    it('should format date as YYYY-MM-DD', () => {
      const date = new Date('2024-06-15T10:30:00');
      const result = formatDateToISO(date);
      
      expect(result).toBe('2024-06-15');
    });

    it('should handle single digit months and days', () => {
      const date = new Date('2024-01-05T10:30:00');
      const result = formatDateToISO(date);
      
      expect(result).toBe('2024-01-05');
    });

    it('should handle end of year dates', () => {
      // Use UTC date to avoid timezone issues
      const date = new Date(Date.UTC(2024, 11, 31, 12, 0, 0)); // December 31, 2024 12:00 UTC
      const result = formatDateToISO(date);
      
      expect(result).toBe('2024-12-31');
    });

    it('should handle start of year dates', () => {
      // Use UTC date to avoid timezone issues
      const date = new Date(Date.UTC(2024, 0, 1, 12, 0, 0)); // January 1, 2024 12:00 UTC
      const result = formatDateToISO(date);
      
      expect(result).toBe('2024-01-01');
    });
  });

  // ==========================================================================
  // getDateRangeForPeriod
  // ==========================================================================

  describe('getDateRangeForPeriod', () => {
    it('should return start and end dates', () => {
      const result = getDateRangeForPeriod('YTD');
      
      expect(result).toHaveProperty('start');
      expect(result).toHaveProperty('end');
      expect(result.start).toBeInstanceOf(Date);
      expect(result.end).toBeInstanceOf(Date);
    });

    it('should have start before end', () => {
      const result = getDateRangeForPeriod('1Y');
      
      expect(result.start.getTime()).toBeLessThan(result.end.getTime());
    });

    it('should have end as today', () => {
      const result = getDateRangeForPeriod('YTD');
      const now = new Date();
      
      // End should be today (within same day)
      expect(result.end.getDate()).toBe(now.getDate());
      expect(result.end.getMonth()).toBe(now.getMonth());
      expect(result.end.getFullYear()).toBe(now.getFullYear());
    });
  });

  // ==========================================================================
  // getPeriodLabel
  // ==========================================================================

  describe('getPeriodLabel', () => {
    describe('Spanish (default)', () => {
      it('should return Spanish label for YTD', () => {
        expect(getPeriodLabel('YTD')).toBe('Año a la fecha');
      });

      it('should return Spanish label for 1M', () => {
        expect(getPeriodLabel('1M')).toBe('Último mes');
      });

      it('should return Spanish label for 3M', () => {
        expect(getPeriodLabel('3M')).toBe('Últimos 3 meses');
      });

      it('should return Spanish label for 6M', () => {
        expect(getPeriodLabel('6M')).toBe('Últimos 6 meses');
      });

      it('should return Spanish label for 1Y', () => {
        expect(getPeriodLabel('1Y')).toBe('Último año');
      });

      it('should return Spanish label for 2Y', () => {
        expect(getPeriodLabel('2Y')).toBe('Últimos 2 años');
      });

      it('should return Spanish label for ALL', () => {
        expect(getPeriodLabel('ALL')).toBe('Todo el historial');
      });
    });

    describe('English', () => {
      it('should return English label for YTD when locale is en', () => {
        expect(getPeriodLabel('YTD', 'en')).toBe('Year to Date');
      });

      it('should return English label for 1M when locale is en-US', () => {
        expect(getPeriodLabel('1M', 'en-US')).toBe('Last Month');
      });

      it('should return English label for ALL when locale is en', () => {
        expect(getPeriodLabel('ALL', 'en')).toBe('All Time');
      });
    });

    it('should return period as fallback for invalid period', () => {
      expect(getPeriodLabel('INVALID')).toBe('INVALID');
    });
  });

  // ==========================================================================
  // daysBetween
  // ==========================================================================

  describe('daysBetween', () => {
    it('should return 0 for same date', () => {
      const date = new Date('2024-06-15');
      const result = daysBetween(date, date);
      
      expect(result).toBe(0);
    });

    it('should return 1 for consecutive days', () => {
      const start = new Date('2024-06-15');
      const end = new Date('2024-06-16');
      const result = daysBetween(start, end);
      
      expect(result).toBe(1);
    });

    it('should return 7 for one week', () => {
      const start = new Date('2024-06-01');
      const end = new Date('2024-06-08');
      const result = daysBetween(start, end);
      
      expect(result).toBe(7);
    });

    it('should return 30 for one month (approximately)', () => {
      const start = new Date('2024-06-01');
      const end = new Date('2024-07-01');
      const result = daysBetween(start, end);
      
      expect(result).toBe(30);
    });

    it('should return 365 for one year (non-leap)', () => {
      const start = new Date('2023-01-01');
      const end = new Date('2024-01-01');
      const result = daysBetween(start, end);
      
      expect(result).toBe(365);
    });

    it('should return 366 for one leap year', () => {
      const start = new Date('2024-01-01');
      const end = new Date('2025-01-01');
      const result = daysBetween(start, end);
      
      expect(result).toBe(366); // 2024 is a leap year
    });

    it('should return negative for reversed dates', () => {
      const start = new Date('2024-06-15');
      const end = new Date('2024-06-10');
      const result = daysBetween(start, end);
      
      expect(result).toBe(-5);
    });
  });

  // ==========================================================================
  // calendarToTradingDays
  // ==========================================================================

  describe('calendarToTradingDays', () => {
    it('should convert 7 calendar days to 5 trading days', () => {
      const result = calendarToTradingDays(7);
      expect(result).toBe(5);
    });

    it('should convert 365 calendar days to approximately 252 trading days', () => {
      const result = calendarToTradingDays(365);
      // 365 * (5/7) = 260.7, floor = 260
      expect(result).toBe(260);
    });

    it('should return 0 for 0 calendar days', () => {
      const result = calendarToTradingDays(0);
      expect(result).toBe(0);
    });

    it('should handle 1 calendar day', () => {
      const result = calendarToTradingDays(1);
      expect(result).toBe(0); // floor(1 * 5/7) = 0
    });

    it('should handle 2 calendar days', () => {
      const result = calendarToTradingDays(2);
      expect(result).toBe(1); // floor(2 * 5/7) = 1
    });
  });

  // ==========================================================================
  // parseAsLocalDate
  // ==========================================================================

  describe('parseAsLocalDate', () => {
    it('should parse ISO date string', () => {
      const result = parseAsLocalDate('2024-06-15');
      
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(5); // June (0-indexed)
      expect(result.getDate()).toBe(15);
    });

    it('should parse date with time', () => {
      const result = parseAsLocalDate('2024-06-15T10:30:00');
      
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(5);
      expect(result.getDate()).toBe(15);
    });

    it('should handle start of year', () => {
      const result = parseAsLocalDate('2024-01-01');
      
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(0);
      expect(result.getDate()).toBe(1);
    });

    it('should handle end of year', () => {
      const result = parseAsLocalDate('2024-12-31');
      
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(11);
      expect(result.getDate()).toBe(31);
    });
  });

  // ==========================================================================
  // isValidPeriod
  // ==========================================================================

  describe('isValidPeriod', () => {
    it('should return true for all valid periods', () => {
      expect(isValidPeriod('YTD')).toBe(true);
      expect(isValidPeriod('1M')).toBe(true);
      expect(isValidPeriod('3M')).toBe(true);
      expect(isValidPeriod('6M')).toBe(true);
      expect(isValidPeriod('1Y')).toBe(true);
      expect(isValidPeriod('2Y')).toBe(true);
      expect(isValidPeriod('ALL')).toBe(true);
    });

    it('should return false for invalid periods', () => {
      expect(isValidPeriod('INVALID')).toBe(false);
      expect(isValidPeriod('5Y')).toBe(false);
      expect(isValidPeriod('10Y')).toBe(false);
      expect(isValidPeriod('')).toBe(false);
      expect(isValidPeriod('ytd')).toBe(false); // Case sensitive
    });
  });
});

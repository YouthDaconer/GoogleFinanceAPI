const TimePeriod = {
  DAY: '1d',
  FIVE_DAYS: '5d',
  SEVEN_DAYS: '7d',
  ONE_MONTH: '1mo',
  THREE_MONTHS: '3mo',
  SIX_MONTHS: '6mo',
  YTD: 'YTD',
  YEAR: '1Y',
  FIVE_YEARS: '5Y',
  TEN_YEARS: '10Y',
  MAX: 'max'
};

const Interval = {
  ONE_MINUTE: '1m',
  FIVE_MINUTES: '5m',
  FIFTEEN_MINUTES: '15m',
  THIRTY_MINUTES: '30m',
  ONE_HOUR: '1h',
  DAILY: '1d',
  WEEKLY: '1wk',
  MONTHLY: '1mo',
  QUARTERLY: '3mo'
};

class HistoricalData {
  constructor(data) {
    this.open = data.open;
    this.high = data.high;
    this.low = data.low;
    this.close = data.close;
    this.adjClose = data.adjClose;
    this.volume = data.volume;
  }
}

class TimeSeries {
  constructor(data) {
    this.history = data.history;
  }
}

module.exports = { TimePeriod, Interval, HistoricalData, TimeSeries }; 
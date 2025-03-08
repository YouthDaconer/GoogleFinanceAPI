const Indicator = {
  SMA: 'SMA',
  EMA: 'EMA',
  WMA: 'WMA',
  VWMA: 'VWMA',
  RSI: 'RSI',
  SRSI: 'SRSI',
  STOCH: 'STOCH',
  CCI: 'CCI',
  OBV: 'OBV',
  BBANDS: 'BBANDS',
  AROON: 'AROON',
  ADX: 'ADX',
  MACD: 'MACD',
  SUPER_TREND: 'SUPERTREND',
  ICHIMOKU: 'ICHIMOKU'
};

class Analysis {
  constructor(type, indicators) {
    this.type = type;
    this.indicators = indicators;
  }
}

class SummaryAnalysis {
  constructor(data) {
    this.symbol = data.symbol;
    this.sma_10 = data.sma_10;
    this.sma_20 = data.sma_20;
    this.sma_50 = data.sma_50;
    this.sma_100 = data.sma_100;
    this.sma_200 = data.sma_200;
    this.ema_10 = data.ema_10;
    this.ema_20 = data.ema_20;
    this.ema_50 = data.ema_50;
    this.ema_100 = data.ema_100;
    this.ema_200 = data.ema_200;
    this.wma_10 = data.wma_10;
    this.wma_20 = data.wma_20;
    this.wma_50 = data.wma_50;
    this.wma_100 = data.wma_100;
    this.wma_200 = data.wma_200;
    this.vwma = data.vwma;
    this.rsi = data.rsi;
    this.srsi = data.srsi;
    this.cci = data.cci;
    this.adx = data.adx;
    this.macd = data.macd;
    this.stoch = data.stoch;
    this.aroon = data.aroon;
    this.bbands = data.bbands;
    this.supertrend = data.supertrend;
    this.ichimoku = data.ichimoku;
  }
}

module.exports = { Indicator, Analysis, SummaryAnalysis }; 
class Quote {
  constructor(data) {
    this.symbol = data.symbol;
    this.name = data.name;
    this.price = data.price;
    this.exchange = data.exchange;
    this.preMarketPrice = data.preMarketPrice;
    this.afterHoursPrice = data.afterHoursPrice;
    this.change = data.change;
    this.percentChange = data.percentChange;
    this.currency = data.currency;
    this.open = data.open;
    this.high = data.high;
    this.low = data.low;
    this.yearHigh = data.yearHigh;
    this.yearLow = data.yearLow;
    this.volume = data.volume;
    this.avgVolume = data.avgVolume;
    this.marketCap = data.marketCap;
    this.beta = data.beta;
    this.pe = data.pe;
    this.eps = data.eps;
    this.dividend = data.dividend;
    this.dividendYield = data.dividendYield;
    this.exDividend = data.exDividend;
    this.netAssets = data.netAssets;
    this.nav = data.nav;
    this.expenseRatio = data.expenseRatio;
    this.category = data.category;
    this.lastCapitalGain = data.lastCapitalGain;
    this.morningstarRating = data.morningstarRating;
    this.morningstarRiskRating = data.morningstarRiskRating;
    this.holdingsTurnover = data.holdingsTurnover;
    this.earningsDate = data.earningsDate;
    this.lastDividend = data.lastDividend;
    this.inceptionDate = data.inceptionDate;
    this.sector = data.sector;
    this.industry = data.industry;
    this.about = data.about;
    this.employees = data.employees;
    this.ytdReturn = data.ytdReturn;
    this.yearReturn = data.yearReturn;
    this.threeYearReturn = data.threeYearReturn;
    this.fiveYearReturn = data.fiveYearReturn;
    this.logo = data.logo;
  }
}

module.exports = Quote; 
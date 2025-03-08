class SimpleQuote {
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
    this.logo = data.logo;
  }
}

module.exports = SimpleQuote; 
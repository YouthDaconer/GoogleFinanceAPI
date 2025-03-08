class MarketMover {
  constructor(data) {
    this.symbol = data.symbol;
    this.name = data.name;
    this.price = data.price;
    this.change = data.change;
    this.percentChange = data.percentChange;
  }
}

module.exports = MarketMover; 
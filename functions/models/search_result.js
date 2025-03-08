const Type = {
  STOCK: 'stock',
  ETF: 'etf',
  TRUST: 'trust'
};

class SearchResult {
  constructor(data) {
    this.name = data.name;
    this.symbol = data.symbol;
    this.exchange = data.exchange;
    this.type = data.type;
  }
}

module.exports = { Type, SearchResult }; 
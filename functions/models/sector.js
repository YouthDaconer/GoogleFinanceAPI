const Sector = {
  BASIC_MATERIALS: 'Basic Materials',
  COMMUNICATION: 'Communication Services',
  CONSUMER_CYCLICAL: 'Consumer Cyclical',
  CONSUMER_DEFENSIVE: 'Consumer Defensive',
  ENERGY: 'Energy',
  FINANCIAL_SERVICES: 'Financial Services',
  HEALTHCARE: 'Healthcare',
  INDUSTRIALS: 'Industrials',
  REAL_ESTATE: 'Real Estate',
  TECHNOLOGY: 'Technology',
  UTILITIES: 'Utilities'
};

class MarketSector {
  constructor(data) {
    this.sector = data.sector;
    this.dayReturn = data.dayReturn;
    this.ytdReturn = data.ytdReturn;
    this.yearReturn = data.yearReturn;
    this.threeYearReturn = data.threeYearReturn;
    this.fiveYearReturn = data.fiveYearReturn;
  }
}

class MarketSectorDetails extends MarketSector {
  constructor(data) {
    super(data);
    this.marketCap = data.marketCap;
    this.marketWeight = data.marketWeight;
    this.industries = data.industries;
    this.companies = data.companies;
    this.topIndustries = data.topIndustries;
    this.topCompanies = data.topCompanies;
  }
}

module.exports = { Sector, MarketSector, MarketSectorDetails }; 
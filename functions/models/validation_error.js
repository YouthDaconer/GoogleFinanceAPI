class ValidationErrorDetail {
  constructor(data) {
    this.loc = data.loc;
    this.msg = data.msg;
    this.type = data.type;
  }
}

class ValidationErrorResponse {
  constructor(data) {
    this.detail = data.detail;
    this.errors = data.errors;
  }
}

module.exports = { ValidationErrorDetail, ValidationErrorResponse }; 
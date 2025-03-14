/* eslint-disable require-jsdoc */
const axios = require("axios");

/**
 * Obtiene todos los índices del endpoint
 * @returns {Promise<Array>} Array con todos los índices
 */
async function requestIndicesFromFinance() {
  try {
    const response = await axios.get(
      "https://dmn46d7xas3rvio6tugd2vzs2q0hxbmb.lambda-url.us-east-1.on.aws/v1/indices"
    );
    return response.data;
  } catch (error) {
    throw new Error(`Error fetching indices: ${error.message}`);
  }
}

module.exports = requestIndicesFromFinance;
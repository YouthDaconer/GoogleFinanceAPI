/* eslint-disable require-jsdoc */
const axios = require("axios");
const { FINANCE_QUERY_API_URL, getServiceHeaders } = require('./config');

/**
 * Obtiene todos los índices del endpoint
 * SEC-CF-001: Migrado a Cloudflare Tunnel
 * SEC-TOKEN-004: Incluye headers de autenticación de servicio
 * @returns {Promise<Array>} Array con todos los índices
 */
async function requestIndicesFromFinance() {
  try {
    const response = await axios.get(
      `${FINANCE_QUERY_API_URL}/indices`,
      { headers: getServiceHeaders() }
    );
    return response.data;
  } catch (error) {
    throw new Error(`Error fetching indices: ${error.message}`);
  }
}

module.exports = requestIndicesFromFinance;
const { SESClient, SendEmailCommand } = require("@aws-sdk/client-ses");
const { buildEmailContent } = require("./emailTemplates");

const SES_REGION = process.env.AWS_SES_REGION || "us-east-2";
const SES_SENDER = process.env.SES_SUBSCRIPTION_SENDER || "subscriptions@portastock.net";
const SES_REPLY_TO = process.env.SES_REPLY_TO || "no-reply@portastock.net";
const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_RETRY_DELAYS = [1000, 3000];
const DEFAULT_TIMEOUT_MS = 8000;

let _sesClient = null;

function _getSesClient() {
  if (!_sesClient) {
    _sesClient = new SESClient({
      region: SES_REGION,
      credentials: {
        accessKeyId: process.env.AWS_SES_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SES_SECRET_ACCESS_KEY,
      },
    });
  }
  return _sesClient;
}

const EMAIL_TEMPLATES = Object.freeze({
  TRIAL_EXPIRING: "trial_expiring",
  PAYMENT_FAILED: "payment_failed",
  SUBSCRIPTION_CANCELLED: "subscription_cancelled",
  PAYMENT_RENEWED: "payment_renewed",
});

async function sendTransactionalEmail(templateName, recipientEmail, templateData = {}, options = {}) {
  if (!recipientEmail || !templateName) {
    console.warn("[Email] Missing required fields — skipping email send");
    return null;
  }

  if (process.env.SES_TRANSACTIONAL_ENABLED === "false") {
    console.log("[Email] Transactional emails disabled via SES_TRANSACTIONAL_ENABLED");
    return null;
  }

  if (!process.env.AWS_SES_ACCESS_KEY_ID) {
    console.warn("[Email] AWS_SES_ACCESS_KEY_ID not configured — skipping email send");
    return null;
  }

  const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const retryDelays = DEFAULT_RETRY_DELAYS.slice(0, maxRetries);

  const enrichedData = {
    ...templateData,
    year: new Date().getFullYear(),
    appName: "Portastock",
    appUrl: process.env.APP_URL || "https://portastock.net",
  };

  const { subject, html, text } = buildEmailContent(templateName, enrichedData);

  const command = new SendEmailCommand({
    Source: `Portastock <${SES_SENDER}>`,
    ReplyToAddresses: [SES_REPLY_TO],
    Destination: { ToAddresses: [recipientEmail] },
    Message: {
      Subject: { Data: subject, Charset: "UTF-8" },
      Body: {
        Html: { Data: html, Charset: "UTF-8" },
        Text: { Data: text, Charset: "UTF-8" },
      },
    },
    Tags: [{ Name: "type", Value: "subscription" }],
  });

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const abortController = new AbortController();
      const timer = setTimeout(() => abortController.abort(), timeoutMs);
      const response = await _getSesClient().send(command, { abortSignal: abortController.signal });
      clearTimeout(timer);
      console.log(`[Email] Sent: ${templateName} → ${recipientEmail} (MessageId: ${response.MessageId})`);
      return response.MessageId;
    } catch (err) {
      if (attempt < maxRetries) {
        console.warn(`[Email] Retry ${attempt + 1}/${maxRetries} for ${templateName}:`, err.message);
        await new Promise((r) => setTimeout(r, retryDelays[attempt]));
      } else {
        console.error(`[Email] Failed to send ${templateName} after ${maxRetries + 1} attempts:`, err.message);
        return null;
      }
    }
  }

  return null;
}

function _resetSesClient() {
  _sesClient = null;
}

module.exports = { sendTransactionalEmail, EMAIL_TEMPLATES, _resetSesClient };

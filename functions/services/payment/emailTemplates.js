const APP_URL_DEFAULT = "https://portastock.net";

// ── Shared HTML Components (dark theme — consistent with email_utils.py) ──

function wrapHtml(title, bodyContent) {
  return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>${escapeHtml(title)}</title></head>
<body style="margin:0;padding:0;background-color:#0f0f23;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#0f0f23;padding:20px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0"
       style="max-width:600px;width:100%;background-color:#1a1a2e;border-radius:12px;overflow:hidden;">
${bodyContent}
${renderFooter()}
</table>
</td></tr></table>
</body></html>`;
}

function renderHeader(title, subtitle) {
  return `<tr><td style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);padding:30px 40px;text-align:center;">
<h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;">${escapeHtml(title)}</h1>
${subtitle ? `<p style="margin:8px 0 0;color:rgba(255,255,255,0.85);font-size:14px;">${escapeHtml(subtitle)}</p>` : ""}
</td></tr>`;
}

function renderCTA(text, url) {
  const safeUrl = encodeURI(url);
  return `<tr><td style="padding:24px 40px;text-align:center;">
<a href="${safeUrl}" target="_blank" rel="noopener"
   style="display:inline-block;padding:14px 32px;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:#ffffff;text-decoration:none;border-radius:8px;font-size:16px;font-weight:600;">
${escapeHtml(text)}</a>
</td></tr>`;
}

function renderFooter() {
  return `<tr><td style="padding:20px 40px;text-align:center;border-top:1px solid #2d2d44;">
<p style="margin:0;color:#6b7280;font-size:12px;">Portastock &mdash; Tu portafolio inteligente</p>
<p style="margin:4px 0 0;color:#6b7280;font-size:11px;">
Este email fue enviado porque tienes una cuenta en Portastock.
</p>
</td></tr>`;
}

function renderSection(content) {
  return `<tr><td style="padding:24px 40px;">
<div style="color:#d1d5db;font-size:15px;line-height:1.6;">${content}</div>
</td></tr>`;
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDate(isoDate) {
  if (!isoDate) return "";
  try {
    return new Date(isoDate).toLocaleDateString("es-ES", {
      year: "numeric", month: "long", day: "numeric",
    });
  } catch {
    return String(isoDate);
  }
}

// ── Template Builders ──

function buildTrialExpiringEmail(data) {
  const { userName, daysLeft, expiryDate, pricingUrl, appUrl } = data;
  const url = pricingUrl || `${appUrl || APP_URL_DEFAULT}/pricing`;
  const subject = `Tu periodo de prueba expira en ${daysLeft} día${daysLeft === 1 ? "" : "s"}`;

  const body =
    renderHeader("Periodo de Prueba por Expirar", `${daysLeft} día${daysLeft === 1 ? "" : "s"} restante${daysLeft === 1 ? "" : "s"}`) +
    renderSection(
      `<p style="margin:0 0 12px;">Hola <strong>${escapeHtml(userName)}</strong>,</p>` +
      `<p style="margin:0 0 12px;">Tu periodo de prueba gratuito de Portastock expira el <strong>${formatDate(expiryDate)}</strong>.</p>` +
      `<p style="margin:0;">Para seguir accediendo a todas las funcionalidades Pro, actualiza tu plan antes de que expire.</p>`
    ) +
    renderCTA("Ver Planes", url);

  const text = `Hola ${userName},\n\nTu periodo de prueba de Portastock expira en ${daysLeft} día(s) — ${formatDate(expiryDate)}.\n\nActualiza tu plan: ${url}\n\nPortastock`;

  return { subject, html: wrapHtml(subject, body), text };
}

function buildPaymentFailedEmail(data) {
  const { userName, updatePaymentUrl, appUrl } = data;
  const url = updatePaymentUrl || `${appUrl || APP_URL_DEFAULT}/settings`;
  const subject = "Problema con tu pago — acción requerida";

  const body =
    renderHeader("Pago No Procesado", "Acción requerida") +
    renderSection(
      `<p style="margin:0 0 12px;">Hola <strong>${escapeHtml(userName)}</strong>,</p>` +
      `<p style="margin:0 0 12px;">No pudimos procesar tu último pago de suscripción. Tu acceso Pro podría verse afectado si no se resuelve pronto.</p>` +
      `<p style="margin:0;">Por favor, verifica tu método de pago en la configuración de tu cuenta.</p>`
    ) +
    renderCTA("Actualizar Método de Pago", url);

  const text = `Hola ${userName},\n\nNo pudimos procesar tu último pago de suscripción. Tu acceso Pro podría verse afectado.\n\nActualiza tu método de pago: ${url}\n\nPortastock`;

  return { subject, html: wrapHtml(subject, body), text };
}

function buildSubscriptionCancelledEmail(data) {
  const { userName, planName, effectiveDate, appUrl } = data;
  const url = `${appUrl || APP_URL_DEFAULT}/pricing`;
  const subject = "Tu suscripción ha sido cancelada";

  const effectiveLine = effectiveDate
    ? `<p style="margin:0 0 12px;">Tu acceso a las funcionalidades <strong>${escapeHtml(planName || "Pro")}</strong> continuará hasta el <strong>${formatDate(effectiveDate)}</strong>.</p>`
    : "";

  const body =
    renderHeader("Suscripción Cancelada") +
    renderSection(
      `<p style="margin:0 0 12px;">Hola <strong>${escapeHtml(userName)}</strong>,</p>` +
      `<p style="margin:0 0 12px;">Hemos procesado la cancelación de tu suscripción.</p>` +
      effectiveLine +
      `<p style="margin:0;">Si cambias de opinión, siempre puedes volver a suscribirte.</p>`
    ) +
    renderCTA("Ver Planes", url);

  const effectiveText = effectiveDate ? ` Tu acceso continuará hasta el ${formatDate(effectiveDate)}.` : "";
  const text = `Hola ${userName},\n\nHemos procesado la cancelación de tu suscripción.${effectiveText}\n\nVer planes: ${url}\n\nPortastock`;

  return { subject, html: wrapHtml(subject, body), text };
}

function buildPaymentRenewedEmail(data) {
  const { userName, planName, amount, appUrl } = data;
  const url = `${appUrl || APP_URL_DEFAULT}/dashboard`;
  const subject = "Pago confirmado — tu suscripción está activa";

  const amountLine = amount
    ? `<p style="margin:0 0 12px;">Monto: <strong>${escapeHtml(amount)}</strong></p>`
    : "";

  const body =
    renderHeader("Pago Confirmado", `Plan ${escapeHtml(planName || "Pro")}`) +
    renderSection(
      `<p style="margin:0 0 12px;">Hola <strong>${escapeHtml(userName)}</strong>,</p>` +
      `<p style="margin:0 0 12px;">Tu pago ha sido procesado exitosamente y tu suscripción <strong>${escapeHtml(planName || "Pro")}</strong> ha sido renovada.</p>` +
      amountLine +
      `<p style="margin:0;">Disfruta de todas las funcionalidades de Portastock.</p>`
    ) +
    renderCTA("Ir al Dashboard", url);

  const amountText = amount ? ` Monto: ${amount}.` : "";
  const text = `Hola ${userName},\n\nTu pago ha sido procesado y tu suscripción ${planName || "Pro"} ha sido renovada.${amountText}\n\nIr al dashboard: ${url}\n\nPortastock`;

  return { subject, html: wrapHtml(subject, body), text };
}

// ── Router ──

const TEMPLATE_BUILDERS = {
  trial_expiring: buildTrialExpiringEmail,
  payment_failed: buildPaymentFailedEmail,
  subscription_cancelled: buildSubscriptionCancelledEmail,
  payment_renewed: buildPaymentRenewedEmail,
};

function buildEmailContent(templateName, data) {
  const builder = TEMPLATE_BUILDERS[templateName];
  if (!builder) throw new Error(`Unknown email template: ${templateName}`);
  return builder(data);
}

module.exports = {
  buildEmailContent,
  buildTrialExpiringEmail,
  buildPaymentFailedEmail,
  buildSubscriptionCancelledEmail,
  buildPaymentRenewedEmail,
  wrapHtml,
  escapeHtml,
  formatDate,
};

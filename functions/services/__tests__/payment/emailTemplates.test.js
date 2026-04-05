const {
  buildEmailContent,
  buildTrialExpiringEmail,
  buildPaymentFailedEmail,
  buildSubscriptionCancelledEmail,
  buildPaymentRenewedEmail,
  wrapHtml,
  escapeHtml,
} = require("../../payment/emailTemplates");

describe("emailTemplates — shared components", () => {
  test("wrapHtml produces valid HTML with dark theme colors", () => {
    const html = wrapHtml("Test Title", "<tr><td>body</td></tr>");
    expect(html).toContain("<!DOCTYPE html>");
    expect(html).toContain("#0f0f23");
    expect(html).toContain("#1a1a2e");
    expect(html).toContain("Test Title");
  });

  test("escapeHtml sanitizes dangerous characters", () => {
    expect(escapeHtml('<script>alert("xss")</script>')).toBe(
      "&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;"
    );
  });

  test("escapeHtml handles null/undefined gracefully", () => {
    expect(escapeHtml(null)).toBe("");
    expect(escapeHtml(undefined)).toBe("");
  });
});

describe("buildTrialExpiringEmail", () => {
  test("returns subject, html, text with interpolated variables", () => {
    const result = buildTrialExpiringEmail({
      userName: "Carlos",
      daysLeft: 3,
      expiryDate: "2026-04-08T00:00:00.000Z",
      pricingUrl: "https://portastock.net/pricing",
    });

    expect(result.subject).toContain("3 días");
    expect(result.html).toContain("Carlos");
    expect(result.html).toContain("#667eea");
    expect(result.html).toContain("Ver Planes");
    expect(result.text).toContain("Carlos");
    expect(result.text).toContain("3 día(s)");
  });

  test("singular day wording for daysLeft=1", () => {
    const result = buildTrialExpiringEmail({
      userName: "User",
      daysLeft: 1,
      expiryDate: "2026-04-06T00:00:00.000Z",
    });

    expect(result.subject).toContain("1 día");
    expect(result.subject).not.toContain("1 días");
  });
});

describe("buildPaymentFailedEmail", () => {
  test("returns HTML with CTA to settings", () => {
    const result = buildPaymentFailedEmail({
      userName: "Ana",
      updatePaymentUrl: "https://portastock.net/settings",
    });

    expect(result.subject).toContain("Problema con tu pago");
    expect(result.html).toContain("Ana");
    expect(result.html).toContain("Actualizar Método de Pago");
    expect(result.html).toContain("portastock.net/settings");
    expect(result.text).toContain("Ana");
  });

  test("uses default URL when updatePaymentUrl not provided", () => {
    const result = buildPaymentFailedEmail({ userName: "User" });
    expect(result.html).toContain("portastock.net/settings");
  });
});

describe("buildSubscriptionCancelledEmail", () => {
  test("includes effectiveDate when provided", () => {
    const result = buildSubscriptionCancelledEmail({
      userName: "Pedro",
      planName: "Pro",
      effectiveDate: "2026-05-01T00:00:00.000Z",
    });

    expect(result.subject).toContain("cancelada");
    expect(result.html).toContain("Pedro");
    expect(result.html).toContain("Pro");
    expect(result.html).toContain("continuará hasta");
    expect(result.html).toMatch(/\d{1,2} de \w+ de 2026/);
    expect(result.text).toContain("Pedro");
  });

  test("omits effectiveDate line when not provided", () => {
    const result = buildSubscriptionCancelledEmail({
      userName: "Pedro",
    });

    expect(result.html).not.toContain("continuará hasta");
    expect(result.text).not.toContain("continuará hasta");
  });
});

describe("buildPaymentRenewedEmail", () => {
  test("includes amount when provided", () => {
    const result = buildPaymentRenewedEmail({
      userName: "Maria",
      planName: "Pro",
      amount: "$9.99",
    });

    expect(result.subject).toContain("Pago confirmado");
    expect(result.html).toContain("Maria");
    expect(result.html).toContain("$9.99");
    expect(result.html).toContain("Ir al Dashboard");
    expect(result.text).toContain("$9.99");
  });

  test("omits amount line when not provided", () => {
    const result = buildPaymentRenewedEmail({
      userName: "Maria",
      planName: "Pro",
    });

    expect(result.html).not.toContain("Monto:");
    expect(result.text).not.toContain("Monto:");
  });
});

describe("buildEmailContent — router", () => {
  test("routes to correct builder for each template", () => {
    const data = { userName: "Test", daysLeft: 2, expiryDate: "2026-04-07T00:00:00.000Z" };

    expect(buildEmailContent("trial_expiring", data).subject).toContain("prueba");
    expect(buildEmailContent("payment_failed", data).subject).toContain("pago");
    expect(buildEmailContent("subscription_cancelled", data).subject).toContain("cancelada");
    expect(buildEmailContent("payment_renewed", data).subject).toContain("confirmado");
  });

  test("throws Error for unknown template", () => {
    expect(() => buildEmailContent("nonexistent", {})).toThrow(
      "Unknown email template: nonexistent"
    );
  });
});

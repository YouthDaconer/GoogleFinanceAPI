const mockSend = jest.fn();

jest.mock("@aws-sdk/client-ses", () => ({
  SESClient: jest.fn().mockImplementation(() => ({ send: mockSend })),
  SendEmailCommand: jest.fn().mockImplementation((params) => params),
}));

const { sendTransactionalEmail, EMAIL_TEMPLATES, _resetSesClient } = require("../../payment/emailService");
const { SESClient, SendEmailCommand } = require("@aws-sdk/client-ses");

describe("emailService", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    _resetSesClient();
    process.env = {
      ...originalEnv,
      AWS_SES_ACCESS_KEY_ID: "test-key-id",
      AWS_SES_SECRET_ACCESS_KEY: "test-secret",
    };
    mockSend.mockResolvedValue({ MessageId: "ses-msg-001" });
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  test("sends email via SES with correct params", async () => {
    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.PAYMENT_RENEWED,
      "user@test.com",
      { userName: "Carlos", planName: "Pro", amount: "$9.99" }
    );

    expect(result).toBe("ses-msg-001");
    expect(SendEmailCommand).toHaveBeenCalledWith(
      expect.objectContaining({
        Source: expect.stringContaining("subscriptions@portastock.net"),
        Destination: { ToAddresses: ["user@test.com"] },
        Tags: [{ Name: "type", Value: "subscription" }],
      })
    );
  });

  test("returns null when recipientEmail is empty", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    const result = await sendTransactionalEmail(EMAIL_TEMPLATES.PAYMENT_FAILED, "", { userName: "X" });

    expect(result).toBeNull();
    expect(mockSend).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  test("returns null when templateName is empty", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    const result = await sendTransactionalEmail("", "user@test.com", {});

    expect(result).toBeNull();
    expect(mockSend).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  test("returns null when AWS_SES_ACCESS_KEY_ID is not configured", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    delete process.env.AWS_SES_ACCESS_KEY_ID;

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.PAYMENT_RENEWED, "user@test.com", { userName: "X" }
    );

    expect(result).toBeNull();
    expect(mockSend).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  test("retries on SES failure and succeeds on second attempt", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    mockSend
      .mockRejectedValueOnce(new Error("Throttling"))
      .mockResolvedValueOnce({ MessageId: "ses-msg-retry" });

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.PAYMENT_FAILED,
      "user@test.com",
      { userName: "Retry" },
      { maxRetries: 2, timeoutMs: 8000 }
    );

    expect(result).toBe("ses-msg-retry");
    expect(mockSend).toHaveBeenCalledTimes(2);
    warnSpy.mockRestore();
  });

  test("returns null after exhausting all retries", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation();
    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    mockSend.mockRejectedValue(new Error("SES unavailable"));

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.PAYMENT_FAILED,
      "user@test.com",
      { userName: "Fail" },
      { maxRetries: 2, timeoutMs: 8000 }
    );

    expect(result).toBeNull();
    expect(mockSend).toHaveBeenCalledTimes(3);
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test("kill switch SES_TRANSACTIONAL_ENABLED=false disables all emails", async () => {
    const logSpy = jest.spyOn(console, "log").mockImplementation();
    process.env.SES_TRANSACTIONAL_ENABLED = "false";

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.PAYMENT_RENEWED, "user@test.com", { userName: "X" }
    );

    expect(result).toBeNull();
    expect(mockSend).not.toHaveBeenCalled();
    logSpy.mockRestore();
  });

  test("options.maxRetries=0 sends only one attempt", async () => {
    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    mockSend.mockRejectedValue(new Error("Fail"));

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.TRIAL_EXPIRING,
      "user@test.com",
      { userName: "X", daysLeft: 3 },
      { maxRetries: 0, timeoutMs: 8000 }
    );

    expect(result).toBeNull();
    expect(mockSend).toHaveBeenCalledTimes(1);
    errorSpy.mockRestore();
  });

  test("timeout aborts a slow SES call", async () => {
    const errorSpy = jest.spyOn(console, "error").mockImplementation();
    mockSend.mockImplementation(() =>
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error("The operation was aborted")), 500);
      })
    );

    const result = await sendTransactionalEmail(
      EMAIL_TEMPLATES.TRIAL_EXPIRING,
      "user@test.com",
      { userName: "X", daysLeft: 1 },
      { maxRetries: 0, timeoutMs: 100 }
    );

    expect(result).toBeNull();
    errorSpy.mockRestore();
  });

  test("EMAIL_TEMPLATES constants are frozen", () => {
    expect(Object.isFrozen(EMAIL_TEMPLATES)).toBe(true);
    expect(EMAIL_TEMPLATES.TRIAL_EXPIRING).toBe("trial_expiring");
    expect(EMAIL_TEMPLATES.PAYMENT_FAILED).toBe("payment_failed");
    expect(EMAIL_TEMPLATES.SUBSCRIPTION_CANCELLED).toBe("subscription_cancelled");
    expect(EMAIL_TEMPLATES.PAYMENT_RENEWED).toBe("payment_renewed");
  });
});

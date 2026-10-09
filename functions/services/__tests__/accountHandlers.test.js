/**
 * GATE-006: Tests unitarios de validación de límite de cuentas
 * @see docs/stories/GATE-006.story.md (AC-09, AC-10, AC-11, AC-20)
 */

const { HttpsError } = require("firebase-functions/v2/https");

// Mock firebase-admin/firestore
//
// HU 2.6: `addPortfolioAccount` escribe en batch y genera el id de la cuenta con
// `.doc()` en vez de `.add()`, porque cada saldo inicial nace con su asiento de
// apertura y las dos escrituras tienen que ir juntas (RN-2.6-C).
const mockBatch = {
  set: jest.fn(),
  update: jest.fn(),
  commit: jest.fn(() => Promise.resolve()),
};
const mockDoc = jest.fn(() => ({
  id: "new-account-id",
  get: jest.fn(() => Promise.resolve({ exists: false, data: () => undefined })),
}));
const mockCollection = jest.fn(() => ({ doc: mockDoc }));

jest.mock("firebase-admin/firestore", () => ({
  getFirestore: () => ({
    collection: mockCollection,
    batch: () => mockBatch,
  }),
  FieldValue: {
    serverTimestamp: () => "SERVER_TIMESTAMP",
  },
}));

jest.mock("../firebaseAdmin", () => {
  const mockAdmin = { firestore: jest.fn(() => ({ collection: mockCollection })) };
  mockAdmin.firestore.FieldValue = { serverTimestamp: () => "SERVER_TIMESTAMP" };
  return mockAdmin;
});

jest.mock("../historicalRateService", () => ({
  getCrossRate: jest.fn(() => Promise.resolve(null)),
  getRateForDate: jest.fn(() => Promise.resolve(null)),
}));

jest.mock("../portfolioDistributionService", () => ({
  invalidateDistributionCache: jest.fn(),
}));

const mockValidateQuantityLimit = jest.fn();
jest.mock("../helpers/subscriptionValidator", () => ({
  validateQuantityLimit: (...args) => mockValidateQuantityLimit(...args),
  validateFeatureAccess: jest.fn(),
}));

const { addPortfolioAccount } = require("../handlers/accountHandlers");

describe("GATE-006: addPortfolioAccount account limit validation", () => {
  const baseContext = (uid = "user-123") => ({ auth: { uid } });
  const basePayload = { name: "Mi Cuenta" };

  beforeEach(() => {
    jest.clearAllMocks();
    mockValidateQuantityLimit.mockResolvedValue(undefined);
  });

  test("Free user con 2 cuentas activas es rechazado con resource-exhausted", async () => {
    mockValidateQuantityLimit.mockRejectedValue(
      new HttpsError("resource-exhausted", "Límite de 2 alcanzado. Actualiza a Pro para acceso ilimitado.")
    );

    await expect(
      addPortfolioAccount(baseContext(), basePayload)
    ).rejects.toMatchObject({ code: "resource-exhausted" });

    expect(mockValidateQuantityLimit).toHaveBeenCalledWith(
      "user-123", "maxAccounts", "portfolioAccounts", { isActive: true }
    );
  });

  test("Free user con 1 cuenta activa puede crear cuenta", async () => {
    mockValidateQuantityLimit.mockResolvedValue(undefined);

    const result = await addPortfolioAccount(baseContext(), basePayload);
    expect(result.success).toBe(true);
    expect(result.accountId).toBe("new-account-id");
  });

  test("Pro user con muchas cuentas puede crear cuenta", async () => {
    mockValidateQuantityLimit.mockResolvedValue(undefined);

    const result = await addPortfolioAccount(baseContext(), basePayload);
    expect(result.success).toBe(true);
  });

  test("User sin subscription (legacy) usa default Free limit y es rechazado", async () => {
    mockValidateQuantityLimit.mockRejectedValue(
      new HttpsError("resource-exhausted", "Límite de 2 alcanzado. Actualiza a Pro para acceso ilimitado.")
    );

    await expect(
      addPortfolioAccount(baseContext(), basePayload)
    ).rejects.toMatchObject({ code: "resource-exhausted" });
  });
});

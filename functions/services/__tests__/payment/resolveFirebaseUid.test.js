const mockGetUserByEmail = jest.fn();

jest.mock("../../firebaseAdmin", () => {
  const authFn = () => ({ getUserByEmail: mockGetUserByEmail });
  const firestoreFn = () => ({});
  firestoreFn.FieldValue = { serverTimestamp: jest.fn() };
  return { auth: authFn, firestore: firestoreFn };
});

const { resolveFirebaseUid } = require("../../payment/webhookHandler");

beforeEach(() => {
  jest.clearAllMocks();
});

describe("resolveFirebaseUid", () => {
  test("AC-22: metadata.firebase_uid present with valid format returns UID", async () => {
    const data = { metadata: { firebase_uid: "abcdefghij1234567890" } };

    const result = await resolveFirebaseUid(data);

    expect(result).toBe("abcdefghij1234567890");
    expect(mockGetUserByEmail).not.toHaveBeenCalled();
  });

  test("invalid firebase_uid format (too short) returns null with warning", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    const data = { metadata: { firebase_uid: "abc" } };

    const result = await resolveFirebaseUid(data);

    expect(result).toBeNull();
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("Invalid firebase_uid format"),
      expect.any(String)
    );

    warnSpy.mockRestore();
  });

  test("AC-23: no metadata, email found in Firebase Auth returns UID", async () => {
    mockGetUserByEmail.mockResolvedValue({ uid: "uid-found-by-email" });
    const data = { user: { email: "user@test.com" } };

    const result = await resolveFirebaseUid(data);

    expect(result).toBe("uid-found-by-email");
    expect(mockGetUserByEmail).toHaveBeenCalledWith("user@test.com");
  });

  test("AC-24: no metadata, email not found in Auth returns null", async () => {
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    mockGetUserByEmail.mockRejectedValue(new Error("User not found"));
    const data = { user: { email: "unknown@test.com" } };

    const result = await resolveFirebaseUid(data);

    expect(result).toBeNull();
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining("resolveFirebaseUid email fallback failed"),
      "User not found"
    );

    warnSpy.mockRestore();
  });

  test("AC-25: no metadata and no email returns null", async () => {
    const data = {};

    const result = await resolveFirebaseUid(data);

    expect(result).toBeNull();
  });

  test("null data returns null", async () => {
    const result = await resolveFirebaseUid(null);

    expect(result).toBeNull();
  });
});

/**
 * BUG-ALERT-002: Tests para alertRetention
 *
 * Cubre la paginación por lotes, el tope por ejecución y el enrutado de cada
 * colección a su campo de fecha y su periodo de retención.
 *
 * @see services/alertRetention.js
 */

jest.mock('firebase-admin/firestore', () => ({
  getFirestore: () => ({}),
}));
jest.mock('../firebaseAdmin', () => ({}));
jest.mock('firebase-functions/v2/scheduler', () => ({
  onSchedule: (_opts, handler) => handler,
}));

const {
  purgeOlderThan,
  purgeAlertHistory,
  NOTIFICATION_RETENTION_DAYS,
  HISTORY_RETENTION_DAYS,
  FIRESTORE_BATCH_LIMIT,
  MAX_DELETES_PER_RUN,
} = require('../alertRetention');

// ============================================================================
// FAKE FIRESTORE
// ============================================================================

/**
 * Firestore de mentira que devuelve páginas predefinidas por colección y
 * registra los borrados y los filtros aplicados.
 */
function makeFirestore(pagesByCollection) {
  const deleted = [];
  const queries = [];
  const commits = [];
  const cursors = {};

  const firestore = {
    collection(name) {
      const q = { collection: name };
      queries.push(q);
      return {
        where(field, op, value) {
          Object.assign(q, { field, op, value });
          return this;
        },
        limit(n) {
          q.limit = n;
          return this;
        },
        async get() {
          cursors[name] = cursors[name] || 0;
          const page = (pagesByCollection[name] || [])[cursors[name]] || [];
          cursors[name] += 1;
          const docs = page.map((id) => ({ ref: { id, collection: name } }));
          return { docs, size: docs.length, empty: docs.length === 0 };
        },
      };
    },
    batch() {
      const staged = [];
      return {
        delete(ref) {
          staged.push(ref);
        },
        async commit() {
          deleted.push(...staged);
          commits.push(staged.length);
        },
      };
    },
  };

  return { firestore, deleted, queries, commits };
}

/** Página llena de `n` ids sintéticos. */
function page(n, prefix) {
  return Array.from({ length: n }, (_, i) => `${prefix}-${i}`);
}

// ============================================================================
// TESTS
// ============================================================================

describe('purgeOlderThan', () => {
  it('no borra nada cuando no hay documentos vencidos', async () => {
    const { firestore, deleted, commits } = makeFirestore({ userNotifications: [[]] });

    const result = await purgeOlderThan(firestore, 'userNotifications', 'createdAt', 90);

    expect(result).toEqual({ deleted: 0, truncated: false });
    expect(deleted).toHaveLength(0);
    expect(commits).toHaveLength(0);
  });

  it('filtra por el campo de fecha con el corte de la retención', async () => {
    const { firestore, queries } = makeFirestore({ userNotifications: [[]] });
    const before = Date.now();

    await purgeOlderThan(firestore, 'userNotifications', 'createdAt', 90);

    const [q] = queries;
    expect(q.collection).toBe('userNotifications');
    expect(q.field).toBe('createdAt');
    expect(q.op).toBe('<');
    expect(q.limit).toBe(FIRESTORE_BATCH_LIMIT);

    const expected = before - 90 * 24 * 60 * 60 * 1000;
    expect(Math.abs(q.value.getTime() - expected)).toBeLessThan(5000);
  });

  it('para en la primera página incompleta', async () => {
    const { firestore, deleted, commits } = makeFirestore({
      alertHistory: [page(3, 'a')],
    });

    const result = await purgeOlderThan(firestore, 'alertHistory', 'triggeredAt', 365);

    expect(result.deleted).toBe(3);
    expect(commits).toEqual([3]);
    expect(deleted.map((d) => d.id)).toEqual(['a-0', 'a-1', 'a-2']);
  });

  it('pagina mientras los lotes vengan llenos', async () => {
    const { firestore, commits } = makeFirestore({
      userNotifications: [
        page(FIRESTORE_BATCH_LIMIT, 'p1'),
        page(FIRESTORE_BATCH_LIMIT, 'p2'),
        page(7, 'p3'),
      ],
    });

    const result = await purgeOlderThan(firestore, 'userNotifications', 'createdAt', 90);

    expect(result.deleted).toBe(FIRESTORE_BATCH_LIMIT * 2 + 7);
    expect(result.truncated).toBe(false);
    expect(commits).toHaveLength(3);
  });

  it('corta en el tope por ejecución para no agotar el timeout', async () => {
    const fullPages = MAX_DELETES_PER_RUN / FIRESTORE_BATCH_LIMIT + 5;
    const { firestore } = makeFirestore({
      userNotifications: Array.from({ length: fullPages }, (_, i) =>
        page(FIRESTORE_BATCH_LIMIT, `p${i}`),
      ),
    });

    const result = await purgeOlderThan(firestore, 'userNotifications', 'createdAt', 90);

    expect(result.truncated).toBe(true);
    expect(result.deleted).toBe(MAX_DELETES_PER_RUN);
  });
});

describe('purgeAlertHistory', () => {
  it('purga cada colección por su campo y su retención', async () => {
    const { firestore, queries } = makeFirestore({
      userNotifications: [page(2, 'n')],
      alertHistory: [page(4, 'h')],
    });

    const result = await purgeAlertHistory(firestore);

    expect(result).toEqual({ notifications: 2, history: 4 });

    const notifQuery = queries.find((q) => q.collection === 'userNotifications');
    const histQuery = queries.find((q) => q.collection === 'alertHistory');
    expect(notifQuery.field).toBe('createdAt');
    expect(histQuery.field).toBe('triggeredAt');

    // El historial es la traza de auditoría: se conserva más que la campana.
    expect(HISTORY_RETENTION_DAYS).toBeGreaterThan(NOTIFICATION_RETENTION_DAYS);
    expect(histQuery.value.getTime()).toBeLessThan(notifQuery.value.getTime());
  });
});

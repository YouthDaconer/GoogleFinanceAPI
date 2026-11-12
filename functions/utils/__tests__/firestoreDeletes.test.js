const { deleteInSmallBatches, DELETE_BATCH_SIZE } = require('../firestoreDeletes');

function fakeDb({ maxPerCommit = Infinity } = {}) {
  const deleted = [];
  const commits = [];
  return {
    deleted,
    commits,
    batch: () => {
      const pending = [];
      return {
        delete: (ref) => pending.push(ref),
        commit: async () => {
          if (pending.length > maxPerCommit) throw new Error('3 INVALID_ARGUMENT: Transaction too big. Decrease transaction size.');
          commits.push(pending.length);
          deleted.push(...pending);
        },
      };
    },
  };
}

const refs = (n) => Array.from({ length: n }, (_, i) => ({ id: `s${i}` }));

test('borra en lotes pequeños: 261 snapshots no van en una sola escritura', async () => {
  const db = fakeDb();
  await expect(deleteInSmallBatches(db, refs(261))).resolves.toBe(261);
  expect(db.deleted).toHaveLength(261);
  expect(Math.max(...db.commits)).toBeLessThanOrEqual(DELETE_BATCH_SIZE);
});

test('si un lote es demasiado grande, borra sus documentos uno a uno', async () => {
  const db = fakeDb({ maxPerCommit: 1 });
  await expect(deleteInSmallBatches(db, refs(25))).resolves.toBe(25);
  expect(db.deleted.map((r) => r.id)).toEqual(refs(25).map((r) => r.id));
});

test('otros errores no se esconden', async () => {
  const db = { batch: () => ({ delete: () => {}, commit: async () => { throw new Error('PERMISSION_DENIED'); } }) };
  await expect(deleteInSmallBatches(db, refs(3))).rejects.toThrow('PERMISSION_DENIED');
});

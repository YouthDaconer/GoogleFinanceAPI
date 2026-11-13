/**
 * Firestore en memoria para los simuladores de la corrección nocturna.
 *
 * Salió del simulador de noches de la HU 6.8.1 (`reconcileTramos.test.js`)
 * cuando la HU 6.8.2 lo necesitó para los tests de la cola y los intentos: una
 * sola copia. Cubre lo que usan la corrección, el motor y la marca: `set` con
 * merge, `update` con rutas, centinelas (`delete`, `increment`), lotes,
 * transacciones, consultas con `where`/`orderBy`/`limit`/`select` y un reloj
 * simulado en el que cada commit cuesta `clock.commitMs`.
 *
 * Uso en un test:
 *   const { clock: mockClock, store: mockStore, db: mockDb } = require('./support/firestoreEnMemoria');
 *   jest.mock('firebase-admin/firestore', () => require('./support/firestoreEnMemoria').firestoreModule);
 *
 * @see platform-docs/stories/6.8.2-nadie-bloquea-correccion-nocturna/refinamiento.md (T11)
 */

const memClock = { ms: 0, commitMs: 1000 };
const memStore = { docs: new Map(), failCommit: null, afterCommit: null, writes: [] };

function memIsOp(v, op) {
  return v !== null && typeof v === 'object' && v.__op === op;
}
function memIsMap(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date) && !v.__op && !v.__ref;
}
function memClone(v) {
  if (v === undefined || v === null || typeof v !== 'object') return v;
  if (v instanceof Date) return new Date(v.getTime());
  if (Array.isArray(v)) return v.map(memClone);
  return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, memClone(x)]));
}
function memField(data, field) {
  return field.split('.').reduce((node, k) => (node == null ? undefined : node[k]), data);
}

/** `set` con merge: los mapas se mezclan campo a campo; los centinelas se aplican. */
function memMerge(target, patch) {
  for (const [k, v] of Object.entries(patch)) {
    if (memIsOp(v, 'delete')) delete target[k];
    else if (memIsOp(v, 'increment')) target[k] = (typeof target[k] === 'number' ? target[k] : 0) + v.n;
    else if (memIsOp(v, 'serverTimestamp')) target[k] = new Date(memClock.ms);
    else if (memIsMap(v)) {
      if (!memIsMap(target[k])) target[k] = {};
      memMerge(target[k], v);
    } else target[k] = memClone(v);
  }
  return target;
}

/** `update`: rutas con punto; un mapa reemplaza el campo entero. */
function memUpdate(target, patch) {
  for (const [key, v] of Object.entries(patch)) {
    const parts = key.split('.');
    const leaf = parts.pop();
    let node = target;
    for (const p of parts) {
      if (!memIsMap(node[p])) node[p] = {};
      node = node[p];
    }
    if (memIsMap(v)) node[leaf] = memMerge({}, v);
    else memMerge(node, { [leaf]: v });
  }
}

/** Aplica `ops` de forma atómica: o todas o ninguna. Cada commit cuesta `commitMs`. */
async function memCommit(ops) {
  memClock.ms += memClock.commitMs;
  if (memStore.failCommit && memStore.failCommit(ops)) throw new Error('commit forzado a fallar (prueba)');
  const staged = new Map();
  const current = (path) => (staged.has(path) ? staged.get(path) : memClone(memStore.docs.get(path)));
  for (const op of ops) {
    if (op.type === 'delete') staged.set(op.path, undefined);
    else if (op.type === 'set') {
      staged.set(op.path, op.opts && op.opts.merge ? memMerge(current(op.path) || {}, op.data) : memMerge({}, op.data));
    } else {
      const base = current(op.path);
      if (base === undefined) throw new Error(`5 NOT_FOUND: ${op.path}`);
      memUpdate(base, op.data);
      staged.set(op.path, base);
    }
  }
  for (const [path, data] of staged) {
    if (data === undefined) memStore.docs.delete(path);
    else memStore.docs.set(path, data);
    memStore.writes.push(path);
  }
  if (memStore.afterCommit) await memStore.afterCommit(ops);
}

function memSnap(path) {
  const data = memStore.docs.get(path);
  return {
    id: path.slice(path.lastIndexOf('/') + 1),
    ref: memDocRef(path),
    exists: data !== undefined,
    data: () => memClone(data),
    get: (f) => memClone(memField(data, f)),
  };
}

function memDocRef(path) {
  return {
    __ref: true,
    path,
    id: path.slice(path.lastIndexOf('/') + 1),
    collection: (name) => memQuery(`${path}/${name}`),
    get: async () => memSnap(path),
    set: (data, opts) => memCommit([{ type: 'set', path, data, opts }]),
    update: (data) => memCommit([{ type: 'update', path, data }]),
    delete: () => memCommit([{ type: 'delete', path }]),
  };
}

function memMatches(path, data, [field, op, value]) {
  const a = field === '__name__' ? path.slice(path.lastIndexOf('/') + 1) : memField(data, field);
  const b = value && value.__ref ? value.id : value;
  switch (op) {
    case '==': return a === b;
    case '!=': return a !== undefined && a !== b;
    case '<': return a !== undefined && a < b;
    case '<=': return a !== undefined && a <= b;
    case '>': return a !== undefined && a > b;
    case '>=': return a !== undefined && a >= b;
    case 'in': return b.includes(a);
    default: throw new Error(`op ${op}`);
  }
}

function memQuery(path, filters = [], order = null, max = null) {
  const query = {
    where: (f, op, v) => memQuery(path, [...filters, [f, op, v]], order, max),
    orderBy: (f, dir = 'asc') => memQuery(path, filters, [f, dir], max),
    limit: (n) => memQuery(path, filters, order, n),
    select: () => query,
    doc: (id) => memDocRef(`${path}/${id}`),
    get: async () => {
      let entries = [...memStore.docs.entries()]
        .filter(([p]) => p.slice(0, p.lastIndexOf('/')) === path)
        .filter(([p, d]) => filters.every((f) => memMatches(p, d, f)));
      if (order) {
        const [f, dir] = order;
        entries = entries
          .filter(([, d]) => memField(d, f) !== undefined)
          .sort(([, x], [, y]) => {
            const a = memField(x, f);
            const b = memField(y, f);
            return a < b ? -1 : a > b ? 1 : 0;
          });
        if (dir === 'desc') entries.reverse();
      }
      if (max !== null) entries = entries.slice(0, max);
      const docs = entries.map(([p]) => memSnap(p));
      return { docs, size: docs.length, empty: docs.length === 0, forEach: (fn) => docs.forEach(fn) };
    },
  };
  return query;
}

const memDb = {
  collection: (name) => memQuery(name),
  batch: () => {
    const ops = [];
    const batch = {
      set: (ref, data, opts) => { ops.push({ type: 'set', path: ref.path, data, opts }); return batch; },
      update: (ref, data) => { ops.push({ type: 'update', path: ref.path, data }); return batch; },
      delete: (ref) => { ops.push({ type: 'delete', path: ref.path }); return batch; },
      commit: () => memCommit(ops),
    };
    return batch;
  },
  runTransaction: async (fn) => {
    const ops = [];
    const tx = {
      get: async (ref) => memSnap(ref.path),
      set: (ref, data, opts) => ops.push({ type: 'set', path: ref.path, data, opts }),
      update: (ref, data) => ops.push({ type: 'update', path: ref.path, data }),
      delete: (ref) => ops.push({ type: 'delete', path: ref.path }),
    };
    const result = await fn(tx);
    if (ops.length > 0) await memCommit(ops);
    return result;
  },
  getAll: async (...args) => args.filter((a) => a && a.__ref).map((r) => memSnap(r.path)),
};

/** Lo que `firebase-admin/firestore` exporta y usan los módulos bajo prueba. */
const firestoreModule = {
  getFirestore: () => memDb,
  FieldValue: {
    delete: () => ({ __op: 'delete' }),
    increment: (n) => ({ __op: 'increment', n }),
    serverTimestamp: () => ({ __op: 'serverTimestamp' }),
  },
  FieldPath: { documentId: () => '__name__' },
};

module.exports = {
  clock: memClock,
  store: memStore,
  db: memDb,
  clone: memClone,
  field: memField,
  isMap: memIsMap,
  firestoreModule,
};

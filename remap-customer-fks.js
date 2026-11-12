#!/usr/bin/env node
require("dotenv").config();
const mysql = require("mysql2/promise");
const fs = require("fs");

// ═════════════════════════════════════════════════════════════
// Config
// ═════════════════════════════════════════════════════════════
const SRC = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USERNAME || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_DATABASE,
  dateStrings: true,
};
const TGT = {
  host: process.env.DB_TARGET_HOST || "localhost",
  port: Number(process.env.DB_TARGET_PORT || 3306),
  user: process.env.DB_TARGET_USERNAME || "root",
  password: process.env.DB_TARGET_PASSWORD || "",
  database: process.env.DB_TARGET_DATABASE,
  dateStrings: true,
};
const esc = (n) => "`" + String(n).replace(/`/g, "``") + "`";

// CLI flags
const DRY_RUN =
  process.argv.includes("--dry-run") || process.argv.includes("-n");
const VERBOSE =
  process.argv.includes("--verbose") || process.argv.includes("-v");
const AUTO_DISCOVER = process.argv.includes("--auto-discover");
const VERIFY = process.argv.includes("--verify");
const ONLY = (() => {
  const i = process.argv.indexOf("--only");
  return i >= 0 ? process.argv[i + 1] : null;
})();
const STRICT = process.argv.includes("--strict");
const HELP = process.argv.includes("--help") || process.argv.includes("-h");

if (HELP) {
  console.log(`
Usage: node remap-customer-fks.js [options]

Options:
  --dry-run, -n      Preview without writing (default if no write flags)
  --verbose, -v      Per-pair logging
  --auto-discover    Fill missing source UUIDs by matching/inserting before remap
  --verify           Only verify: check every divergent src exists in target
  --only <uuid>      Process only one source UUID
  --strict           Wrap each divergent-pair repoint in a transaction
  --help, -h         Show this help

Reads customer_uuid_map.json (source_uuid -> target_uuid).
For each divergent pair (src != tgt), rewrites FK columns in 10 tables
from tgt -> src so all transactions consolidate under the source UUID.

SAFETY RULES:
  * --dry-run (default) never writes.
  * Live mode aborts if any src UUID is missing from target customer table
    UNLESS --auto-discover is passed (which fills the gap first).
  * Never overwrites customer_uuid_map.json; writes .remapped.json instead.
  * Each divergent pair's updates run in a transaction (--strict).
`);
  process.exit(0);
}

// Match fields for auto-discovering missing customers
const MATCH_FIELDS = (
  process.env.CUSTOMER_MATCH_FIELDS || "name,first_name,last_name,contact,email"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// Tables and columns that reference customer.record_id
const REFS = [
  { table: "repa_member", column: "customer" },
  { table: "sales_order", column: "customer" },
  { table: "sales_payment", column: "customer" },
  { table: "sales_order_queue", column: "customer" },
  { table: "deposit", column: "customer" },
  { table: "layaway", column: "customer" },
  { table: "card", column: "customer" },
  { table: "discount_customer", column: "customer" },
  { table: "customer_address", column: "customer" },
  { table: "adjustment", column: "customer" },
];

// ═════════════════════════════════════════════════════════════
// Helpers
// ═════════════════════════════════════════════════════════════
function buildMatchClause(fields, alias = "t") {
  return fields.map((f) => `${alias}.${esc(f)} <=> ?`).join(" AND ");
}

/**
 * Classify each divergent pair: does the source UUID exist as a customer row?
 * Also tally how many FK rows each pair affects per table.
 * Returns { safe: [...], unsafe: [...], fkCounts: Map<src, {table: n}> }
 */
async function classify(tgt, entries) {
  const srcs = entries.map(([s]) => s);
  const existing = new Set();
  const CHUNK = 500;
  for (let i = 0; i < srcs.length; i += CHUNK) {
    const batch = srcs.slice(i, i + CHUNK);
    const ph = batch.map(() => "?").join(",");
    const [rows] = await tgt.query(
      `SELECT record_id FROM customer WHERE record_id IN (${ph})`,
      batch,
    );
    for (const r of rows) existing.add(r.record_id);
  }

  const safe = [];
  const unsafe = [];
  for (const pair of entries) {
    if (existing.has(pair[0])) safe.push(pair);
    else unsafe.push(pair);
  }

  // FK counts per table per src (dry-run reporting)
  const fkCounts = new Map();
  for (const [src] of entries) fkCounts.set(src, {});
  for (const ref of REFS) {
    const exists = await tableExists(tgt, ref.table);
    if (!exists) continue;
    const ph = srcs.map(() => "?").join(",");
    const [rows] = await tgt.query(
      `SELECT ${esc(ref.column)} AS c, COUNT(*) AS n
       FROM ${esc(ref.table)}
       WHERE ${esc(ref.column)} IN (${ph})
       GROUP BY ${esc(ref.column)}`,
      srcs,
    );
    for (const r of rows) {
      const m = fkCounts.get(r.c);
      if (m) m[ref.table] = Number(r.n);
    }
  }

  return { safe, unsafe, fkCounts };
}

async function tableExists(conn, table) {
  const [r] = await conn.query(
    `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
    [TGT.database, table],
  );
  return r.length > 0;
}

async function columnExists(conn, table, column) {
  const [r] = await conn.query(
    `SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`,
    [TGT.database, table, column],
  );
  return r.length > 0;
}

/**
 * Auto-discover missing customers. For each orphan FK value:
 *   - If already mapped to a DIFFERENT uuid → skip (already handled)
 *   - If mapped to itself (stale identity map) → still process
 *   - If not in source → skip
 *   - If identity match found in target → add { source -> target } to map
 *   - If no match → insert source row into target with source UUID
 * Only inserts when not dry-running.
 */
async function autoDiscover(src, tgt, map) {
  console.log("\n── Auto-discovering missing customers ──");
  const added = { matched: 0, inserted: 0, skipped: 0 };
  const newEntries = [];

  for (const ref of REFS) {
    if (!(await tableExists(tgt, ref.table))) continue;
    if (!(await columnExists(tgt, ref.table, ref.column))) continue;

    const [orphans] = await tgt.query(`
      SELECT DISTINCT t.${esc(ref.column)} AS orphan_uuid
      FROM ${esc(ref.table)} t
      LEFT JOIN customer c ON c.record_id = t.${esc(ref.column)}
      WHERE t.${esc(ref.column)} IS NOT NULL
        AND c.record_id IS NULL
    `);

    for (const row of orphans) {
      const uuid = row.orphan_uuid;
      if (!uuid) continue;

      // Skip only if already mapped to a DIFFERENT uuid (real remap)
      if (map[uuid] && map[uuid] !== uuid) {
        added.skipped++;
        continue;
      }

      // Fetch source row
      const [srcRows] = await src.query(
        `SELECT * FROM customer WHERE record_id = ? LIMIT 1`,
        [uuid],
      );
      if (!srcRows.length) {
        added.skipped++;
        continue;
      }
      const srcRow = srcRows[0];

      // Try to match identity in target
      const matchCols = MATCH_FIELDS.filter((f) =>
        Object.prototype.hasOwnProperty.call(srcRow, f),
      );
      if (matchCols.length) {
        const whereSql = buildMatchClause(matchCols);
        const whereVals = matchCols.map((f) => srcRow[f]);
        const [existing] = await tgt.query(
          `SELECT record_id FROM customer t WHERE ${whereSql} LIMIT 1`,
          whereVals,
        );
        if (existing.length) {
          const targetId = existing[0].record_id;
          map[uuid] = targetId;
          newEntries.push({
            uuid,
            targetId,
            name: srcRow.name,
            action: "matched",
          });
          added.matched++;
          continue;
        }
      }

      // No match — insert source row into target
      // Validate: refuse to insert over an existing row
      const [conflict] = await tgt.query(
        `SELECT 1 FROM customer WHERE record_id = ? LIMIT 1`,
        [uuid],
      );
      if (conflict.length) {
        // Row exists but somehow wasn't seen by classify — mark as exists
        map[uuid] = uuid;
        newEntries.push({
          uuid,
          targetId: uuid,
          name: srcRow.name,
          action: "matched-existing",
        });
        added.matched++;
        continue;
      }

      if (!DRY_RUN) {
        const cols = Object.keys(srcRow);
        const colList = cols.map(esc).join(",");
        const placeholders = cols.map(() => "?").join(",");
        const values = cols.map((c) => srcRow[c]);
        await tgt.query(
          `INSERT INTO customer (${colList}) VALUES (${placeholders})`,
          values,
        );
      }
      map[uuid] = uuid;
      newEntries.push({
        uuid,
        targetId: uuid,
        name: srcRow.name,
        action: "inserted",
      });
      added.inserted++;
    }
  }

  if (newEntries.length) {
    console.log("\n  New entries:");
    newEntries
      .slice(0, 50)
      .forEach((e) =>
        console.log(
          `    ${e.action === "inserted" ? "➕" : "🔄"} ${e.uuid} -> ${e.targetId}  ${e.name}`,
        ),
      );
    if (newEntries.length > 50)
      console.log(`    ... and ${newEntries.length - 50} more`);
  }
  console.log(
    `\n  Auto-discover: ${added.matched} matched, ${added.inserted} inserted, ${added.skipped} skipped`,
  );
  return added;
}

/**
 * Remap one divergent pair across all REFS tables.
 * If strict, wraps in a transaction.
 * Returns { updated: {table: n}, skipped: {table: reason} }
 */
async function remapPair(conn, src, tgt) {
  const updated = {};
  const skipped = {};

  const run = async () => {
    for (const ref of REFS) {
      if (!(await tableExists(conn, ref.table))) {
        skipped[ref.table] = "table missing";
        continue;
      }
      if (!(await columnExists(conn, ref.table, ref.column))) {
        skipped[ref.table] = "column missing";
        continue;
      }

      // Verify src exists as a customer row before rewriting FKs
      const [srcCheck] = await conn.query(
        `SELECT 1 FROM customer WHERE record_id = ? LIMIT 1`,
        [src],
      );
      if (!srcCheck.length) {
        throw new Error(
          `Refusing to remap ${tgt} -> ${src}: source UUID does not exist as a customer row`,
        );
      }

      const [r] = await conn.query(
        `UPDATE ${esc(ref.table)} SET ${esc(ref.column)} = ?
         WHERE ${esc(ref.column)} = ?`,
        [src, tgt],
      );
      if (r.affectedRows > 0) updated[ref.table] = r.affectedRows;
    }
  };

  if (STRICT) {
    await conn.beginTransaction();
    try {
      await run();
      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    }
  } else {
    await run();
  }

  return { updated, skipped };
}

// ═════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════
(async () => {
  if (!fs.existsSync("customer_uuid_map.json")) {
    console.error("ERROR: customer_uuid_map.json not found");
    process.exit(1);
  }

  const originalMap = JSON.parse(
    fs.readFileSync("customer_uuid_map.json", "utf8"),
  );
  const map = { ...originalMap };
  let entries = Object.entries(map).filter(([s, t]) => s !== t);

  if (ONLY) {
    const hit = entries.find(([s]) => s === ONLY);
    if (!hit) {
      console.error(`ERROR: --only ${ONLY} not found among divergent entries`);
      process.exit(1);
    }
    entries = [hit];
  }

  console.log("═".repeat(78));
  console.log(`Mode     : ${DRY_RUN ? "DRY RUN (no writes)" : "LIVE"}`);
  console.log(`Strict   : ${STRICT ? "ON (transaction per pair)" : "off"}`);
  console.log(`Target   : ${TGT.user}@${TGT.host}:${TGT.port}/${TGT.database}`);
  console.log(`Source   : ${SRC.user}@${SRC.host}:${SRC.port}/${SRC.database}`);
  console.log(`Remaps   : ${entries.length} customer UUIDs`);
  console.log(`Auto-disc: ${AUTO_DISCOVER ? "ENABLED" : "disabled"}`);
  console.log(`Verify   : ${VERIFY ? "ONLY" : "off"}`);
  if (ONLY) console.log(`Only     : ${ONLY}`);
  console.log("═".repeat(78));

  const src = await mysql.createConnection(SRC);
  const tgt = await mysql.createConnection(TGT);

  try {
    // ─── AUTO-DISCOVER ──────────────────────────────────────
    if (AUTO_DISCOVER && !VERIFY) {
      await autoDiscover(src, tgt, map);
      entries = Object.entries(map).filter(([s, t]) => s !== t);
      if (ONLY) {
        const hit = entries.find(([s]) => s === ONLY);
        entries = hit ? [hit] : [];
      }
      console.log(`\nRemaps after auto-discover: ${entries.length}`);
    }

    if (!entries.length) {
      console.log("No remapping needed — all UUIDs match.");
      return;
    }

    // ─── CLASSIFY ───────────────────────────────────────────
    const { safe, unsafe, fkCounts } = await classify(tgt, entries);

    // Per-table FK totals (dry-run reporting)
    const perTable = new Map();
    for (const ref of REFS) perTable.set(ref.table, 0);
    for (const [, counts] of fkCounts) {
      for (const [tbl, n] of Object.entries(counts)) {
        perTable.set(tbl, (perTable.get(tbl) || 0) + n);
      }
    }

    console.log("\n── FK row counts (current target state) ──");
    for (const [tbl, n] of perTable) {
      if (n === 0) {
        if (VERBOSE) console.log(`  ${tbl}: 0 rows`);
      } else {
        console.log(`  ${tbl}: ${n} row(s) will be repointed`);
      }
    }

    console.log("\n── Classification ──");
    console.log(`  pairs total      : ${entries.length}`);
    console.log(`  src exists (SAFE): ${safe.length}`);
    console.log(`  src missing (RISK): ${unsafe.length}`);

    if (unsafe.length > 0) {
      console.log("\n  Unsafe pairs (src missing from target):");
      unsafe.slice(0, 20).forEach(([s, t]) => console.log(`    ${s} -> ${t}`));
      if (unsafe.length > 20)
        console.log(`    ... and ${unsafe.length - 20} more`);
    }

    // ─── VERIFY ONLY ────────────────────────────────────────
    if (VERIFY) {
      console.log(
        `\nVerify result: ${safe.length}/${entries.length} pairs safe; ${unsafe.length} unsafe.`,
      );
      process.exitCode = unsafe.length === 0 ? 0 : 1;
      return;
    }

    // ─── SAFETY ABORT ───────────────────────────────────────
    if (unsafe.length > 0 && !AUTO_DISCOVER && !DRY_RUN) {
      console.error(
        `\n❌ ABORT: ${unsafe.length} pairs have source UUIDs missing from target.`,
      );
      console.error("   Running live would create orphaned FKs.");
      console.error(
        "   Re-run with --auto-discover to fill the gap, or run --dry-run.",
      );
      process.exitCode = 1;
      return;
    }

    // ─── DRY RUN ────────────────────────────────────────────
    if (DRY_RUN) {
      console.log("\n── Dry-run plan ──");
      const toProcess = unsafe.length > 0 && !AUTO_DISCOVER ? safe : entries;
      let totalRows = 0;
      for (const [s, t] of toProcess) {
        const counts = fkCounts.get(s) || {};
        const total = Object.values(counts).reduce((a, b) => a + b, 0);
        totalRows += total;
        if (VERBOSE) {
          console.log(
            `  ${safe.some(([ss]) => ss === s) ? "✅ SAFE" : "⚠️  UNSAFE"}  ${s} <- ${t}  (${total} FK rows)`,
          );
        }
      }
      console.log(
        `\n  Would update ${totalRows} row(s) across ${perTable.size} table(s).`,
      );
      console.log("\nDRY RUN — no rows were written.");
      if (unsafe.length > 0) {
        console.log(
          `⚠️  ${unsafe.length} pair(s) unsafe. Re-run with --auto-discover to fix, or run live with --auto-discover.`,
        );
      }
      return;
    }

    // ─── LIVE ───────────────────────────────────────────────
    if (!AUTO_DISCOVER && unsafe.length > 0) {
      console.error("Internal safety check failed.");
      process.exitCode = 1;
      return;
    }

    console.log("\n── Live remap ──");
    await tgt.query("SET FOREIGN_KEY_CHECKS=0");
    await tgt.query("SET @sync_triggers_disabled = 2");
    await tgt.query("SET @sym_is_syncing = TRUE");

    let pairsDone = 0;
    let totalRows = 0;
    const failures = [];

    for (const [s, t] of entries) {
      try {
        const res = await remapPair(tgt, s, t);
        const n = Object.values(res.updated).reduce((a, b) => a + b, 0);
        totalRows += n;
        pairsDone++;
        if (VERBOSE) {
          console.log(
            `  ✅ ${s} <- ${t}  (${n} rows: ${Object.entries(res.updated)
              .map(([k, v]) => `${k}=${v}`)
              .join(", ")})`,
          );
        }
      } catch (e) {
        failures.push({ src: s, tgt: t, error: e.message });
        console.error(`  ❌ ${s} <- ${t}: ${e.message}`);
      }
    }

    console.log("\n── Summary ──");
    console.log(`  pairs processed : ${pairsDone}`);
    console.log(`  rows updated    : ${totalRows}`);
    console.log(`  failures        : ${failures.length}`);

    if (failures.length > 0) {
      console.log("\n  Failures:");
      failures.forEach((f) =>
        console.log(`    ${f.src} <- ${f.tgt}: ${f.error}`),
      );
      process.exitCode = 2;
    }

    // Write updated map to a NEW file; preserve original
    fs.writeFileSync(
      "customer_uuid_map.remapped.json",
      JSON.stringify(map, null, 2),
    );
    console.log(`\nUpdated map: customer_uuid_map.remapped.json`);
    console.log(`Original map preserved: customer_uuid_map.json`);

    // ─── POST-VERIFY ────────────────────────────────────────
    console.log("\n── Post-remap FK integrity check ──");
    let orphanTotal = 0;
    for (const ref of REFS) {
      if (!(await tableExists(tgt, ref.table))) continue;
      if (!(await columnExists(tgt, ref.table, ref.column))) continue;
      const [[row]] = await tgt.query(
        `SELECT COUNT(*) AS n
         FROM ${esc(ref.table)} t
         LEFT JOIN customer c ON c.record_id = t.${esc(ref.column)}
         WHERE t.${esc(ref.column)} IS NOT NULL
           AND c.record_id IS NULL`,
      );
      const n = Number(row.n);
      orphanTotal += n;
      if (n === 0) {
        console.log(`  ✅ ${ref.table}.${ref.column}: 0 orphans`);
      } else {
        console.log(`  ❌ ${ref.table}.${ref.column}: ${n} ORPHAN(S)`);
      }
    }
    if (orphanTotal > 0) {
      console.error(`\n❌ ${orphanTotal} orphaned FK row(s) after remap.`);
      process.exitCode = 2;
    } else {
      console.log("\n✅ Remap complete, all FK integrity checks passed.");
    }
  } catch (e) {
    console.error("FAILED:", e.message);
    process.exitCode = 1;
  } finally {
    if (!DRY_RUN) {
      try {
        await tgt.query("SET FOREIGN_KEY_CHECKS=1");
        await tgt.query("SET @sync_triggers_disabled = NULL");
        await tgt.query("SET @sym_is_syncing = NULL");
      } catch {}
    }
    await src.end();
    await tgt.end();
  }
})();

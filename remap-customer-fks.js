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

const DRY_RUN =
  process.argv.includes("--dry-run") || process.argv.includes("-n");
const VERBOSE =
  process.argv.includes("--verbose") || process.argv.includes("-v");
const AUTO_DISCOVER = process.argv.includes("--auto-discover");

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
 * Discover customers missing from target and add them to the map.
 *
 * For each orphan FK value:
 *   1. If already mapped to a DIFFERENT uuid → skip (already handled)
 *   2. If mapped to itself (stale identity map) → still process
 *   3. If not in source → skip (can't help)
 *   4. If identity match found in target → add { source -> target } to map
 *   5. If no match → insert source row into target with source UUID
 */
async function autoDiscover(src, tgt, map) {
  console.log("\n── Auto-discovering missing customers ──");
  const added = { matched: 0, inserted: 0 };
  const newEntries = [];

  for (const ref of REFS) {
    const [tbl] = await tgt.query(
      `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
      [TGT.database, ref.table],
    );
    if (!tbl.length) continue;

    const [col] = await tgt.query(
      `SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`,
      [TGT.database, ref.table, ref.column],
    );
    if (!col.length) continue;

    // Orphaned FK values in this table
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

      // Skip only if already mapped to a DIFFERENT uuid (real remap).
      // Self-maps (uuid -> uuid) still get processed because the target
      // row doesn't exist — the identity map is stale.
      if (map[uuid] && map[uuid] !== uuid) continue;

      // Fetch source row
      const [srcRows] = await src.query(
        `SELECT * FROM customer WHERE record_id = ? LIMIT 1`,
        [uuid],
      );
      if (!srcRows.length) continue;
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
      if (!DRY_RUN) {
        const cols = Object.keys(srcRow);
        const colList = cols.map(esc).join(",");
        const placeholders = cols.map(() => "?").join(",");
        const values = cols.map((c) => srcRow[c]);
        const updateCols = cols.filter((c) => c !== "record_id");
        const updateClause = updateCols.length
          ? ` ON DUPLICATE KEY UPDATE ${updateCols.map((c) => `${esc(c)} = VALUES(${esc(c)})`).join(", ")}`
          : "";
        await tgt.query(
          `INSERT INTO customer (${colList}) VALUES (${placeholders})${updateClause}`,
          values,
        );
      }
      map[uuid] = uuid; // identity — the inserted row uses this UUID
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
    newEntries.forEach((e) =>
      console.log(
        `    ${e.action === "matched" ? "🔄" : "➕"} ${e.uuid} -> ${e.targetId}  ${e.name}`,
      ),
    );
  }
  console.log(
    `\n  Auto-discover results: ${added.matched} matched, ${added.inserted} inserted`,
  );

  return added;
}

// ═════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════
(async () => {
  if (!fs.existsSync("customer_uuid_map.json")) {
    console.error("ERROR: customer_uuid_map.json not found");
    process.exit(1);
  }

  const map = JSON.parse(fs.readFileSync("customer_uuid_map.json", "utf8"));
  let entries = Object.entries(map).filter(([s, t]) => s !== t);

  console.log("═".repeat(78));
  console.log(`Mode     : ${DRY_RUN ? "DRY RUN (no writes)" : "LIVE"}`);
  console.log(`Target   : ${TGT.user}@${TGT.host}:${TGT.port}/${TGT.database}`);
  console.log(`Source   : ${SRC.user}@${SRC.host}:${SRC.port}/${SRC.database}`);
  console.log(`Remaps   : ${entries.length} customer UUIDs (initial)`);
  console.log(`Auto-disc: ${AUTO_DISCOVER ? "ENABLED" : "disabled"}`);
  console.log("═".repeat(78));

  const src = await mysql.createConnection(SRC);
  const tgt = await mysql.createConnection(TGT);

  try {
    // Auto-discover if requested
    if (AUTO_DISCOVER) {
      await autoDiscover(src, tgt, map);
      // Re-read entries from updated map
      entries = Object.entries(map).filter(([s, t]) => s !== t);
      console.log(
        `\nRemaps   : ${entries.length} customer UUIDs (after auto-discover)`,
      );
    }

    if (!entries.length) {
      console.log("No remapping needed — all UUIDs match.");
      return;
    }

    if (!DRY_RUN) {
      await tgt.query("SET FOREIGN_KEY_CHECKS=0");
      await tgt.query("SET @sync_triggers_disabled = 2");
      await tgt.query("SET @sym_is_syncing = TRUE");
    }

    let tablesUpdated = 0;
    let totalRows = 0;

    for (const ref of REFS) {
      const [tbl] = await tgt.query(
        `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
        [TGT.database, ref.table],
      );
      if (!tbl.length) {
        if (VERBOSE)
          console.log(`  ${ref.table}.${ref.column}: skipped (table missing)`);
        continue;
      }
      const [col] = await tgt.query(
        `SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`,
        [TGT.database, ref.table, ref.column],
      );
      if (!col.length) {
        if (VERBOSE)
          console.log(`  ${ref.table}.${ref.column}: skipped (column missing)`);
        continue;
      }

      const oldIds = entries.map(([old]) => old);
      const ph = oldIds.map(() => "?").join(",");
      const [[{ n }]] = await tgt.query(
        `SELECT COUNT(*) AS n FROM ${esc(ref.table)} WHERE ${esc(ref.column)} IN (${ph})`,
        oldIds,
      );

      if (n === 0) {
        if (VERBOSE) console.log(`  ${ref.table}.${ref.column}: 0 rows`);
        continue;
      }

      if (DRY_RUN) {
        console.log(
          `  ${ref.table}.${ref.column}: [dry-run] would update ${n} row(s)`,
        );
        tablesUpdated++;
        totalRows += Number(n);
        continue;
      }

      const BATCH = 200;
      let updated = 0;
      for (let i = 0; i < entries.length; i += BATCH) {
        const batch = entries.slice(i, i + BATCH);
        const caseSql =
          "CASE " +
          esc(ref.column) +
          " " +
          batch.map(() => `WHEN ? THEN ?`).join(" ") +
          " ELSE " +
          esc(ref.column) +
          " END";
        const params = batch.flat();
        const inList = batch.map(() => "?").join(",");
        params.push(...batch.map(([old]) => old));

        const [r] = await tgt.query(
          `UPDATE ${esc(ref.table)} SET ${esc(ref.column)} = ${caseSql}
           WHERE ${esc(ref.column)} IN (${inList})`,
          params,
        );
        updated += r.affectedRows;
      }
      console.log(`  ${ref.table}.${ref.column}: ${updated} row(s) updated`);
      tablesUpdated++;
      totalRows += updated;
    }

    console.log("\n── Summary ──");
    console.log(`  tables touched : ${tablesUpdated}`);
    console.log(`  rows ${DRY_RUN ? "to update" : "updated"} : ${totalRows}`);

    if (DRY_RUN) {
      console.log("\nDRY RUN — no rows were written.");
      console.log("Rerun without --dry-run to apply.");
    } else {
      // Save updated map AND archive a copy (do not rename — preserve original)
      fs.writeFileSync("customer_uuid_map.json", JSON.stringify(map, null, 2));
      fs.copyFileSync(
        "customer_uuid_map.json",
        "customer_uuid_map.applied.json",
      );
      console.log(
        "\nMap saved as customer_uuid_map.json (preserved for future runs)",
      );
      console.log("Archive copy: customer_uuid_map.applied.json");
    }
  } catch (e) {
    console.error("FAILED:", e.message);
    process.exitCode = 1;
  } finally {
    if (!DRY_RUN) {
      await tgt.query("SET FOREIGN_KEY_CHECKS=1");
      await tgt.query("SET @sync_triggers_disabled = NULL");
      await tgt.query("SET @sym_is_syncing = NULL");
    }
    await src.end();
    await tgt.end();
  }
})();

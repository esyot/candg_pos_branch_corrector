#!/usr/bin/env node
require("dotenv").config();
const mysql = require("mysql2/promise");
const { randomUUID } = require("crypto");
const fs = require("fs");

// ═════════════════════════════════════════════════════════════
// CLI parsing
// ═════════════════════════════════════════════════════════════
const argv = process.argv.slice(2);
const CLI = {
  dryRun: argv.includes("--dry-run") || argv.includes("-n"),
  verbose: argv.includes("--verbose") || argv.includes("-v"),
  help: argv.includes("--help") || argv.includes("-h"),
};

if (CLI.help) {
  console.log(`
Usage: node migrate-customers-smart.js [options]

Options:
  --dry-run, -n   Preview matches/inserts without writing
  --verbose, -v   Show per-row match decisions
  --help, -h      Show this help

Reads needed_customers.txt (one UUID per line) and for each source customer:
  - If a target customer matches on CUSTOMER_MATCH_FIELDS, reuse it
    (optionally updating CUSTOMER_UPDATE_FIELDS on the target row)
  - Otherwise insert a new row (with new UUID if CUSTOMER_NEW_UUID=true,
    or with source UUID otherwise)

Writes customer_uuid_map.json mapping source UUID -> target UUID
(useful for FK remapping when CUSTOMER_NEW_UUID=true).

Environment (.env):
  DB_*                       Source connection
  DB_TARGET_*                Target connection
  CUSTOMER_MATCH_MODE        match | always  (default: match)
  CUSTOMER_MATCH_FIELDS      comma-separated identity fields
  CUSTOMER_UPDATE_FIELDS     comma-separated fields to refresh on match
  CUSTOMER_NEW_UUID          true | false  (default: false)
`);
  process.exit(0);
}

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

const MATCH_FIELDS = (
  process.env.CUSTOMER_MATCH_FIELDS || "name,first_name,last_name,contact,email"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const UPDATE_FIELDS = (process.env.CUSTOMER_UPDATE_FIELDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const NEW_UUID =
  String(process.env.CUSTOMER_NEW_UUID ?? "false").toLowerCase() === "true";
const MODE = (process.env.CUSTOMER_MATCH_MODE || "match").toLowerCase();

const ALL_COLS = [
  "record_id",
  "code",
  "name",
  "first_name",
  "last_name",
  "middle_name",
  "suffix",
  "address",
  "contact",
  "deposited_amount",
  "email",
  "email_verified_at",
  "created_at",
  "updated_at",
];

/**
 * Build a NULL-safe match clause: name <=> ? AND contact <=> ? etc.
 * MySQL's `<=>` treats NULL <=> NULL as TRUE.
 */
function buildMatchClause(fields, alias = "t") {
  return fields.map((f) => `${alias}.${esc(f)} <=> ?`).join(" AND ");
}

// ═════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════
(async () => {
  if (!fs.existsSync("needed_customers.txt")) {
    console.error("ERROR: needed_customers.txt not found in current directory");
    console.error("Generate it first with:");
    console.error(
      `  mysql -u root -p -N -B -e "SELECT ... " > needed_customers.txt`,
    );
    process.exit(1);
  }

  const ids = fs
    .readFileSync("needed_customers.txt", "utf8")
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);

  if (!ids.length) {
    console.error("ERROR: needed_customers.txt is empty");
    process.exit(1);
  }

  console.log("═".repeat(78));
  console.log(`Mode        : ${CLI.dryRun ? "DRY RUN (no writes)" : "LIVE"}`);
  console.log(
    `Source      : ${SRC.user}@${SRC.host}:${SRC.port}/${SRC.database}`,
  );
  console.log(
    `Target      : ${TGT.user}@${TGT.host}:${TGT.port}/${TGT.database}`,
  );
  console.log(`Customers   : ${ids.length}`);
  console.log(`Match mode  : ${MODE}`);
  console.log(`Match fields: ${MATCH_FIELDS.join(", ")}`);
  console.log(`Update flds : ${UPDATE_FIELDS.join(", ") || "(none)"}`);
  console.log(`New UUID    : ${NEW_UUID}`);
  console.log("═".repeat(78));

  const src = await mysql.createConnection(SRC);
  const tgt = await mysql.createConnection(TGT);
  const uuidMap = {};
  const stats = { matched: 0, inserted: 0, updated: 0, failed: 0, missing: 0 };
  const failures = [];
  const samples = [];

  try {
    if (!CLI.dryRun) {
      await tgt.query("SET FOREIGN_KEY_CHECKS=0");
      await tgt.query("SET @sync_triggers_disabled = 2");
      await tgt.query("SET @sym_is_syncing = TRUE");
    }

    const BATCH = 200;

    for (let i = 0; i < ids.length; i += BATCH) {
      const batch = ids.slice(i, i + BATCH);
      const ph = batch.map(() => "?").join(",");
      const [rows] = await src.query(
        `SELECT ${ALL_COLS.map(esc).join(",")}
         FROM customer WHERE record_id IN (${ph})`,
        batch,
      );

      // Which requested IDs are missing from source?
      const foundIds = new Set(rows.map((r) => r.record_id));
      for (const id of batch) {
        if (!foundIds.has(id)) stats.missing++;
      }

      for (const row of rows) {
        try {
          let targetId = null;
          let action = null;

          if (MODE === "match") {
            const matchCols = MATCH_FIELDS.filter((f) =>
              Object.prototype.hasOwnProperty.call(row, f),
            );

            if (matchCols.length) {
              const whereSql = buildMatchClause(matchCols);
              const whereVals = matchCols.map((f) => row[f]);
              const [existing] = await tgt.query(
                `SELECT ${esc("record_id")} AS id
                 FROM customer t
                 WHERE ${whereSql}
                 LIMIT 1`,
                whereVals,
              );
              if (existing.length) {
                targetId = existing[0].id;
                action = "matched";
              }
            }
          }

          if (targetId) {
            // Match found
            stats.matched++;
            uuidMap[row.record_id] = targetId;

            if (samples.length < 10) {
              samples.push({
                action: "MATCH",
                source_uuid: row.record_id,
                target_uuid: targetId,
                name: row.name,
              });
            }

            if (!CLI.dryRun && UPDATE_FIELDS.length) {
              const validUpdate = UPDATE_FIELDS.filter((f) =>
                Object.prototype.hasOwnProperty.call(row, f),
              );
              if (validUpdate.length) {
                const setClause = validUpdate
                  .map((f) => `${esc(f)} = ?`)
                  .join(", ");
                const setVals = validUpdate.map((f) => row[f]);
                await tgt.query(
                  `UPDATE customer SET ${setClause} WHERE record_id = ?`,
                  [...setVals, targetId],
                );
                stats.updated++;
              }
            }
          } else {
            // No match -> insert
            const newId = NEW_UUID ? randomUUID() : row.record_id;
            targetId = newId;
            uuidMap[row.record_id] = targetId;
            stats.inserted++;

            if (samples.length < 10) {
              samples.push({
                action: "INSERT",
                source_uuid: row.record_id,
                target_uuid: targetId,
                name: row.name,
              });
            }

            if (!CLI.dryRun) {
              const colList = ALL_COLS.map(esc).join(",");
              const placeholders = ALL_COLS.map(() => "?").join(",");
              const values = ALL_COLS.map((c) =>
                c === "record_id" ? newId : row[c],
              );
              const updateCols = ALL_COLS.filter((c) => c !== "record_id");
              const updateClause = updateCols.length
                ? ` ON DUPLICATE KEY UPDATE ${updateCols.map((c) => `${esc(c)} = VALUES(${esc(c)})`).join(", ")}`
                : "";

              await tgt.query(
                `INSERT INTO customer (${colList}) VALUES (${placeholders})${updateClause}`,
                values,
              );
            }
          }

          if (CLI.verbose) {
            console.log(
              `  ${action.toUpperCase()} ${row.record_id} -> ${targetId}  (${row.name})`,
            );
          }
        } catch (e) {
          stats.failed++;
          failures.push({
            record_id: row.record_id,
            name: row.name,
            error: e.message,
          });
          if (CLI.verbose)
            console.error(`  FAIL ${row.record_id}: ${e.message}`);
        }
      }

      const done = Math.min(i + BATCH, ids.length);
      console.log(
        `  [${done}/${ids.length}] matched=${stats.matched} inserted=${stats.inserted} updated=${stats.updated} failed=${stats.failed} missing=${stats.missing}`,
      );
    }

    if (samples.length) {
      console.log("\n── Sample decisions (first 10) ──");
      for (const s of samples) {
        console.log(
          `  ${s.action.padEnd(7)} ${s.source_uuid} -> ${s.target_uuid}  ${s.name}`,
        );
      }
    }

    if (failures.length) {
      console.log("\n── Failures ──");
      failures
        .slice(0, 20)
        .forEach((f) =>
          console.log(`  ${f.record_id} (${f.name}): ${f.error}`),
        );
      if (failures.length > 20) {
        console.log(`  ... and ${failures.length - 20} more`);
      }
    }

    console.log("\n── Summary ──");
    console.log(`  matched  : ${stats.matched}`);
    console.log(`  inserted : ${stats.inserted}`);
    console.log(
      `  updated  : ${stats.updated}${CLI.dryRun ? " (dry-run: skipped)" : ""}`,
    );
    console.log(`  failed   : ${stats.failed}`);
    console.log(`  missing  : ${stats.missing} (requested but not in source)`);
    console.log(
      `  total    : ${stats.matched + stats.inserted + stats.failed + stats.missing} / ${ids.length}`,
    );

    if (CLI.dryRun) {
      console.log("\nDRY RUN — no rows were written.");
      console.log("Rerun without --dry-run to apply.");
      const wantNewUuid = NEW_UUID;
      if (wantNewUuid) {
        const wouldRemap = Object.entries(uuidMap).filter(
          ([s, t]) => s !== t,
        ).length;
        console.log(
          `With CUSTOMER_NEW_UUID=true, ${wouldRemap} FK remaps would be needed.`,
        );
        console.log("Run remap-customer-fks.js after live migration.");
      }
    } else {
      fs.writeFileSync(
        "customer_uuid_map.json",
        JSON.stringify(uuidMap, null, 2),
      );
      console.log(
        `\nMapping saved to customer_uuid_map.json (${Object.keys(uuidMap).length} entries)`,
      );

      const remapNeeded = Object.entries(uuidMap).filter(
        ([s, t]) => s !== t,
      ).length;
      if (remapNeeded > 0) {
        console.log(`⚠ ${remapNeeded} customers got new UUIDs.`);
        console.log("  Run remap-customer-fks.js to remap FK references.");
      } else {
        console.log("✓ No FK remapping needed (all UUIDs preserved).");
      }
    }
  } catch (e) {
    console.error("\nFAILED:", e.message);
    process.exitCode = 1;
  } finally {
    if (!CLI.dryRun) {
      await tgt.query("SET FOREIGN_KEY_CHECKS=1");
      await tgt.query("SET @sync_triggers_disabled = NULL");
      await tgt.query("SET @sym_is_syncing = NULL");
    }
    await src.end();
    await tgt.end();
  }
})();

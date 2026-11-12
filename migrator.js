#!/usr/bin/env node
require("dotenv").config();

const mysql = require("mysql2/promise");
const { randomUUID } = require("crypto");

// ═════════════════════════════════════════════════════════════
// CLI parsing
// ═════════════════════════════════════════════════════════════
const argv = process.argv.slice(2);
const CLI = {
  dryRun: argv.includes("--dry-run") || argv.includes("-n"),
  truncate: argv.includes("--truncate"),
  verbose: argv.includes("--verbose") || argv.includes("-v"),
  help: argv.includes("--help") || argv.includes("-h"),
  force: argv.includes("--force"),
  strict: argv.includes("--strict"),
  skipFkCheck: argv.includes("--skip-fk-check"),
  noAutoResolve: argv.includes("--no-auto-resolve"),
};

if (CLI.help) {
  console.log(`
Usage: node migrator.js [options]

Options:
  --dry-run, -n        Analyze without writing any data
  --truncate           Truncate target tables before inserting
  --verbose, -v        Show per-batch progress and sample rows
  --force              Proceed even if FK pre-flight finds unresolved orphans
  --strict             Abort if ANY FK would be orphaned (no partial migrate)
  --skip-fk-check      Skip the FK pre-flight (faster, riskier)
  --no-auto-resolve    Disable auto-resolution of missing FK references
  --help, -h           Show this help

Environment (.env):
  DB_*                    Source connection
  DB_TARGET_*             Target connection
  TABLES                  Comma-separated list of tables
  BATCH_SIZE              Rows per batch (default: 1000)
  DRY_RUN                 true/false
  TRUNCATE_TARGET         true/false
  DATE_SCOPE              on/off
  DATE_MODE               range | from | to
  DATE_COLUMN             created_at | updated_at
  DATE_FROM               Lower bound
  DATE_TO                 Upper bound
  SUPPRESS_TRIGGERS       true/false
  AUTO_RESOLVE_FKS        true/false — auto-insert missing FK refs
  AUTO_RESOLVE_TABLES     comma-separated parent tables to auto-resolve
  AUTO_RESOLVE_MATCH_FIELDS  identity fields for content matching
  AUTO_RESOLVE_NEW_UUID   true/false — new UUID on insert
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

const OPTIONS = {
  tables: (process.env.TABLES || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean),
  batchSize: Number(process.env.BATCH_SIZE || 1000),
  dryRun: CLI.dryRun || String(process.env.DRY_RUN).toLowerCase() === "true",
  truncateTarget:
    CLI.truncate ||
    String(process.env.TRUNCATE_TARGET).toLowerCase() === "true",
  suppressTriggers:
    String(process.env.SUPPRESS_TRIGGERS).toLowerCase() === "true",
  verbose: CLI.verbose,
  force: CLI.force,
  strict: CLI.strict,
  skipFkCheck: CLI.skipFkCheck,
  autoResolveFks:
    !CLI.noAutoResolve &&
    String(process.env.AUTO_RESOLVE_FKS).toLowerCase() === "true",
  autoResolveTables: (process.env.AUTO_RESOLVE_TABLES || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  autoResolveMatchFields: (
    process.env.AUTO_RESOLVE_MATCH_FIELDS ||
    "name,first_name,last_name,contact,email"
  )
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
  autoResolveNewUuid:
    String(process.env.AUTO_RESOLVE_NEW_UUID ?? "false").toLowerCase() ===
    "true",
};

// ═════════════════════════════════════════════════════════════
// Date scope
// ═════════════════════════════════════════════════════════════
const DATE_SCOPE = (() => {
  const enabled = String(process.env.DATE_SCOPE ?? "on").toLowerCase();
  if (enabled === "off" || enabled === "false") return null;

  const from = process.env.DATE_FROM?.trim() || "2026-10-20";
  const to = process.env.DATE_TO?.trim() || "2026-11-11";
  const column = (process.env.DATE_COLUMN || "created_at").trim();
  const mode = (process.env.DATE_MODE || "range").trim().toLowerCase();

  if (!from && !to) {
    console.warn(
      "⚠ DATE_SCOPE is on but DATE_FROM and DATE_TO are both empty — ignoring.",
    );
    return null;
  }

  const normalize = (v, endOfDay = false) => {
    if (!v) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(v))
      return endOfDay ? `${v} 23:59:59` : `${v} 00:00:00`;
    return v;
  };

  return {
    column,
    mode,
    from: normalize(from, false),
    to: normalize(to, true),
  };
})();

// ═════════════════════════════════════════════════════════════
// Logging
// ═════════════════════════════════════════════════════════════
const log = (...a) => console.log(new Date().toISOString(), ...a);
const hr = (c = "─") => console.log(c.repeat(78));

// ═════════════════════════════════════════════════════════════
// Helpers
// ═════════════════════════════════════════════════════════════
const escapeIdent = (n) => "`" + String(n).replace(/`/g, "``") + "`";

async function getColumns(conn, db, table) {
  const [rows] = await conn.query(
    `SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_DEFAULT, EXTRA
     FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
     ORDER BY ORDINAL_POSITION`,
    [db, table],
  );
  return rows;
}

async function getPrimaryKey(conn, db, table) {
  const [rows] = await conn.query(
    `SELECT COLUMN_NAME
     FROM information_schema.KEY_COLUMN_USAGE
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
     ORDER BY ORDINAL_POSITION`,
    [db, table],
  );
  return rows.map((r) => r.COLUMN_NAME);
}

async function getFkEdges(conn, db, tables) {
  if (!tables.length) return [];
  const ph = tables.map(() => "?").join(",");
  const [rows] = await conn.query(
    `SELECT k.TABLE_NAME AS child_table, k.REFERENCED_TABLE_NAME AS parent_table
     FROM information_schema.KEY_COLUMN_USAGE k
     WHERE k.TABLE_SCHEMA = ?
       AND k.REFERENCED_COLUMN_NAME IS NOT NULL
       AND k.REFERENCED_TABLE_SCHEMA = ?
       AND k.TABLE_NAME IN (${ph})
       AND k.REFERENCED_TABLE_NAME IN (${ph})`,
    [db, db, ...tables, ...tables],
  );
  return rows;
}

function topoSort(nodes, edges) {
  const children = new Map(nodes.map((n) => [n, new Set()]));
  const indeg = new Map(nodes.map((n) => [n, 0]));
  for (const { child_table: c, parent_table: p } of edges) {
    if (c === p) continue;
    if (!children.has(p) || !children.has(c)) continue;
    children.get(p).add(c);
    indeg.set(c, indeg.get(c) + 1);
  }
  const q = nodes.filter((n) => indeg.get(n) === 0);
  const out = [];
  while (q.length) {
    const n = q.shift();
    out.push(n);
    for (const c of children.get(n)) {
      indeg.set(c, indeg.get(c) - 1);
      if (indeg.get(c) === 0) q.push(c);
    }
  }
  if (out.length !== nodes.length) {
    const cyc = nodes.filter((n) => !out.includes(n));
    throw new Error(`Cycle: ${cyc.join(", ")}`);
  }
  return out;
}

function buildDateWhere(table, columns) {
  if (!DATE_SCOPE) return { sql: "", params: [] };
  if (!columns.includes(DATE_SCOPE.column)) {
    console.warn(
      `⚠ ${table}: column "${DATE_SCOPE.column}" not present — skipping date filter.`,
    );
    return { sql: "", params: [] };
  }
  const col = escapeIdent(DATE_SCOPE.column);
  const parts = [];
  const params = [];
  if (
    (DATE_SCOPE.mode === "range" || DATE_SCOPE.mode === "from") &&
    DATE_SCOPE.from
  ) {
    parts.push(`${col} >= ?`);
    params.push(DATE_SCOPE.from);
  }
  if (
    (DATE_SCOPE.mode === "range" || DATE_SCOPE.mode === "to") &&
    DATE_SCOPE.to
  ) {
    parts.push(`${col} <= ?`);
    params.push(DATE_SCOPE.to);
  }
  if (!parts.length) return { sql: "", params: [] };
  return { sql: ` WHERE ${parts.join(" AND ")}`, params };
}

async function countRows(conn, table, dateWhere) {
  try {
    const sql =
      `SELECT COUNT(*) AS n FROM ${escapeIdent(table)}` +
      (dateWhere?.sql || "");
    const [[{ n }]] = await conn.query(sql, dateWhere?.params || []);
    return Number(n);
  } catch {
    return null;
  }
}

// ═════════════════════════════════════════════════════════════
// Auto-resolve missing FK references
// ═════════════════════════════════════════════════════════════
/**
 * Fetch missing parent rows from source, match against target by identity
 * fields (or insert new), and return a map of { source_uuid -> target_uuid }.
 */
async function autoResolveFk(src, tgt, fk, missingIds) {
  const map = {};
  const BATCH = 200;

  for (let i = 0; i < missingIds.length; i += BATCH) {
    const batch = missingIds.slice(i, i + BATCH);
    const ph = batch.map(() => "?").join(",");

    const [rows] = await src.query(
      `SELECT * FROM ${escapeIdent(fk.ref_table)}
       WHERE ${escapeIdent(fk.ref_col)} IN (${ph})`,
      batch,
    );

    for (const row of rows) {
      const srcId = row[fk.ref_col];
      let targetId = null;

      // Try to match existing target row by identity fields
      if (OPTIONS.autoResolveMatchFields.length) {
        const matchCols = OPTIONS.autoResolveMatchFields.filter((f) =>
          Object.prototype.hasOwnProperty.call(row, f),
        );
        if (matchCols.length) {
          const clauses = matchCols
            .map((f) => `${escapeIdent(f)} <=> ?`)
            .join(" AND ");
          const vals = matchCols.map((f) => row[f]);
          const [existing] = await tgt.query(
            `SELECT ${escapeIdent(fk.ref_col)} AS id
             FROM ${escapeIdent(fk.ref_table)}
             WHERE ${clauses}
             LIMIT 1`,
            vals,
          );
          if (existing.length) targetId = existing[0].id;
        }
      }

      // No match -> insert new row
      if (!targetId) {
        targetId = OPTIONS.autoResolveNewUuid ? randomUUID() : srcId;

        const cols = Object.keys(row);
        const colList = cols.map(escapeIdent).join(",");
        const placeholders = cols.map(() => "?").join(",");
        const values = cols.map((c) => (c === fk.ref_col ? targetId : row[c]));
        const updateCols = cols.filter((c) => c !== fk.ref_col);

        const updateClause = updateCols.length
          ? ` ON DUPLICATE KEY UPDATE ${updateCols.map((c) => `${escapeIdent(c)} = VALUES(${escapeIdent(c)})`).join(", ")}`
          : "";

        await tgt.query(
          `INSERT INTO ${escapeIdent(fk.ref_table)} (${colList}) VALUES (${placeholders})${updateClause}`,
          values,
        );
      }

      map[srcId] = targetId;
    }
  }

  return map;
}

/**
 * Apply a source->target UUID remap to a column in the target table.
 * Returns number of rows updated.
 */
async function applyRemap(tgt, table, column, uuidMap) {
  const entries = Object.entries(uuidMap).filter(([s, t]) => s !== t);
  if (!entries.length) return 0;

  const BATCH = 200;
  let total = 0;

  for (let i = 0; i < entries.length; i += BATCH) {
    const batch = entries.slice(i, i + BATCH);
    const caseSql =
      "CASE " +
      escapeIdent(column) +
      " " +
      batch.map(() => "WHEN ? THEN ?").join(" ") +
      " ELSE " +
      escapeIdent(column) +
      " END";
    const params = batch.flat();
    const inList = batch.map(() => "?").join(",");
    params.push(...batch.map(([oldId]) => oldId));

    const [r] = await tgt.query(
      `UPDATE ${escapeIdent(table)} SET ${escapeIdent(column)} = ${caseSql}
       WHERE ${escapeIdent(column)} IN (${inList})`,
      params,
    );
    total += r.affectedRows;
  }
  return total;
}

// ═════════════════════════════════════════════════════════════
// FK pre-flight audit (with auto-resolve)
// ═════════════════════════════════════════════════════════════
async function fkPreflight(src, tgt, insertOrder) {
  const db = TGT.database;
  const results = [];

  for (const table of insertOrder) {
    const [fks] = await tgt.query(
      `SELECT COLUMN_NAME AS col,
              REFERENCED_TABLE_NAME  AS ref_table,
              REFERENCED_COLUMN_NAME AS ref_col,
              CONSTRAINT_NAME        AS name
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ?
         AND TABLE_NAME = ?
         AND REFERENCED_TABLE_NAME IS NOT NULL
         AND REFERENCED_COLUMN_NAME IS NOT NULL`,
      [db, table],
    );

    if (!fks.length) continue;

    const childCols = (await getColumns(tgt, db, table)).map(
      (c) => c.COLUMN_NAME,
    );
    const childWhere = buildDateWhere(table, childCols);
    const childAnd = childWhere.sql
      ? "AND " + childWhere.sql.replace(/^\s*WHERE\s+/i, "")
      : "";

    for (const fk of fks) {
      if (fk.ref_table === table) continue;

      const [parentExists] = await tgt.query(
        `SELECT 1 FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
        [db, fk.ref_table],
      );
      if (!parentExists.length) {
        results.push({
          table,
          fk: `${fk.col} -> ${fk.ref_table}.${fk.ref_col}`,
          ref_table: fk.ref_table,
          missingCount: -1,
          sampleMissingRefs: [],
          note: "parent table missing in target",
        });
        continue;
      }

      const parentCols = (await getColumns(tgt, db, fk.ref_table)).map(
        (c) => c.COLUMN_NAME,
      );
      const parentWhere = buildDateWhere(fk.ref_table, parentCols);
      const parentAnd = parentWhere.sql
        ? "AND " + parentWhere.sql.replace(/^\s*WHERE\s+/i, "")
        : "";

      const countSql = `
        SELECT COUNT(*) AS n FROM (
          SELECT DISTINCT c.${escapeIdent(fk.col)} AS fk_val
          FROM ${escapeIdent(SRC.database)}.${escapeIdent(table)} c
          ${childWhere.sql}
        ) child
        WHERE child.fk_val IS NOT NULL
          AND child.fk_val NOT IN (
            SELECT ${escapeIdent(fk.ref_col)}
            FROM ${escapeIdent(TGT.database)}.${escapeIdent(fk.ref_table)}
            WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
          )
          AND child.fk_val NOT IN (
            SELECT ${escapeIdent(fk.ref_col)}
            FROM ${escapeIdent(SRC.database)}.${escapeIdent(fk.ref_table)}
            WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
            ${parentAnd}
          )
      `;
      const params = [...childWhere.params, ...parentWhere.params];

      const [[{ n }]] = await tgt.query(countSql, params);
      let missingCount = Number(n);

      const sampleMissingRefs = [];
      if (missingCount > 0) {
        const sampleSql = `
          SELECT DISTINCT c.${escapeIdent(fk.col)} AS fk_val
          FROM ${escapeIdent(SRC.database)}.${escapeIdent(table)} c
          WHERE c.${escapeIdent(fk.col)} IS NOT NULL
            ${childAnd}
            AND c.${escapeIdent(fk.col)} NOT IN (
              SELECT ${escapeIdent(fk.ref_col)}
              FROM ${escapeIdent(TGT.database)}.${escapeIdent(fk.ref_table)}
              WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
            )
            AND c.${escapeIdent(fk.col)} NOT IN (
              SELECT ${escapeIdent(fk.ref_col)}
              FROM ${escapeIdent(SRC.database)}.${escapeIdent(fk.ref_table)}
              WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
              ${parentAnd}
            )
          LIMIT 5
        `;
        const [samples] = await tgt.query(sampleSql, params);
        samples.forEach((s) => sampleMissingRefs.push(s.fk_val));
      }

      // Auto-resolve if enabled and applicable
      if (
        missingCount > 0 &&
        OPTIONS.autoResolveFks &&
        OPTIONS.autoResolveTables.includes(fk.ref_table)
      ) {
        if (OPTIONS.dryRun) {
          console.log(
            `  → [dry-run] would auto-resolve ${missingCount} missing ${fk.ref_table} ref(s) for ${table}.${fk.col}`,
          );
        } else {
          console.log(
            `  → auto-resolving ${missingCount} missing ${fk.ref_table} ref(s) for ${table}.${fk.col}...`,
          );

          // Fetch full list of missing IDs
          const [allMissing] = await tgt.query(
            `SELECT DISTINCT c.${escapeIdent(fk.col)} AS fk_val
             FROM ${escapeIdent(SRC.database)}.${escapeIdent(table)} c
             WHERE c.${escapeIdent(fk.col)} IS NOT NULL
               ${childAnd}
               AND c.${escapeIdent(fk.col)} NOT IN (
                 SELECT ${escapeIdent(fk.ref_col)}
                 FROM ${escapeIdent(TGT.database)}.${escapeIdent(fk.ref_table)}
                 WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
               )
               AND c.${escapeIdent(fk.col)} NOT IN (
                 SELECT ${escapeIdent(fk.ref_col)}
                 FROM ${escapeIdent(SRC.database)}.${escapeIdent(fk.ref_table)}
                 WHERE ${escapeIdent(fk.ref_col)} IS NOT NULL
                 ${parentAnd}
               )`,
            params,
          );

          const missingIds = allMissing.map((r) => r.fk_val);
          const uuidMap = await autoResolveFk(src, tgt, fk, missingIds);

          const remapped = Object.entries(uuidMap).filter(
            ([s, t]) => s !== t,
          ).length;
          if (remapped > 0) {
            console.log(
              `  → remapping ${remapped} ${table}.${fk.col} value(s)...`,
            );
            const rows = await applyRemap(tgt, table, fk.col, uuidMap);
            console.log(`  → ${rows} row(s) remapped`);
          }

          // Re-verify
          const [[{ n: stillMissing }]] = await tgt.query(countSql, params);
          missingCount = Number(stillMissing);

          if (missingCount === 0) {
            console.log(`  ✓ ${fk.ref_table} references resolved`);
          } else {
            console.log(
              `  ⚠ ${missingCount} ${fk.ref_table} ref(s) still missing after auto-resolve`,
            );
          }
        }
      }

      results.push({
        table,
        fk: `${fk.col} -> ${fk.ref_table}.${fk.ref_col}`,
        ref_table: fk.ref_table,
        missingCount,
        sampleMissingRefs,
      });
    }
  }

  return results;
}

function printFkPreflight(results) {
  console.log("\n── FK pre-flight ──");
  let problems = 0;
  if (!results.length) {
    console.log("  ✓ no outgoing FKs to check");
    return 0;
  }
  for (const r of results) {
    if (r.missingCount < 0) {
      problems++;
      console.log(`  ⚠ ${r.table}.${r.fk}: ${r.note}`);
    } else if (r.missingCount > 0) {
      problems++;
      console.log(
        `  ⚠ ${r.table}.${r.fk}: ${r.missingCount} orphan reference(s)`,
      );
      if (r.sampleMissingRefs.length) {
        console.log(`     sample missing: ${r.sampleMissingRefs.join(", ")}`);
      }
    } else if (OPTIONS.verbose) {
      console.log(`  ✓ ${r.table}.${r.fk}: all resolve`);
    }
  }
  if (problems === 0) {
    console.log(
      "  ✓ all FKs resolve (target + in-scope parents + auto-resolved)",
    );
  } else {
    console.log(`\n  ${problems} FK(s) will produce orphans.`);
    if (!OPTIONS.force) {
      console.log(
        "  Use --force to proceed anyway, or fix the missing references first.",
      );
    }
  }
  return problems;
}

// ═════════════════════════════════════════════════════════════
// Post-insert orphan check
// ═════════════════════════════════════════════════════════════
async function postInsertOrphanCheck(tgt, insertOrder) {
  const db = TGT.database;
  const report = [];

  for (const table of insertOrder) {
    const [fks] = await tgt.query(
      `SELECT COLUMN_NAME AS col, REFERENCED_TABLE_NAME AS ref_table,
              REFERENCED_COLUMN_NAME AS ref_col
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
         AND REFERENCED_TABLE_NAME IS NOT NULL
         AND REFERENCED_COLUMN_NAME IS NOT NULL`,
      [db, table],
    );

    for (const fk of fks) {
      if (fk.ref_table === table) continue;

      const cols = (await getColumns(tgt, db, table)).map((c) => c.COLUMN_NAME);
      const where = buildDateWhere(table, cols);
      const andWhere = where.sql
        ? "AND " + where.sql.replace(/^\s*WHERE\s+/i, "")
        : "";

      const [[{ n }]] = await tgt.query(
        `SELECT COUNT(*) AS n
         FROM ${escapeIdent(table)} c
         WHERE c.${escapeIdent(fk.col)} IS NOT NULL
           ${andWhere}
           AND NOT EXISTS (
             SELECT 1 FROM ${escapeIdent(fk.ref_table)} p
             WHERE p.${escapeIdent(fk.ref_col)} = c.${escapeIdent(fk.col)}
           )`,
        where.params,
      );

      if (Number(n) > 0) {
        report.push({
          table,
          fk: `${fk.col} -> ${fk.ref_table}.${fk.ref_col}`,
          orphans: Number(n),
        });
      }
    }
  }

  return report;
}

// ═════════════════════════════════════════════════════════════
// DRY RUN
// ═════════════════════════════════════════════════════════════
async function dryRun(src, tgt, insertOrder, deleteOrder, edges) {
  hr("=");
  log("DRY RUN — no data will be written");
  hr("=");

  log("\nSource:", `${SRC.user}@${SRC.host}:${SRC.port}/${SRC.database}`);
  log("Target:", `${TGT.user}@${TGT.host}:${TGT.port}/${TGT.database}`);
  log("Tables:", insertOrder.length);

  if (DATE_SCOPE) {
    log(
      `Date scope: ${DATE_SCOPE.column} ${DATE_SCOPE.mode} ` +
        `[${DATE_SCOPE.from || "-inf"} … ${DATE_SCOPE.to || "+inf"}]`,
    );
  } else {
    log("Date scope: none (all rows)");
  }

  if (OPTIONS.autoResolveFks) {
    log(
      `Auto-resolve: ON for [${OPTIONS.autoResolveTables.join(", ")}] ` +
        `(match: ${OPTIONS.autoResolveMatchFields.join(",")})`,
    );
  } else {
    log("Auto-resolve: OFF");
  }

  console.log("\n── FK edges among selected tables ──");
  if (!edges.length) console.log("  (none)");
  else
    edges.forEach((e) =>
      console.log(`  ${e.child_table} -> ${e.parent_table}`),
    );

  console.log("\n── Insert order (parents first) ──");
  insertOrder.forEach((t, i) =>
    console.log(`  ${String(i + 1).padStart(2)}. ${t}`),
  );

  console.log("\n── Delete order (children first) ──");
  deleteOrder.forEach((t, i) =>
    console.log(`  ${String(i + 1).padStart(2)}. ${t}`),
  );

  const report = [];
  for (const table of insertOrder) {
    const srcCols = await getColumns(src, SRC.database, table);
    const tgtCols = await getColumns(tgt, TGT.database, table);
    const srcNames = srcCols.map((c) => c.COLUMN_NAME);
    const tgtNames = tgtCols.map((c) => c.COLUMN_NAME);
    const shared = srcNames.filter((c) => tgtNames.includes(c));
    const onlySrc = srcNames.filter((c) => !tgtNames.includes(c));
    const onlyTgt = tgtNames.filter((c) => !srcNames.includes(c));

    const srcPk = await getPrimaryKey(src, SRC.database, table);
    const where = buildDateWhere(table, shared);
    const srcCount = await countRows(src, table, where);
    const tgtCount = await countRows(tgt, table, where);

    let sample = [];
    if (OPTIONS.verbose && srcCount > 0) {
      const [rows] = await src.query(
        `SELECT * FROM ${escapeIdent(table)}${where.sql} LIMIT 3`,
        where.params,
      );
      sample = rows;
    }

    report.push({
      table,
      sourceRows: srcCount,
      targetRows: tgtCount,
      sharedCols: shared.length,
      onlySource: onlySrc,
      onlyTarget: onlyTgt,
      pk: srcPk.length ? srcPk.join(",") : "(none)",
      sample,
    });
  }

  console.log("\n── Per-table report ──\n");
  for (const r of report) {
    console.log(`▶ ${r.table}`);
    console.log(`   source rows : ${r.sourceRows}`);
    console.log(`   target rows : ${r.targetRows}`);
    console.log(`   shared cols : ${r.sharedCols}`);
    console.log(`   PK          : ${r.pk}`);
    if (r.onlySource.length)
      console.log(`   ⚠ only in SOURCE : ${r.onlySource.join(", ")}`);
    if (r.onlyTarget.length)
      console.log(`   ⚠ only in TARGET : ${r.onlyTarget.join(", ")}`);
    if (OPTIONS.verbose && r.sample.length)
      console.log(`   sample: ${JSON.stringify(r.sample[0])}`);
    console.log("");
  }

  // FK pre-flight (with dry-run auto-resolve preview)
  if (!OPTIONS.skipFkCheck) {
    const fkResults = await fkPreflight(src, tgt, insertOrder);
    const problems = printFkPreflight(fkResults);

    if (problems > 0 && OPTIONS.strict && !OPTIONS.force) {
      hr("=");
      console.log(
        "STRICT MODE: aborting due to FK orphans. Use --force to override.",
      );
      hr("=");
      process.exitCode = 2;
      return;
    }
  } else {
    console.log("\n── FK pre-flight: SKIPPED (--skip-fk-check) ──");
  }

  const totalSrc = report.reduce((s, r) => s + (r.sourceRows || 0), 0);
  const totalTgt = report.reduce((s, r) => s + (r.targetRows || 0), 0);
  console.log("\n── Summary ──");
  console.log(`  tables   : ${report.length}`);
  console.log(`  src rows : ${totalSrc}`);
  console.log(`  tgt rows : ${totalTgt}`);
  console.log(`  delta    : ${totalSrc - totalTgt}`);
  const drift = report.filter(
    (r) => r.onlySource.length || r.onlyTarget.length,
  );
  if (drift.length) {
    console.log(
      `  ⚠ schema drift in ${drift.length} table(s): ${drift.map((r) => r.table).join(", ")}`,
    );
  } else {
    console.log("  ✓ no schema drift");
  }
  hr("=");
  log("DRY RUN complete — rerun without --dry-run to apply.");
  hr("=");
}

// ═════════════════════════════════════════════════════════════
// Migrate one table
// ═════════════════════════════════════════════════════════════
async function migrateTable(src, tgt, table) {
  const srcCols = await getColumns(src, SRC.database, table);
  const tgtCols = await getColumns(tgt, TGT.database, table);
  const shared = srcCols
    .map((c) => c.COLUMN_NAME)
    .filter((c) => tgtCols.some((t) => t.COLUMN_NAME === c));
  const pkCols = await getPrimaryKey(tgt, TGT.database, table);

  if (!shared.length) {
    log(`  ${table}: skipped (no shared columns)`);
    return { table, total: 0, inserted: 0, skipped: true };
  }

  const where = buildDateWhere(table, shared);
  const [[{ n: total }]] = await src.query(
    `SELECT COUNT(*) AS n FROM ${escapeIdent(table)}` + where.sql,
    where.params,
  );

  log(
    `  ${table}: ${total} rows in scope${where.sql ? " (date-filtered)" : ""}, ${shared.length} cols, PK=[${pkCols.join(",")}]`,
  );

  if (OPTIONS.truncateTarget) {
    await tgt.query("SET FOREIGN_KEY_CHECKS=0");
    await tgt.query(`TRUNCATE TABLE ${escapeIdent(table)}`);
    await tgt.query("SET FOREIGN_KEY_CHECKS=1");
    log(`  ${table}: target truncated`);
  }

  const colList = shared.map(escapeIdent).join(", ");
  const placeholders = shared.map(() => "?").join(", ");
  const updateList =
    shared
      .filter((c) => !pkCols.includes(c))
      .map((c) => `${escapeIdent(c)} = VALUES(${escapeIdent(c)})`)
      .join(", ") ||
    (pkCols[0] ? `${escapeIdent(pkCols[0])} = ${escapeIdent(pkCols[0])}` : "");
  const updateClause = updateList
    ? ` ON DUPLICATE KEY UPDATE ${updateList}`
    : "";

  let offset = 0,
    inserted = 0;
  const orderBy = pkCols.length ? pkCols.map(escapeIdent).join(", ") : "1";

  while (offset < total) {
    const [rows] = await src.query(
      `SELECT ${colList} FROM ${escapeIdent(table)}${where.sql} ORDER BY ${orderBy} LIMIT ? OFFSET ?`,
      [...where.params, OPTIONS.batchSize, offset],
    );
    if (!rows.length) break;

    const values = rows.flatMap((r) => shared.map((c) => r[c]));
    const multi = rows.map(() => `(${placeholders})`).join(", ");
    const sql = `INSERT INTO ${escapeIdent(table)} (${colList}) VALUES ${multi}${updateClause}`;

    await tgt.query(sql, values);
    inserted += rows.length;
    offset += rows.length;
    if (OPTIONS.verbose || inserted % (OPTIONS.batchSize * 10) === 0) {
      log(`  ${table}: ${inserted}/${total}`);
    }
  }

  return { table, total, inserted };
}

// ═════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════
(async () => {
  if (!OPTIONS.tables.length) {
    console.error("ERROR: TABLES is empty. Set TABLES=table1,table2 in .env");
    process.exit(1);
  }

  hr("=");
  log(`Mode: ${OPTIONS.dryRun ? "DRY RUN (no writes)" : "LIVE MIGRATION"}`);
  log(`Source: ${SRC.user}@${SRC.host}:${SRC.port}/${SRC.database}`);
  log(`Target: ${TGT.user}@${TGT.host}:${TGT.port}/${TGT.database}`);
  log(`Tables: ${OPTIONS.tables.join(", ")}`);
  log(`Strategy: ${OPTIONS.truncateTarget ? "truncate + insert" : "upsert"}`);
  log(`Triggers: ${OPTIONS.suppressTriggers ? "SUPPRESSED" : "will fire"}`);
  log(
    `FK check: ${OPTIONS.skipFkCheck ? "SKIPPED" : "enabled"}${OPTIONS.force ? " (--force)" : ""}${OPTIONS.strict ? " (--strict)" : ""}`,
  );
  log(
    `Auto-resolve: ${OPTIONS.autoResolveFks ? "ON [" + OPTIONS.autoResolveTables.join(", ") + "]" : "OFF"}`,
  );
  if (DATE_SCOPE) {
    log(
      `Date scope: ${DATE_SCOPE.column} ${DATE_SCOPE.mode} [${DATE_SCOPE.from || "-inf"} … ${DATE_SCOPE.to || "+inf"}]`,
    );
  } else {
    log("Date scope: none (all rows)");
  }
  hr("=");

  const src = await mysql.createConnection({ ...SRC });
  const tgt = await mysql.createConnection({ ...TGT });

  try {
    for (const t of OPTIONS.tables) {
      const [s] = await src.query(
        `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
        [SRC.database, t],
      );
      const [d] = await tgt.query(
        `SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
        [TGT.database, t],
      );
      if (!s.length) throw new Error(`Missing in SOURCE: ${t}`);
      if (!d.length) throw new Error(`Missing in TARGET: ${t}`);
    }

    const edges = await getFkEdges(src, SRC.database, OPTIONS.tables);
    const insertOrder = topoSort(OPTIONS.tables, edges);
    const deleteOrder = [...insertOrder].reverse();

    if (OPTIONS.dryRun) {
      await dryRun(src, tgt, insertOrder, deleteOrder, edges);
      return;
    }

    // Live migration: FK pre-flight first (may auto-resolve)
    if (!OPTIONS.skipFkCheck) {
      const fkResults = await fkPreflight(src, tgt, insertOrder);
      const problems = printFkPreflight(fkResults);
      if (problems > 0 && !OPTIONS.force) {
        hr("=");
        console.log(`Aborting: ${problems} FK(s) would produce orphans.`);
        console.log(
          "Fix the missing references, or pass --force to proceed anyway.",
        );
        hr("=");
        process.exitCode = 2;
        return;
      }
    }

    await tgt.query("SET FOREIGN_KEY_CHECKS=0");
    if (OPTIONS.suppressTriggers) {
      await tgt.query("SET @sync_triggers_disabled = 2");
      await tgt.query("SET @sym_is_syncing = TRUE");
      log("Triggers suppressed for this session");
    }

    const summary = [];
    try {
      console.log("\n── Migrating ──");
      for (const t of insertOrder) {
        summary.push(await migrateTable(src, tgt, t));
      }
    } finally {
      await tgt.query("SET FOREIGN_KEY_CHECKS=1");
      if (OPTIONS.suppressTriggers) {
        await tgt.query("SET @sync_triggers_disabled = NULL");
        await tgt.query("SET @sym_is_syncing = NULL");
      }
    }

    // Post-insert orphan check
    if (!OPTIONS.skipFkCheck) {
      const orphans = await postInsertOrphanCheck(tgt, insertOrder);
      console.log("\n── Post-insert orphan check ──");
      if (!orphans.length) {
        console.log("  ✓ no orphans in any in-scope table");
      } else {
        orphans.forEach((o) =>
          console.log(`  ⚠ ${o.table}.${o.fk}: ${o.orphans} orphan row(s)`),
        );
        if (OPTIONS.strict) {
          console.log("\nSTRICT MODE: orphans present after migration.");
          process.exitCode = 3;
        }
      }
    }

    console.log("\n── Summary ──");
    console.table(summary);
    log("Migration complete.");
  } catch (err) {
    console.error("FAILED:", err.message);
    process.exitCode = 1;
  } finally {
    await src.end();
    await tgt.end();
  }
})();

/**
 * diff-lookup.js
 *
 * Pure helper for auditing a lookup table (product_type, product_category,
 * classification, price, unit_of_measure).
 *
 * Compares the distinct lookup names referenced by a store's products on the
 * legacy (source) DB against the full set of names on the target DB, and
 * classifies every legacy name into one of:
 *
 *   MATCH                 — exact normalized match on an active target row
 *   ALIAS                 — matched via ALIASES map (target active)
 *   AMBIGUOUS             — multiple active target rows normalize to same key
 *   INACTIVE              — only inactive target rows have this name
 *   ALIAS_TO_INACTIVE     — alias points to an inactive target row
 *   ALIAS_TARGET_MISSING  — alias exists but its target is missing entirely
 *   MISSING               — no match at all
 *
 * Returns a summary object consumed by audit.js's SECTION 3 verdict.
 */

"use strict";

/**
 * Normalize a name for comparison: trim + lowercase.
 * Matches the behavior of audit.js's and runner.js's `norm`.
 */
function norm(n) {
  return n == null ? null : String(n).trim().toLowerCase();
}

/**
 * Diff one lookup table.
 *
 * @param {import('mysql2/promise').Connection} legacy   Source DB connection
 * @param {import('mysql2/promise').Connection} target   Target DB connection
 * @param {object}   opts
 * @param {string}   opts.label          Logical label ("product_type", ...)
 * @param {string}   opts.targetTable    Actual target table name
 * @param {string}   opts.sql            SELECT returning { name } for legacy
 * @param {Array}    opts.params         Parameters for opts.sql
 * @param {object}   opts.aliases        ALIASES map for this label
 * @param {boolean}  [opts.verbose=true] Print per-name diff to console
 *
 * @returns {Promise<{
 *   label: string,
 *   targetTable: string,
 *   legacyNames: string[],
 *   targetRowCount: number,
 *   activeRowCount: number,
 *   counts: Record<string, number>,
 *   classifications: Array<object>,
 *   activeMatches: number,
 *   softMisses: number,
 *   hardMisses: number,
 *   ambiguous: number
 * }>}
 */
async function diffLookup(
  legacy,
  target,
  { label, targetTable, sql, params, aliases, verbose = true },
) {
  // 1. Fetch distinct legacy lookup names referenced by this store's products.
  const [legacyRows] = await legacy.execute(sql, params ?? []);
  const legacyNames = [
    ...new Set(
      legacyRows.map((r) => r.name).filter((n) => n != null && n !== ""),
    ),
  ];

  // 2. Fetch all target lookup rows (active + inactive).
  const [targetRows] = await target.execute(
    `SELECT record_id, name, status FROM ${targetTable}`,
  );

  // 3. Build indexes.
  const byNorm = new Map(); // norm(name) -> [rows]
  for (const t of targetRows) {
    const key = norm(t.name);
    if (!key) continue;
    if (!byNorm.has(key)) byNorm.set(key, []);
    byNorm.get(key).push({ ...t, key });
  }

  const byActive = new Map(); // norm(name) -> [active rows]
  for (const [k, list] of byNorm) {
    const active = list.filter((r) => r.status === 1);
    if (active.length) byActive.set(k, active);
  }

  // 4. Classify each legacy name.
  const classifications = [];
  for (const raw of legacyNames) {
    const key = norm(raw);
    const item = {
      raw,
      key,
      verdict: "MISSING",
      targetName: null,
      targetStatus: null,
      targetId: null,
      alias: null,
    };

    if (byActive.has(key)) {
      const activeRows = byActive.get(key);
      item.targetName = activeRows[0].name;
      item.targetStatus = 1;
      item.targetId = activeRows[0].record_id;
      item.verdict = activeRows.length > 1 ? "AMBIGUOUS" : "MATCH";
    } else if (byNorm.has(key)) {
      const row = byNorm.get(key)[0];
      item.targetName = row.name;
      item.targetStatus = row.status;
      item.targetId = row.record_id;
      item.verdict = "INACTIVE";
    } else {
      const aliasTarget = aliases?.[key];
      if (aliasTarget) {
        const aliasKey = norm(aliasTarget);
        item.alias = aliasTarget;
        if (byActive.has(aliasKey)) {
          const row = byActive.get(aliasKey)[0];
          item.targetName = row.name;
          item.targetStatus = 1;
          item.targetId = row.record_id;
          item.verdict = "ALIAS";
        } else if (byNorm.has(aliasKey)) {
          const row = byNorm.get(aliasKey)[0];
          item.targetName = row.name;
          item.targetStatus = row.status;
          item.targetId = row.record_id;
          item.verdict = "ALIAS_TO_INACTIVE";
        } else {
          item.verdict = "ALIAS_TARGET_MISSING";
        }
      } else {
        item.verdict = "MISSING";
      }
    }

    classifications.push(item);
  }

  // 5. Count by verdict.
  const counts = {};
  for (const c of classifications) {
    counts[c.verdict] = (counts[c.verdict] || 0) + 1;
  }

  // 6. Print.
  if (verbose) {
    console.log(`\n=== ${label} (target table: ${targetTable}) ===`);
    console.log(`  legacy names          : ${legacyNames.length}`);
    console.log(
      `  target rows / active  : ${targetRows.length} / ${targetRows.filter((r) => r.status === 1).length}`,
    );
    if (legacyNames.length === 0) {
      console.log("  (no legacy names reference this lookup — nothing to do)");
    } else {
      const order = [
        "MATCH",
        "ALIAS",
        "AMBIGUOUS",
        "INACTIVE",
        "ALIAS_TO_INACTIVE",
        "ALIAS_TARGET_MISSING",
        "MISSING",
      ];
      const icons = {
        MATCH: "✅",
        ALIAS: "⚠️ ",
        AMBIGUOUS: "⚠️ ",
        INACTIVE: "❌",
        ALIAS_TO_INACTIVE: "❌",
        ALIAS_TARGET_MISSING: "❌",
        MISSING: "❌",
      };
      for (const v of order) {
        if (!counts[v]) continue;
        console.log(`  ${icons[v]} ${v} (${counts[v]}):`);
        for (const c of classifications.filter((x) => x.verdict === v)) {
          const nm = JSON.stringify(c.raw);
          if (v === "MATCH") {
            console.log(`      ${nm}  →  target "${c.targetName}"`);
          } else if (v === "ALIAS") {
            console.log(
              `      ${nm}  →  alias "${c.alias}"  →  target "${c.targetName}"`,
            );
          } else if (v === "AMBIGUOUS") {
            console.log(
              `      ${nm}  →  MULTIPLE active targets (resolver picks first — verify)`,
            );
          } else if (v === "INACTIVE") {
            console.log(
              `      ${nm}  →  target "${c.targetName}" (status=${c.targetStatus})`,
            );
          } else if (v === "ALIAS_TO_INACTIVE") {
            console.log(
              `      ${nm}  →  alias "${c.alias}"  →  target "${c.targetName}" (status=${c.targetStatus})`,
            );
          } else if (v === "ALIAS_TARGET_MISSING") {
            console.log(
              `      ${nm}  →  alias "${c.alias}"  →  NOT in target (INSERT needed)`,
            );
          } else if (v === "MISSING") {
            console.log(`      ${nm}  →  NOT in target (INSERT needed)`);
          }
        }
      }
    }
  }

  // 7. Summary.
  const activeMatches = (counts.MATCH || 0) + (counts.ALIAS || 0);
  const softMisses = (counts.INACTIVE || 0) + (counts.ALIAS_TO_INACTIVE || 0);
  const hardMisses = (counts.MISSING || 0) + (counts.ALIAS_TARGET_MISSING || 0);
  const ambiguous = counts.AMBIGUOUS || 0;

  return {
    label,
    targetTable,
    legacyNames,
    targetRowCount: targetRows.length,
    activeRowCount: targetRows.filter((r) => r.status === 1).length,
    counts,
    classifications,
    activeMatches,
    softMisses,
    hardMisses,
    ambiguous,
  };
}

module.exports = { diffLookup, norm };

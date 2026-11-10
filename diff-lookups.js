require("dotenv").config();
const mysql = require("mysql2/promise");

const norm = (n) => (n == null ? null : String(n).trim().toLowerCase());

// Known renames: legacy name -> target name. Keys must be normalized (lowercase, trimmed).
const ALIASES = {
  product_type: {
    "pots / pans": "pot & pans",
    decoration: "decorations",
    topperware: "toppersware",
    "gel pen": "gelpen",
  },
  product_category: {},
  classification: {
    deskfan: "desk fan",
    standfan: "stand fan",
    wallfan: "wall fan",
    flowers: "flower",
  },
  price: {},
  unit_of_measure: {},
};

async function main() {
  const legacy = await mysql.createConnection({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT) || 3306,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
  });
  const target = await mysql.createConnection({
    host: process.env.DB_TARGET_HOST,
    port: Number(process.env.DB_TARGET_PORT) || 3306,
    user: process.env.DB_TARGET_USERNAME,
    password: process.env.DB_TARGET_PASSWORD,
    database: process.env.DB_TARGET_DATABASE,
  });

  const store = process.env.SOURCE_STORE_RECORD_ID;

  const lookups = [
    {
      label: "product_type",
      targetTable: "product_type",
      sql: `SELECT DISTINCT pt.name AS name
            FROM product p
            LEFT JOIN product_type pt ON p.product_type = pt.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "product_category",
      targetTable: "product_category",
      sql: `SELECT DISTINCT pc.name AS name
            FROM product p
            LEFT JOIN product_category pc ON p.category = pc.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "classification",
      targetTable: "classification",
      sql: `SELECT DISTINCT c.name AS name
            FROM product p
            LEFT JOIN classification c ON p.classification = c.record_id
            WHERE p.product_code IN (
              SELECT p_sub.product_code FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              JOIN product p_sub ON sod_sub.product = p_sub.record_id
              WHERE so_sub.store = ?
            )`,
    },
    {
      label: "price",
      targetTable: "price",
      sql: `SELECT DISTINCT pr.name AS name
            FROM pricing_scheme ps
            LEFT JOIN price pr ON ps.price = pr.record_id
            WHERE ps.store = ? AND ps.product IN (
              SELECT sod_sub.product FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              WHERE so_sub.store = ?
            )`,
      params: [store, store],
    },
    {
      label: "unit_of_measure",
      targetTable: "unit_of_measure",
      sql: `SELECT DISTINCT uom.name AS name
            FROM pricing_scheme ps
            LEFT JOIN unit_of_measure uom ON ps.uom = uom.record_id
            WHERE ps.store = ? AND ps.product IN (
              SELECT sod_sub.product FROM sales_order so_sub
              JOIN sales_order_details sod_sub ON so_sub.record_id = sod_sub.sales_order
              WHERE so_sub.store = ?
            )`,
      params: [store, store],
    },
  ];

  for (const { label, targetTable, sql, params } of lookups) {
    const [legacyRows] = await legacy.execute(sql, params ?? [store]);

    const [activeRows] = await target.execute(
      `SELECT name FROM ${targetTable} WHERE status = 1`,
    );
    const [allRows] = await target.execute(
      `SELECT name, status FROM ${targetTable}`,
    );

    const activeSet = new Set(
      activeRows.map((r) => norm(r.name)).filter(Boolean),
    );
    const inactiveOnlySet = new Set();
    for (const r of allRows) {
      const k = norm(r.name);
      if (!k) continue;
      if (!activeSet.has(k)) inactiveOnlySet.add(k);
    }

    const missingEntirely = [];
    const inactiveOnly = [];
    const alias = [];

    const seen = new Set();
    for (const row of legacyRows) {
      const raw = row.name;
      const key = norm(raw);
      if (key == null || seen.has(key)) continue;
      seen.add(key);

      if (activeSet.has(key)) continue;

      const aliasTo = ALIASES[label]?.[key];
      if (aliasTo && activeSet.has(norm(aliasTo))) {
        alias.push({ from: raw, to: aliasTo });
        continue;
      }

      if (inactiveOnlySet.has(key)) inactiveOnly.push(raw);
      else missingEntirely.push(raw);
    }

    console.log(`\n=== ${label} (target table: ${targetTable}) ===`);
    console.log(`  legacy distinct: ${seen.size}`);
    console.log(
      `  active exact matches: ${seen.size - alias.length - inactiveOnly.length - missingEntirely.length}`,
    );
    console.log(`  alias-mapped: ${alias.length}`);
    for (const a of alias)
      console.log(`    ~ ${JSON.stringify(a.from)} -> ${JSON.stringify(a.to)}`);
    console.log(
      `  only-inactive in target (flip status=1 or alias): ${inactiveOnly.length}`,
    );
    for (const u of inactiveOnly.sort())
      console.log(`    - ${JSON.stringify(u)}`);
    console.log(`  missing entirely (INSERT): ${missingEntirely.length}`);
    for (const u of missingEntirely.sort())
      console.log(`    - ${JSON.stringify(u)}`);
  }

  await legacy.end();
  await target.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

use std::fs::File;
use std::io::Write;

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::Connection;

use crate::download::ensure_reference_db;
use crate::stats::StatsStore;
use crate::AlleleReportArgs;

pub fn run_allele_report(args: AlleleReportArgs) -> Result<()> {
    if args.output.extension().is_none() {
        anyhow::bail!("--output must include a filename (e.g. report.html)");
    }

    let sqlite_path = ensure_reference_db(Some(&args.sqlite))?;
    let store = StatsStore::connect(&sqlite_path)?;
    let conn = store.open_connection()?;
    let summary = FormatSummary::gather(&conn)?;

    if let Some(parent) = args.output.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Create report directory {:?}", parent))?;
        }
    }

    let mut file = File::create(&args.output)
        .with_context(|| format!("Create report file {:?}", args.output))?;
    write_header(&mut file, &summary, &args)?;
    write_table_rows(&mut file, &conn)?;
    write_footer(&mut file)?;
    file.flush()?;

    println!(
        "🧾 RSID coverage report written to {} ({} formats; {} format/rsid rows)",
        args.output.display(),
        summary.unique_formats,
        summary.total_rows
    );
    Ok(())
}

struct FormatSummary {
    unique_formats: i64,
    unique_rsids: i64,
    total_rows: i64,
    generated_at: String,
}

impl FormatSummary {
    fn gather(conn: &Connection) -> Result<Self> {
        let unique_formats: i64 = conn
            .query_row(
                "SELECT COALESCE(COUNT(DISTINCT format_id), 0) FROM rsid_reference",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        let unique_rsids: i64 = conn
            .query_row(
                "SELECT COALESCE(COUNT(*), 0) FROM rsid_reference",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        let total_rows: i64 = conn
            .query_row(
                "SELECT COALESCE(COUNT(*), 0) FROM rsid_reference",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(Self {
            unique_formats,
            unique_rsids,
            total_rows,
            generated_at: Utc::now().to_rfc3339(),
        })
    }
}

fn write_header(file: &mut File, summary: &FormatSummary, args: &AlleleReportArgs) -> Result<()> {
    let source = html_escape(args.sqlite.display().to_string().as_str());
    let generated_at = html_escape(&summary.generated_at);
    writeln!(
        file,
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>RSID Coverage Report</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 32px;
      color: #1f2933;
      background-color: #f8fafc;
    }}
    h1 {{ margin-bottom: 0.25rem; }}
    .meta {{ color: #475569; margin-bottom: 1rem; }}
    table {{
      border-collapse: collapse;
      width: 100%;
      background: #fff;
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.08);
      border-radius: 8px;
      overflow: hidden;
    }}
    thead th {{
      background: #0f172a;
      color: #e2e8f0;
      padding: 12px;
      text-align: left;
      cursor: pointer;
      position: sticky;
      top: 0;
    }}
    tbody td {{
      padding: 10px 12px;
      border-bottom: 1px solid #e2e8f0;
      font-family: "SFMono-Regular", Consolas, monospace;
    }}
    tbody tr:nth-child(even) {{ background: #f1f5f9; }}
    tbody tr:hover {{ background: #e2e8f0; }}
    .count {{ text-align: right; }}
    .empty {{ text-align: center; padding: 2rem; color: #94a3b8; }}
  </style>
</head>
<body>
  <h1>RSID Coverage Report</h1>
  <div class="meta">
    Source database: <strong>{source}</strong><br/>
    Generated at: <strong>{generated_at}</strong><br/>
    Formats tracked: <strong>{formats}</strong>,
    Unique rsids: <strong>{unique_rsids}</strong>,
    Format/rsid rows: <strong>{total_rows}</strong>
  </div>
  <table id="rsid-table">
    <thead>
      <tr>
        <th data-type="string">Format</th>
        <th data-type="string">RSID / Marker</th>
        <th data-type="number">Observations</th>
      </tr>
    </thead>
    <tbody>
"#,
        formats = summary.unique_formats,
        unique_rsids = summary.unique_rsids,
        total_rows = summary.total_rows
    )
    .context("write report header")?;
    Ok(())
}

fn write_table_rows(file: &mut File, conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT f.name as format, rr.rsid, 1 as count
         FROM rsid_reference rr
         JOIN formats f ON f.id = rr.format_id
         ORDER BY f.name ASC, rr.rsid ASC",
    )?;
    let mut rows = stmt.query([])?;
    let mut has_rows = false;
    while let Some(row) = rows.next()? {
        has_rows = true;
        let format: String = row.get(0)?;
        let rsid: i64 = row.get(1)?;
        let count: i64 = row.get(2)?;
        writeln!(
            file,
            r#"      <tr>
        <td>{format}</td>
        <td>rs{rsid}</td>
        <td class="count" data-sort-value="{count}">{count}</td>
      </tr>"#,
            format = html_escape(&format),
            rsid = rsid,
            count = count
        )
        .context("write report row")?;
    }

    if !has_rows {
        writeln!(
            file,
            r#"      <tr><td colspan="3" class="empty">No rsid data available</td></tr>"#
        )?;
    }
    Ok(())
}

fn write_footer(file: &mut File) -> Result<()> {
    writeln!(
        file,
        r#"    </tbody>
  </table>
  <script>
    (function() {{
      const table = document.getElementById("rsid-table");
      if (!table) return;
      const getCellValue = (row, idx) => {{
        const cell = row.children[idx];
        return cell.getAttribute("data-sort-value") ?? cell.textContent.trim();
      }};
      const comparer = (idx, type, asc) => (a, b) => {{
        const v1 = getCellValue(asc ? a : b, idx);
        const v2 = getCellValue(asc ? b : a, idx);
        if (type === "number") {{
          return parseFloat(v1) - parseFloat(v2);
        }}
        return v1.localeCompare(v2);
      }};
      table.querySelectorAll("th").forEach((th, index) => {{
        th.addEventListener("click", () => {{
          const tbody = table.querySelector("tbody");
          const current = th.getAttribute("data-order") === "asc";
          const type = th.getAttribute("data-type") || "string";
          Array.from(tbody.querySelectorAll("tr"))
            .sort(comparer(index, type, !current))
            .forEach(row => tbody.appendChild(row));
          th.setAttribute("data-order", current ? "desc" : "asc");
        }});
      }});
    }})();
  </script>
</body>
</html>
"#
    )
    .context("write report footer")?;
    Ok(())
}

fn html_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

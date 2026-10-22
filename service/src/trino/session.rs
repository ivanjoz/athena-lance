//! Statements answered without running a query, and Trino-only syntax rewritten to SQL the
//! engine understands (trino-api-plan.md §5.1, §5.2, §7.3): session and transaction statements,
//! PREPARE / EXECUTE / DEALLOCATE (prepared statements live in the client and come back in the
//! X-Trino-Prepared-Statement header, so nothing is kept between requests), SHOW / DESCRIBE.

use std::sync::LazyLock;

use regex::Regex;

use super::http::HttpRequest;
use crate::config::TrinoCfg;
use crate::error::{Error, Result, code};

pub enum Action {
    /// Run this SQL.
    Query(String),
    /// Nothing to run: answer with these response headers and Trino's `updateType`.
    Done { update_type: &'static str, headers: Vec<(&'static str, String)> },
}

macro_rules! re {
    ($name:ident, $pattern:expr) => {
        static $name: LazyLock<Regex> = LazyLock::new(|| Regex::new(concat!("(?is)^", $pattern, "$")).unwrap());
    };
}

// A single-quoted SQL string literal ('' escapes a quote) and a possibly quoted identifier.
re!(PREPARE, r"PREPARE\s+(\w+)\s+FROM\s+(.+)");
re!(EXECUTE_IMMEDIATE, r"EXECUTE\s+IMMEDIATE\s+'((?:[^']|'')*)'(?:\s+USING\s+(.+))?");
re!(EXECUTE, r"EXECUTE\s+(\w+)(?:\s+USING\s+(.+))?");
re!(DEALLOCATE, r"DEALLOCATE\s+PREPARE\s+(\w+)");
re!(USE, r#"USE\s+(?:("[^"]+"|\w+)\.)?("[^"]+"|\w+)"#);
re!(SET_SESSION, r"SET\s+SESSION\s+([\w.]+)\s*=\s*(.+)");
re!(RESET_SESSION, r"RESET\s+SESSION\s+([\w.]+)");
re!(TRANSACTION, r"(START\s+TRANSACTION.*|COMMIT(?:\s+WORK)?|ROLLBACK(?:\s+WORK)?)");
re!(SHOW_CATALOGS, r"SHOW\s+CATALOGS(?:\s+LIKE\s+('(?:[^']|'')*'))?");
re!(SHOW_SCHEMAS, r#"SHOW\s+SCHEMAS(?:\s+(?:FROM|IN)\s+("[^"]+"|\w+))?(?:\s+LIKE\s+('(?:[^']|'')*'))?"#);
re!(SHOW_TABLES, r#"SHOW\s+TABLES(?:\s+(?:FROM|IN)\s+([\w."]+))?(?:\s+LIKE\s+('(?:[^']|'')*'))?"#);
re!(SHOW_COLUMNS, r#"(?:SHOW\s+COLUMNS\s+(?:FROM|IN)|DESCRIBE|DESC)\s+([\w."]+)"#);
re!(SHOW_SESSION, r"SHOW\s+SESSION.*");
re!(SHOW_FUNCTIONS, r"SHOW\s+FUNCTIONS.*");

/// An identifier as SQL resolves it: unquoted lower-cased, quoted as written.
fn ident(raw: &str) -> String {
    match raw.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => raw.to_ascii_lowercase(),
    }
}

/// `'text'` as a SQL literal.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn encode(value: &str) -> String {
    form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

pub fn classify(sql: &str, req: &HttpRequest, cfg: &TrinoCfg) -> Result<Action> {
    let done = |update_type, headers| Ok(Action::Done { update_type, headers });
    if let Some(c) = PREPARE.captures(sql) {
        return done("PREPARE", vec![("x-trino-added-prepare", format!("{}={}", &c[1], encode(&c[2])))]);
    }
    if let Some(c) = EXECUTE_IMMEDIATE.captures(sql) {
        let statement = c[1].replace("''", "'");
        return Ok(Action::Query(bind(&statement, c.get(2).map(|m| m.as_str()))?));
    }
    if let Some(c) = EXECUTE.captures(sql) {
        let statement = prepared(req, &c[1])?;
        return Ok(Action::Query(bind(&statement, c.get(2).map(|m| m.as_str()))?));
    }
    if let Some(c) = DEALLOCATE.captures(sql) {
        return done("DEALLOCATE", vec![("x-trino-deallocated-prepare", c[1].to_string())]);
    }
    if let Some(c) = USE.captures(sql) {
        let mut headers = vec![("x-trino-set-schema", ident(&c[2]))];
        if let Some(catalog) = c.get(1) {
            headers.push(("x-trino-set-catalog", ident(catalog.as_str())));
        }
        return done("USE", headers);
    }
    if let Some(c) = SET_SESSION.captures(sql) {
        let value = c[2].trim().trim_matches('\'').to_string();
        return done("SET SESSION", vec![("x-trino-set-session", format!("{}={}", &c[1], encode(&value)))]);
    }
    if let Some(c) = RESET_SESSION.captures(sql) {
        return done("RESET SESSION", vec![("x-trino-clear-session", c[1].to_string())]);
    }
    if let Some(c) = TRANSACTION.captures(sql) {
        let word = c[1].split_whitespace().next().unwrap_or_default().to_ascii_uppercase();
        return done(if word == "START" { "START TRANSACTION" } else if word == "COMMIT" { "COMMIT" } else { "ROLLBACK" }, vec![]);
    }

    let like = |column: &str, pattern: Option<regex::Match>| match pattern {
        Some(p) => format!(" AND {column} LIKE {}", p.as_str()),
        None => String::new(),
    };
    if let Some(c) = SHOW_CATALOGS.captures(sql) {
        return Ok(Action::Query(format!(
            "SELECT table_cat AS \"Catalog\" FROM system.jdbc.catalogs WHERE true{} ORDER BY 1",
            like("table_cat", c.get(1))
        )));
    }
    if let Some(c) = SHOW_SCHEMAS.captures(sql) {
        let catalog = c.get(1).map(|m| ident(m.as_str())).unwrap_or_else(|| cfg.catalog.clone());
        return Ok(Action::Query(format!(
            "SELECT table_schem AS \"Schema\" FROM system.jdbc.schemas WHERE table_catalog = {}{} ORDER BY 1",
            literal(&catalog),
            like("table_schem", c.get(2))
        )));
    }
    if let Some(c) = SHOW_TABLES.captures(sql) {
        let schema = c.get(1).map(|m| ident(m.as_str().rsplit('.').next().unwrap_or_default())).unwrap_or_else(|| cfg.schema.clone());
        return Ok(Action::Query(format!(
            "SELECT table_name AS \"Table\" FROM system.jdbc.tables WHERE table_schem = {}{} ORDER BY 1",
            literal(&schema),
            like("table_name", c.get(2))
        )));
    }
    if let Some(c) = SHOW_COLUMNS.captures(sql) {
        let table = ident(c[1].rsplit('.').next().unwrap_or_default());
        return Ok(Action::Query(format!(
            "SELECT column_name AS \"Column\", type_name AS \"Type\", '' AS \"Extra\", '' AS \"Comment\" \
             FROM system.jdbc.columns WHERE table_name = {} ORDER BY ordinal_position",
            literal(&table)
        )));
    }
    if SHOW_SESSION.is_match(sql) {
        return Ok(Action::Query(
            "SELECT '' AS \"Name\", '' AS \"Value\", '' AS \"Default\", '' AS \"Type\", '' AS \"Description\" WHERE false".into(),
        ));
    }
    if SHOW_FUNCTIONS.is_match(sql) {
        return Ok(Action::Query(
            "SELECT '' AS \"Function\", '' AS \"Return Type\", '' AS \"Argument Types\", '' AS \"Function Type\", \
             true AS \"Deterministic\", '' AS \"Description\" WHERE false"
                .into(),
        ));
    }
    Ok(Action::Query(sql.to_string()))
}

/// The SQL of a prepared statement, from the X-Trino-Prepared-Statement header
/// (`name=urlencoded-sql`, comma-separated).
fn prepared(req: &HttpRequest, name: &str) -> Result<String> {
    req.header("x-trino-prepared-statement")
        .unwrap_or_default()
        .split(',')
        .flat_map(|entry| form_urlencoded::parse(entry.trim().as_bytes()))
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| Error::api(code::SQL_ERROR, format!("prepared statement {name:?} not found")))
}

/// Replaces the `?` placeholders (outside strings and quoted identifiers) with the USING values,
/// which are SQL literals already (`'PE'`, `20`, `DATE '2026-10-01'`).
fn bind(statement: &str, using: Option<&str>) -> Result<String> {
    let values = using.map(split_top_level).unwrap_or_default();
    let mut out = String::with_capacity(statement.len());
    let (mut quote, mut next) = (None, 0);
    for ch in statement.chars() {
        match (quote, ch) {
            (None, '\'' | '"') => quote = Some(ch),
            (Some(q), c) if c == q => quote = None,
            (None, '?') => {
                let value = values.get(next).ok_or_else(|| {
                    Error::api(code::SQL_ERROR, format!("the statement has more parameters than the {} given", values.len()))
                })?;
                out.push_str(value);
                next += 1;
                continue;
            }
            _ => {}
        }
        out.push(ch);
    }
    if next != values.len() {
        return Err(Error::api(code::SQL_ERROR, format!("{} parameters given, the statement has {next}", values.len())));
    }
    Ok(out)
}

/// Splits `a, f(b, c), 'x,y'` at the commas outside parentheses and strings.
fn split_top_level(list: &str) -> Vec<String> {
    let (mut parts, mut current, mut depth, mut quote) = (Vec::new(), String::new(), 0, None);
    for ch in list.chars() {
        match (quote, ch) {
            (None, '\'' | '"') => quote = Some(ch),
            (Some(q), c) if c == q => quote = None,
            (None, '(') => depth += 1,
            (None, ')') => depth -= 1,
            (None, ',') if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(ch);
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn cfg() -> TrinoCfg {
        TrinoCfg {
            user: "u".into(),
            password_sha256: "x".into(),
            catalog: "lance".into(),
            schema: "default".into(),
            server_version: "476".into(),
            max_rows: 10,
            max_result_mb: 1,
            max_stream_seconds: 10,
        }
    }

    fn req(prepared: &str) -> HttpRequest {
        let mut headers = BTreeMap::new();
        headers.insert("x-trino-prepared-statement".into(), prepared.into());
        HttpRequest { method: "POST".into(), path: "/v1/statement".into(), headers, body: String::new(), source_ip: String::new() }
    }

    fn query(sql: &str, r: &HttpRequest) -> String {
        match classify(sql, r, &cfg()).unwrap() {
            Action::Query(q) => q,
            Action::Done { update_type, .. } => panic!("{sql}: done {update_type}"),
        }
    }

    fn headers(sql: &str) -> (&'static str, Vec<(&'static str, String)>) {
        match classify(sql, &req(""), &cfg()).unwrap() {
            Action::Done { update_type, headers } => (update_type, headers),
            Action::Query(q) => panic!("{sql}: query {q}"),
        }
    }

    #[test]
    fn prepare_and_execute_round_trip() {
        let sql = "SELECT id FROM ventas WHERE region = ? AND id < ? AND note <> 'a?b'";
        let (kind, h) = headers(&format!("PREPARE statement1 FROM {sql}"));
        assert_eq!(kind, "PREPARE");
        assert_eq!(h[0].0, "x-trino-added-prepare");
        // The client sends it back (as JDBC does) and executes it with literals.
        let r = req(&format!("other=SELECT+1, {}", h[0].1));
        assert_eq!(
            query("EXECUTE statement1 USING 'P,E', 20", &r),
            "SELECT id FROM ventas WHERE region = 'P,E' AND id < 20 AND note <> 'a?b'"
        );
        assert!(classify("EXECUTE statement1 USING 1", &r, &cfg()).is_err());
        assert!(classify("EXECUTE missing", &r, &cfg()).is_err());
        assert_eq!(query("EXECUTE IMMEDIATE 'SELECT ''x'', ?' USING DATE '2026-10-01'", &r), "SELECT 'x', DATE '2026-10-01'");
    }

    #[test]
    fn session_statements_and_show() {
        assert_eq!(headers("USE lance.\"default\"").1, vec![("x-trino-set-schema", "default".into()), ("x-trino-set-catalog", "lance".into())]);
        assert_eq!(headers("SET SESSION query_max_run_time = '10m'").1, vec![("x-trino-set-session", "query_max_run_time=10m".into())]);
        assert_eq!(headers("START TRANSACTION READ ONLY").0, "START TRANSACTION");
        assert_eq!(headers("ROLLBACK").0, "ROLLBACK");
        let r = req("");
        assert!(query("SHOW TABLES FROM lance.default LIKE 'v%'", &r).contains("table_schem = 'default' AND table_name LIKE 'v%'"));
        assert!(query("DESCRIBE lance.\"default\".ventas", &r).contains("table_name = 'ventas'"));
        assert_eq!(query("SELECT 1", &r), "SELECT 1");
    }
}

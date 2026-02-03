use std::fs;

use anyhow::{anyhow, Context, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};
use snowflake_connector_rs::{
    SnowflakeAuthMethod,
    SnowflakeClient,
    SnowflakeClientConfig,
    SnowflakeRow,
};

use crate::config::{CommonMappingFields, Config, SnowflakeConfig};

/// Logical row abstraction used by the mapping layer.
#[derive(Debug, Clone)]
pub struct LogicalRow {
    pub values: JsonMap<String, JsonValue>,
}

impl LogicalRow {
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.values.get(key)
    }
}

/// Fetch all rows for a given mapping, from either a file or Snowflake.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        return load_rows_from_file(file);
    }

    if let Some(sf_cfg) = &cfg.snowflake {
        return fetch_rows_from_snowflake(sf_cfg, common, watermark).await;
    }

    Err(anyhow!(
        "No supported source configured for mapping {} (need `file` or Snowflake)",
        common.name
    ))
}

async fn fetch_rows_from_snowflake(
    sf_cfg: &SnowflakeConfig,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    let sql = build_sql(common, watermark)?;

    let auth = if let Some(key_path) = &sf_cfg.private_key_path {
        // Key-pair auth: use private_key_path as encrypted PEM and password as key passphrase.
        let pem = std::fs::read_to_string(key_path)
            .with_context(|| format!("Failed to read Snowflake private key from {}", key_path))?;
        let pass_bytes = sf_cfg
            .password
            .as_deref()
            .unwrap_or("")
            .as_bytes()
            .to_vec();
        SnowflakeAuthMethod::KeyPair {
            encrypted_pem: pem,
            password: pass_bytes,
        }
    } else if let Some(pw) = &sf_cfg.password {
        SnowflakeAuthMethod::Password(pw.clone())
    } else {
        return Err(anyhow!(
            "SnowflakeConfig.password or private_key_path must be set for authentication"
        ));
    };

    let config = SnowflakeClientConfig {
        account: sf_cfg.account.clone(),
        warehouse: Some(sf_cfg.warehouse.clone()),
        database: Some(sf_cfg.database.clone()),
        schema: Some(sf_cfg.schema.clone()),
        role: sf_cfg.role.clone(),
        timeout: sf_cfg
            .query_timeout_ms
            .map(|ms| std::time::Duration::from_millis(ms)),
    };

    // Create client and session, then run query
    let client = SnowflakeClient::new(&sf_cfg.user, auth, config)?;
    let session = client.create_session().await?;
    let rows = session.query(sql.as_str()).await?;

    let logical_rows = rows
        .into_iter()
        .map(snowflake_row_to_logical_row)
        .collect::<Result<Vec<_>>>()?;

    Ok(logical_rows)
}

fn build_sql(common: &CommonMappingFields, watermark: Option<&str>) -> Result<String> {
    // If the user provided a full SELECT, we respect it as-is. We don't attempt to inject
    // incremental predicates automatically here.
    if let Some(sel) = &common.source.select {
        return Ok(sel.clone());
    }

    if let Some(table) = &common.source.table {
        let mut sql = format!("SELECT * FROM {}", table);
        let mut has_where = false;
        if let Some(w) = &common.source.r#where {
            sql.push_str(" WHERE ");
            sql.push_str(w);
            has_where = true;
        }

        if let (Some(wm), Some(delta)) = (watermark, &common.delta) {
            let predicate = format!("{} > '{}'", delta.updated_at_column, wm);
            if has_where {
                sql.push_str(" AND ");
                sql.push_str(&predicate);
            } else {
                sql.push_str(" WHERE ");
                sql.push_str(&predicate);
            }
        }

        return Ok(sql);
    }

    Err(anyhow!(
        "Snowflake source for mapping '{}' must specify `source.table` or `source.select`",
        common.name
    ))
}

fn snowflake_row_to_logical_row(row: SnowflakeRow) -> Result<LogicalRow> {
    let mut values = JsonMap::new();

    for name in row.column_names() {
        let name = name.to_string();
        // Try to decode as JSON; fall back to string.
        let json_val: JsonValue = match row.get::<JsonValue>(&name) {
            Ok(v) => v,
            Err(_) => {
                let s: String = row.get(&name)?;
                JsonValue::String(s)
            }
        };
        values.insert(name, json_val);
    }

    Ok(LogicalRow { values })
}

fn load_rows_from_file(path: &str) -> Result<Vec<LogicalRow>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read input file {}", path))?;

    let value: JsonValue = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse JSON input from {}", path))?;

    let arr = value.as_array().cloned().ok_or_else(|| {
        anyhow!("Expected top-level JSON array in input file {}", path)
    })?;

    let mut rows = Vec::with_capacity(arr.len());
    for (idx, v) in arr.into_iter().enumerate() {
        match v {
            JsonValue::Object(map) => rows.push(LogicalRow { values: map }),
            _ => {
                return Err(anyhow!(
                    "Row at index {} in {} is not a JSON object",
                    idx,
                    path
                ));
            }
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CommonMappingFields, Mode, SnowflakeConfig, SourceConfig};
    use anyhow::Result;

    /// Optional Snowflake connectivity smoke test.
    ///
    /// This test will only actually hit Snowflake if the following env vars are set:
    /// - SNOWFLAKE_ACCOUNT
    /// - SNOWFLAKE_USER
    /// - SNOWFLAKE_PASSWORD
    /// - SNOWFLAKE_WAREHOUSE
    /// - SNOWFLAKE_DATABASE
    /// - SNOWFLAKE_SCHEMA
    ///
    /// Otherwise it returns Ok(()) immediately so it doesn't fail in environments
    /// without Snowflake credentials configured.
    #[tokio::test]
    async fn snowflake_connectivity_smoke_test() -> Result<()> {
        let account = match std::env::var("SNOWFLAKE_ACCOUNT") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let user = match std::env::var("SNOWFLAKE_USER") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let password = match std::env::var("SNOWFLAKE_PASSWORD") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let warehouse = match std::env::var("SNOWFLAKE_WAREHOUSE") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let database = match std::env::var("SNOWFLAKE_DATABASE") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let schema = match std::env::var("SNOWFLAKE_SCHEMA") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let sf_cfg = SnowflakeConfig {
            account,
            user,
            password: Some(password),
            private_key_path: None,
            warehouse,
            database,
            schema,
            role: None,
            fetch_batch_size: None,
            query_timeout_ms: Some(10_000),
        };

        let common = CommonMappingFields {
            name: "snowflake_connectivity_smoke".to_string(),
            source: SourceConfig {
                file: None,
                table: None,
                select: Some("SELECT 1 AS ONE".to_string()),
                r#where: None,
            },
            mode: Mode::Full,
            delta: None,
        };

        let rows = fetch_rows_from_snowflake(&sf_cfg, &common, None).await?;
        assert!(!rows.is_empty());
        Ok(())
    }
}

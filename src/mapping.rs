use anyhow::{anyhow, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{EdgeMappingConfig, MatchOn, NodeMappingConfig};
use crate::sink::MappedNode;
use crate::sink_async::MappedEdge;
use crate::source::LogicalRow;

/// Map tabular rows to FalkorDB nodes according to a NodeMappingConfig.
pub fn map_rows_to_nodes(
    rows: &[LogicalRow],
    mapping: &NodeMappingConfig,
) -> Result<Vec<MappedNode>> {
    let mut out = Vec::with_capacity(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let key_value = row
            .get(&mapping.key.column)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "Row {} is missing key column '{}'",
                    idx,
                    mapping.key.column
                )
            })?;

        let mut props = JsonMap::new();
        // Always include key property
        props.insert(mapping.key.property.clone(), key_value.clone());

        for (prop_name, spec) in &mapping.properties {
            let val = row.get(&spec.column).cloned().ok_or_else(|| {
                anyhow!(
                    "Row {} is missing column '{}' required for property '{}'",
                    idx,
                    spec.column,
                    prop_name
                )
            })?;
            props.insert(prop_name.clone(), val);
        }

        out.push(MappedNode { key: key_value, props });
    }

    Ok(out)
}

/// Build a property map for matching endpoints based on MatchOn specs.
fn build_match_props(row: &LogicalRow, specs: &[MatchOn]) -> Result<JsonMap<String, JsonValue>> {
    let mut props = JsonMap::new();
    for spec in specs {
        let val = row
            .get(&spec.column)
            .cloned()
            .ok_or_else(|| anyhow!("Missing column '{}' for endpoint match", spec.column))?;
        props.insert(spec.property.clone(), val);
    }
    Ok(props)
}

/// Map tabular rows to FalkorDB edges according to an EdgeMappingConfig.
pub fn map_rows_to_edges(
    rows: &[LogicalRow],
    mapping: &EdgeMappingConfig,
) -> Result<Vec<MappedEdge>> {
    let mut out = Vec::with_capacity(rows.len());

    for row in rows {
        let from_props = build_match_props(row, &mapping.from.match_on)?;
        let to_props = build_match_props(row, &mapping.to.match_on)?;

        let edge_key = if let Some(edge_key_spec) = &mapping.key {
            Some(
                row.get(&edge_key_spec.column)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "Missing column '{}' for edge key",
                            edge_key_spec.column
                        )
                    })?,
            )
        } else {
            None
        };

        let mut props = JsonMap::new();
        for (prop_name, spec) in &mapping.properties {
            let val = row.get(&spec.column).cloned().ok_or_else(|| {
                anyhow!(
                    "Missing column '{}' required for edge property '{}'",
                    spec.column,
                    prop_name
                )
            })?;
            props.insert(prop_name.clone(), val);
        }

        out.push(MappedEdge {
            from_props,
            to_props,
            edge_key,
            props,
        });
    }

    Ok(out)
}

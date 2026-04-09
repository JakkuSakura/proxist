use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::expr::{eval_predicate, Expr};
use crate::query::{
    infer_select_schema, AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, ResultSet,
    SelectExpr, WindowBound, WindowExpr, WindowSpec,
};
use crate::types::{ColumnSpec, ColumnType, Row, Schema, Value};

#[derive(Debug, Clone)]
struct Table {
    schema: Schema,
    rows: Vec<Row>,
}

#[derive(Debug, Default)]
pub struct MemStore {
    tables: HashMap<String, Table>,
}

#[derive(Debug, Clone)]
enum ProjectOp {
    Column(usize),
    Literal(Value),
    Wildcard,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(Error::InvalidData(format!(
                "table already exists: {name}"
            )));
        }
        self.tables.insert(
            name.to_string(),
            Table {
                schema,
                rows: Vec::new(),
            },
        );
        Ok(())
    }

    pub fn insert(&mut self, table: &str, columns: &[String], rows: &[Vec<Value>]) -> Result<()> {
        let table = self
            .tables
            .entry(table.to_string())
            .or_insert_with(|| Table {
                schema: Schema::new(Vec::new()).expect("empty schema"),
                rows: Vec::new(),
            });

        let mut col_indices = Vec::with_capacity(columns.len());
        for (col_pos, name) in columns.iter().enumerate() {
            let idx = match table.schema.column_index(name) {
                Some(idx) => idx,
                None => {
                    let col_type = infer_column_type_from_rows(rows, col_pos);
                    table
                        .schema
                        .add_column(ColumnSpec {
                            name: name.clone(),
                            col_type,
                            nullable: true,
                        })?;
                    let idx = table.schema.columns().len() - 1;
                    for row in &mut table.rows {
                        row.values.push(Value::Null);
                    }
                    idx
                }
            };
            col_indices.push(idx);
        }

        let schema_len = table.schema.columns().len();
        table.rows.reserve(rows.len());
        for values in rows {
            if values.len() != columns.len() {
                return Err(Error::InvalidData(
                    "insert row length mismatch".to_string(),
                ));
            }
            let mut row = vec![Value::Null; schema_len];
            for (idx, value) in col_indices.iter().zip(values.iter()) {
                row[*idx] = value.clone();
            }
            table.rows.push(Row { values: row });
        }
        Ok(())
    }

    pub fn table_schema(&self, table: &str) -> Option<&Schema> {
        self.tables.get(table).map(|table| &table.schema)
    }

    pub fn table_rows(&self, table: &str) -> Option<&[Row]> {
        self.tables.get(table).map(|table| table.rows.as_slice())
    }

    pub fn table_row_count(&self, table: &str) -> Option<usize> {
        self.tables.get(table).map(|table| table.rows.len())
    }

    pub fn table_row(&self, table: &str, index: usize) -> Option<&Row> {
        self.tables.get(table).and_then(|table| table.rows.get(index))
    }

    pub fn update(
        &mut self,
        table: &str,
        assignments: &[(String, Value)],
        filter: Option<&Expr>,
    ) -> Result<u64> {
        let Some(table) = self.tables.get_mut(table) else {
            return Ok(0);
        };

        let mut assign_indices = Vec::with_capacity(assignments.len());
        for (name, value) in assignments {
            let idx = match table.schema.column_index(name) {
                Some(idx) => idx,
                None => {
                    let col_type = value.column_type();
                    table
                        .schema
                        .add_column(ColumnSpec {
                            name: name.clone(),
                            col_type,
                            nullable: true,
                        })?;
                    let idx = table.schema.columns().len() - 1;
                    for row in &mut table.rows {
                        row.values.push(Value::Null);
                    }
                    idx
                }
            };
            assign_indices.push((idx, value.clone()));
        }

        if filter.is_none() {
            let mut updated = 0u64;
            for row in &mut table.rows {
                for (idx, value) in &assign_indices {
                    row.values[*idx] = value.clone();
                }
                updated += 1;
            }
            return Ok(updated);
        }

        let expr = filter.expect("filter checked");
        let mut updated = 0u64;
        for row in &mut table.rows {
            if !eval_predicate(expr, &row.values, &table.schema)? {
                continue;
            }
            for (idx, value) in &assign_indices {
                row.values[*idx] = value.clone();
            }
            updated += 1;
        }
        Ok(updated)
    }

    pub fn delete(&mut self, table: &str, filter: Option<&Expr>) -> Result<u64> {
        let Some(table) = self.tables.get_mut(table) else {
            return Ok(0);
        };
        if filter.is_none() {
            let count = table.rows.len() as u64;
            table.rows.clear();
            return Ok(count);
        }
        let expr = filter.expect("filter checked");
        let mut removed = 0u64;
        table.rows.retain(|row| match eval_predicate(expr, &row.values, &table.schema) {
            Ok(true) => {
                removed += 1;
                false
            }
            Ok(false) => true,
            Err(_) => true,
        });
        Ok(removed)
    }

    pub fn query(&self, plan: &QueryPlan) -> Result<ResultSet> {
        let left = self
            .tables
            .get(&plan.table)
            .ok_or_else(|| Error::InvalidData("table not found".to_string()))?;
        let mut schema = left.schema.clone();
        let mut rows: Vec<&Row> = left.rows.iter().collect();

        let joined_rows = if let Some(join) = &plan.join {
            let (joined_schema, joined_rows) = self.apply_join(join, &schema, &rows)?;
            schema = joined_schema;
            Some(joined_rows)
        } else {
            None
        };

        if let Some(joined_rows) = &joined_rows {
            rows = joined_rows.iter().collect();
        }

        if let Some(filter) = &plan.filter {
            rows.retain(|row| eval_predicate(filter, &row.values, &schema).unwrap_or(false));
        }

        let has_window = plan.select.iter().any(|item| matches!(item.expr, SelectExpr::Window(_)));
        if has_window && !plan.group_by.is_empty() {
            return Err(Error::InvalidData(
                "window functions cannot be used with group by".to_string(),
            ));
        }

        if has_window {
            return self.apply_window(plan, &schema, &rows);
        }

        let has_aggregate = plan
            .select
            .iter()
            .any(|item| matches!(item.expr, SelectExpr::Aggregate(_)));

        if !plan.group_by.is_empty() || has_aggregate {
            return self.apply_group_by(plan, &schema, &rows);
        }

        let result_schema = Schema::new(
            infer_select_schema(&plan.select, &schema)
                .into_iter()
                .map(|(name, col_type)| ColumnSpec {
                    name,
                    col_type,
                    nullable: true,
                })
                .collect(),
        )?;

        let mut proj_ops = Vec::with_capacity(plan.select.len());
        let mut proj_capacity = 0usize;
        for item in &plan.select {
            match &item.expr {
                SelectExpr::Column(name) => {
                    let idx = schema.column_index(name).ok_or_else(|| {
                        Error::InvalidData(format!("unknown column {name}"))
                    })?;
                    proj_ops.push(ProjectOp::Column(idx));
                    proj_capacity += 1;
                }
                SelectExpr::Literal(value) => {
                    proj_ops.push(ProjectOp::Literal(value.clone()));
                    proj_capacity += 1;
                }
                SelectExpr::Wildcard => {
                    proj_ops.push(ProjectOp::Wildcard);
                    proj_capacity += schema.columns().len();
                }
                SelectExpr::Aggregate(_) => {
                    return Err(Error::InvalidData(
                        "aggregate not allowed without group by".to_string(),
                    ));
                }
                SelectExpr::Window(_) => {
                    return Err(Error::InvalidData(
                        "window not allowed without window context".to_string(),
                    ));
                }
            }
        }

        let mut out_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(proj_capacity);
            for op in &proj_ops {
                match op {
                    ProjectOp::Column(idx) => {
                        out.push(row.values.get(*idx).cloned().unwrap_or(Value::Null));
                    }
                    ProjectOp::Literal(value) => out.push(value.clone()),
                    ProjectOp::Wildcard => out.extend_from_slice(&row.values),
                }
            }
            out_rows.push(Row { values: out });
        }

        Ok(ResultSet {
            schema: result_schema,
            rows: out_rows,
        })
    }

    fn apply_join(
        &self,
        join: &JoinSpec,
        left_schema: &Schema,
        left_rows: &[&Row],
    ) -> Result<(Schema, Vec<Row>)> {
        let right = self
            .tables
            .get(&join.right_table)
            .ok_or_else(|| Error::InvalidData("join table not found".to_string()))?;

        let schema =
            Schema::merge_with_prefix(("left", left_schema), (&join.right_table, &right.schema))?;

        let left_on = left_schema
            .column_index(&join.left_on)
            .ok_or_else(|| Error::InvalidData("join left key not found".to_string()))?;
        let right_on = right
            .schema
            .column_index(&join.right_on)
            .ok_or_else(|| Error::InvalidData("join right key not found".to_string()))?;

        let right_null = right_null_row(&right.schema);
        let mut rows = Vec::with_capacity(left_rows.len());
        for left_row in left_rows {
            let left_row = *left_row;
            let mut matched = false;
            match join.join_type {
                JoinType::Inner | JoinType::LeftOuter => {
                    for right_row in &right.rows {
                        if left_row.values[left_on] == right_row.values[right_on] {
                            matched = true;
                            rows.push(merge_rows(left_row, right_row));
                        }
                    }
                }
                JoinType::Asof => {
                    let left_ts_name = join
                        .left_ts
                        .as_ref()
                        .ok_or_else(|| Error::InvalidData("asof left_ts required".to_string()))?;
                    let right_ts_name = join
                        .right_ts
                        .as_ref()
                        .ok_or_else(|| Error::InvalidData("asof right_ts required".to_string()))?;
                    let left_ts_idx = left_schema
                        .column_index(left_ts_name)
                        .ok_or_else(|| Error::InvalidData("left ts missing".to_string()))?;
                    let right_ts_idx = right
                        .schema
                        .column_index(right_ts_name)
                        .ok_or_else(|| Error::InvalidData("right ts missing".to_string()))?;

                    let mut best: Option<&Row> = None;
                    let mut best_ts: Option<i64> = None;
                    for right_row in &right.rows {
                        if left_row.values[left_on] != right_row.values[right_on] {
                            continue;
                        }
                        let left_ts = left_row.values[left_ts_idx].as_i64().unwrap_or(0);
                        let right_ts = right_row.values[right_ts_idx].as_i64().unwrap_or(0);
                        if right_ts <= left_ts {
                            if best_ts.map(|ts| right_ts > ts).unwrap_or(true) {
                                best_ts = Some(right_ts);
                                best = Some(right_row);
                            }
                        }
                    }
                    if let Some(best_row) = best {
                        matched = true;
                        rows.push(merge_rows(left_row, best_row));
                    }
                }
            }

            if !matched && join.join_type == JoinType::LeftOuter {
                rows.push(merge_rows(left_row, &right_null));
            }
            if !matched && join.join_type == JoinType::Asof {
                rows.push(merge_rows(left_row, &right_null));
            }
        }

        Ok((schema, rows))
    }

    fn apply_group_by(
        &self,
        plan: &QueryPlan,
        schema: &Schema,
        rows: &[&Row],
    ) -> Result<ResultSet> {
        let mut groups: HashMap<Vec<Value>, Vec<&Row>> = HashMap::new();
        let mut key_indices = Vec::with_capacity(plan.group_by.len());
        for col in &plan.group_by {
            let idx = schema.column_index(col).ok_or_else(|| {
                Error::InvalidData(format!("group by column not found: {col}"))
            })?;
            key_indices.push(idx);
        }

        if key_indices.is_empty() {
            groups.insert(Vec::new(), rows.iter().copied().collect());
        } else {
            for row in rows {
                let row = *row;
                let key: Vec<Value> = key_indices
                    .iter()
                    .map(|idx| row.values[*idx].clone())
                    .collect();
                groups.entry(key).or_default().push(row);
            }
        }

        let result_schema = Schema::new(
            infer_select_schema(&plan.select, schema)
                .into_iter()
                .map(|(name, col_type)| ColumnSpec {
                    name,
                    col_type,
                    nullable: true,
                })
                .collect(),
        )?;

        let mut key_pos = vec![None; schema.columns().len()];
        for (pos, idx) in key_indices.iter().enumerate() {
            if let Some(slot) = key_pos.get_mut(*idx) {
                *slot = Some(pos);
            }
        }

        let mut out_rows = Vec::with_capacity(groups.len());
        for (key, group_rows) in groups {
            let mut out = Vec::with_capacity(plan.select.len());
            for item in &plan.select {
                let value = match &item.expr {
                    SelectExpr::Column(name) => {
                        let idx = schema.column_index(name).ok_or_else(|| {
                            Error::InvalidData(format!("unknown column {name}"))
                        })?;
                        if key_indices.is_empty() {
                            group_rows
                                .first()
                                .and_then(|row| row.values.get(idx).cloned())
                                .unwrap_or(Value::Null)
                        } else {
                            let key_idx = key_pos
                                .get(idx)
                                .and_then(|pos| *pos)
                                .ok_or_else(|| {
                                    Error::InvalidData(format!(
                                        "column {name} must appear in group by"
                                    ))
                                })?;
                            key.get(key_idx).cloned().unwrap_or(Value::Null)
                        }
                    }
                    SelectExpr::Aggregate(agg) => aggregate_value(agg, &group_rows, schema)?,
                    SelectExpr::Literal(value) => value.clone(),
                    SelectExpr::Wildcard => {
                        return Err(Error::InvalidData(
                            "wildcard not allowed with group by".to_string(),
                        ));
                    }
                    SelectExpr::Window(_) => {
                        return Err(Error::InvalidData(
                            "window not allowed with group by".to_string(),
                        ));
                    }
                };
                out.push(value);
            }
            out_rows.push(Row { values: out });
        }

        Ok(ResultSet {
            schema: result_schema,
            rows: out_rows,
        })
    }

    fn apply_window(
        &self,
        plan: &QueryPlan,
        schema: &Schema,
        rows: &[&Row],
    ) -> Result<ResultSet> {
        let result_schema = Schema::new(
            infer_select_schema(&plan.select, schema)
                .into_iter()
                .map(|(name, col_type)| ColumnSpec {
                    name,
                    col_type,
                    nullable: true,
                })
                .collect(),
        )?;

        let mut partitions: HashMap<Vec<Value>, Vec<(usize, &Row)>> = HashMap::new();
        let mut partition_indices = Vec::new();
        let mut order_idx = None;

        for item in &plan.select {
            if let SelectExpr::Window(WindowExpr { spec, .. }) = &item.expr {
                partition_indices = spec
                    .partition_by
                    .iter()
                    .filter_map(|col| schema.column_index(col))
                    .collect();
                order_idx = schema.column_index(&spec.order_by);
                break;
            }
        }

        let order_idx = order_idx.ok_or_else(|| Error::InvalidData("window order missing".to_string()))?;

        if partition_indices.is_empty() {
            let bucket = partitions.entry(Vec::new()).or_default();
            for (idx, row) in rows.iter().enumerate() {
                bucket.push((idx, *row));
            }
        } else {
            for (idx, row) in rows.iter().enumerate() {
                let row = *row;
                let key: Vec<Value> = partition_indices
                    .iter()
                    .map(|i| row.values[*i].clone())
                    .collect();
                partitions.entry(key).or_default().push((idx, row));
            }
        }

        let mut out_rows = vec![Row { values: Vec::new() }; rows.len()];
        for (_key, mut part_rows) in partitions {
            part_rows.sort_by(|a, b| {
                a.1.values[order_idx]
                    .cmp_for_order(&b.1.values[order_idx])
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            for (pos, (orig_idx, row)) in part_rows.iter().enumerate() {
                let frame_rows = window_frame_rows(pos, &part_rows, plan)?;
                let values = project_with_window(plan, schema, row, &frame_rows)?;
                out_rows[*orig_idx] = Row { values };
            }
        }

        Ok(ResultSet {
            schema: result_schema,
            rows: out_rows,
        })
    }
}

fn infer_column_type_from_rows(rows: &[Vec<Value>], col_pos: usize) -> ColumnType {
    for row in rows {
        if let Some(value) = row.get(col_pos) {
            if !value.is_null() {
                return value.column_type();
            }
        }
    }
    ColumnType::String
}

fn right_null_row(schema: &Schema) -> Row {
    Row {
        values: vec![Value::Null; schema.columns().len()],
    }
}

fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut values = Vec::with_capacity(left.values.len() + right.values.len());
    values.extend_from_slice(&left.values);
    values.extend_from_slice(&right.values);
    Row { values }
}

fn aggregate_value(
    agg: &AggregateExpr,
    rows: &[&Row],
    schema: &Schema,
) -> Result<Value> {
    match agg.func {
        AggFunc::Count => Ok(Value::I64(rows.len() as i64)),
        AggFunc::Sum => sum_values(agg, rows, schema).map(Value::F64),
        AggFunc::Avg => {
            let sum = sum_values(agg, rows, schema)?;
            let count = rows.len() as f64;
            Ok(Value::F64(if count == 0.0 { 0.0 } else { sum / count }))
        }
        AggFunc::Min => min_max_value(agg, rows, schema, true),
        AggFunc::Max => min_max_value(agg, rows, schema, false),
    }
}

fn sum_values(agg: &AggregateExpr, rows: &[&Row], schema: &Schema) -> Result<f64> {
    let mut sum = 0.0;
    for row in rows {
        let value = crate::expr::eval_value(&agg.arg, &row.values, schema)?;
        if let Some(v) = value.as_f64() {
            sum += v;
        }
    }
    Ok(sum)
}

fn min_max_value(
    agg: &AggregateExpr,
    rows: &[&Row],
    schema: &Schema,
    is_min: bool,
) -> Result<Value> {
    let mut best: Option<Value> = None;
    for row in rows {
        let value = crate::expr::eval_value(&agg.arg, &row.values, schema)?;
        if best.is_none() {
            best = Some(value);
            continue;
        }
        let current = best.as_ref().unwrap();
        let ord = value.cmp_for_order(current);
        if let Some(ord) = ord {
            if (is_min && ord.is_lt()) || (!is_min && ord.is_gt()) {
                best = Some(value);
            }
        }
    }
    Ok(best.unwrap_or(Value::Null))
}

fn window_frame_rows<'a>(
    pos: usize,
    rows: &[(usize, &'a Row)],
    plan: &QueryPlan,
) -> Result<Vec<&'a Row>> {
    let mut spec: Option<&WindowSpec> = None;
    for item in &plan.select {
        if let SelectExpr::Window(WindowExpr { spec: s, .. }) = &item.expr {
            spec = Some(s);
            break;
        }
    }
    let spec = spec.ok_or_else(|| Error::InvalidData("window spec missing".to_string()))?;
    let start = match spec.start {
        WindowBound::UnboundedPreceding => 0,
        WindowBound::CurrentRow => pos,
        WindowBound::Preceding => pos.saturating_sub(spec.start_value.unwrap_or(0) as usize),
    };
    let mut out = Vec::new();
    for idx in start..=pos {
        out.push(rows[idx].1);
    }
    Ok(out)
}

fn project_with_window(
    plan: &QueryPlan,
    schema: &Schema,
    row: &Row,
    frame: &[&Row],
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(plan.select.len());
    for item in &plan.select {
        match &item.expr {
            SelectExpr::Column(name) => {
                let idx = schema.column_index(name).ok_or_else(|| {
                    Error::InvalidData(format!("unknown column {name}"))
                })?;
                out.push(row.values.get(idx).cloned().unwrap_or(Value::Null));
            }
            SelectExpr::Literal(value) => out.push(value.clone()),
            SelectExpr::Window(window) => {
                let agg = AggregateExpr {
                    func: window.func,
                    arg: window.arg.clone(),
                };
                out.push(aggregate_value(&agg, frame, schema)?);
            }
            SelectExpr::Aggregate(_) => {
                return Err(Error::InvalidData(
                    "aggregate not allowed with window".to_string(),
                ));
            }
            SelectExpr::Wildcard => out.extend_from_slice(&row.values),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::MemStore;
    use crate::expr::{BinaryOp, Expr};
    use crate::query::{
        AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, SelectExpr, SelectItem,
        WindowBound, WindowExpr, WindowFrameUnit, WindowSpec,
    };
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};

    #[test]
    fn insert_and_select_roundtrip() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
            },
        ])
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[vec![Value::String("AAPL".to_string()), Value::F64(10.0)]],
        )
        .expect("insert");

        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: Some(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column("symbol".to_string())),
                right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
            }),
            group_by: Vec::new(),
            select: vec![SelectItem {
                expr: SelectExpr::Column("price".to_string()),
                alias: None,
            }],
        };

        let result = mem.query(&plan).expect("query");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::F64(10.0));
    }

    #[test]
    fn table_accessors_expose_rows() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(12.0)],
            ],
        )
        .expect("insert");

        let schema = mem.table_schema("ticks").expect("schema");
        let price_idx = schema.column_index("price").expect("price idx");
        assert_eq!(mem.table_row_count("ticks"), Some(2));

        let rows = mem.table_rows("ticks").expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[price_idx], Value::F64(10.0));

        let row = mem.table_row("ticks", 1).expect("row");
        assert_eq!(row.values[price_idx], Value::F64(12.0));
    }

    #[test]
    fn group_by_aggregate() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("AAPL".to_string()), Value::F64(12.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
            ],
        )
        .expect("insert");

        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: None,
            group_by: vec!["symbol".to_string()],
            select: vec![
                SelectItem {
                    expr: SelectExpr::Column("symbol".to_string()),
                    alias: None,
                },
                SelectItem {
                    expr: SelectExpr::Aggregate(AggregateExpr {
                        func: AggFunc::Avg,
                        arg: Expr::Column("price".to_string()),
                    }),
                    alias: None,
                },
            ],
        };

        let result = mem.query(&plan).expect("query");
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn update_adds_column_and_filters() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
            ],
        )
        .expect("insert");

        let filter = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
        };
        let updated = mem
            .update(
                "ticks",
                &[("notes".to_string(), Value::String("hot".to_string()))],
                Some(&filter),
            )
            .expect("update");
        assert_eq!(updated, 1);

        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: Some(filter),
            group_by: Vec::new(),
            select: vec![SelectItem {
                expr: SelectExpr::Column("notes".to_string()),
                alias: None,
            }],
        };
        let result = mem.query(&plan).expect("query");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values[0],
            Value::String("hot".to_string())
        );
    }

    #[test]
    fn delete_with_filter_removes_matches() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
                vec![Value::String("AAPL".to_string()), Value::F64(12.0)],
            ],
        )
        .expect("insert");

        let filter = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("symbol".to_string())),
            right: Box::new(Expr::Literal(Value::String("MSFT".to_string()))),
        };
        let removed = mem.delete("ticks", Some(&filter)).expect("delete");
        assert_eq!(removed, 1);

        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: None,
            group_by: Vec::new(),
            select: vec![SelectItem {
                expr: SelectExpr::Column("symbol".to_string()),
                alias: None,
            }],
        };
        let result = mem.query(&plan).expect("query");
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn left_outer_join_keeps_unmatched_rows() {
        let mut mem = MemStore::new();
        mem.insert(
            "trades",
            &["symbol".to_string(), "price".to_string()],
            &[
                vec![Value::String("AAPL".to_string()), Value::F64(10.0)],
                vec![Value::String("MSFT".to_string()), Value::F64(5.0)],
            ],
        )
        .expect("insert left");
        mem.insert(
            "quotes",
            &["symbol".to_string(), "bid".to_string()],
            &[vec![Value::String("AAPL".to_string()), Value::F64(9.5)]],
        )
        .expect("insert right");

        let plan = QueryPlan {
            table: "trades".to_string(),
            join: Some(JoinSpec {
                join_type: JoinType::LeftOuter,
                right_table: "quotes".to_string(),
                left_on: "symbol".to_string(),
                right_on: "symbol".to_string(),
                left_ts: None,
                right_ts: None,
            }),
            filter: None,
            group_by: Vec::new(),
            select: vec![SelectItem {
                expr: SelectExpr::Wildcard,
                alias: None,
            }],
        };

        let result = mem.query(&plan).expect("join");
        assert_eq!(result.rows.len(), 2);
        let right_bid_idx = 3;
        let msft_row = result
            .rows
            .iter()
            .find(|row| row.values[0] == Value::String("MSFT".to_string()))
            .expect("msft row");
        assert_eq!(msft_row.values[right_bid_idx], Value::Null);
    }

    #[test]
    fn asof_join_picks_latest_prior_row() {
        let mut mem = MemStore::new();
        mem.insert(
            "trades",
            &["symbol".to_string(), "ts".to_string(), "price".to_string()],
            &[
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(3),
                    Value::F64(10.0),
                ],
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(6),
                    Value::F64(12.0),
                ],
                vec![
                    Value::String("MSFT".to_string()),
                    Value::I64(3),
                    Value::F64(5.0),
                ],
            ],
        )
        .expect("insert left");
        mem.insert(
            "quotes",
            &["symbol".to_string(), "ts".to_string(), "bid".to_string()],
            &[
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(9.5),
                ],
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(4),
                    Value::F64(10.5),
                ],
                vec![
                    Value::String("MSFT".to_string()),
                    Value::I64(5),
                    Value::F64(4.5),
                ],
            ],
        )
        .expect("insert right");

        let plan = QueryPlan {
            table: "trades".to_string(),
            join: Some(JoinSpec {
                join_type: JoinType::Asof,
                right_table: "quotes".to_string(),
                left_on: "symbol".to_string(),
                right_on: "symbol".to_string(),
                left_ts: Some("ts".to_string()),
                right_ts: Some("ts".to_string()),
            }),
            filter: None,
            group_by: Vec::new(),
            select: vec![SelectItem {
                expr: SelectExpr::Wildcard,
                alias: None,
            }],
        };

        let result = mem.query(&plan).expect("asof join");
        assert_eq!(result.rows.len(), 3);
        let right_bid_idx = 5;
        let aapl_ts3 = result
            .rows
            .iter()
            .find(|row| row.values[0] == Value::String("AAPL".to_string()) && row.values[1] == Value::I64(3))
            .expect("aapl ts3");
        assert_eq!(aapl_ts3.values[right_bid_idx], Value::F64(9.5));
        let msft_row = result
            .rows
            .iter()
            .find(|row| row.values[0] == Value::String("MSFT".to_string()))
            .expect("msft row");
        assert_eq!(msft_row.values[right_bid_idx], Value::Null);
    }

    #[test]
    fn window_preceding_frame_computes_avg() {
        let mut mem = MemStore::new();
        mem.insert(
            "ticks",
            &["symbol".to_string(), "ts".to_string(), "price".to_string()],
            &[
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(1),
                    Value::F64(10.0),
                ],
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(2),
                    Value::F64(30.0),
                ],
                vec![
                    Value::String("AAPL".to_string()),
                    Value::I64(3),
                    Value::F64(20.0),
                ],
            ],
        )
        .expect("insert");

        let plan = QueryPlan {
            table: "ticks".to_string(),
            join: None,
            filter: None,
            group_by: Vec::new(),
            select: vec![
                SelectItem {
                    expr: SelectExpr::Column("symbol".to_string()),
                    alias: None,
                },
                SelectItem {
                    expr: SelectExpr::Column("ts".to_string()),
                    alias: None,
                },
                SelectItem {
                    expr: SelectExpr::Window(WindowExpr {
                        func: AggFunc::Avg,
                        arg: Expr::Column("price".to_string()),
                        spec: WindowSpec {
                            partition_by: vec!["symbol".to_string()],
                            order_by: "ts".to_string(),
                            unit: WindowFrameUnit::Rows,
                            start: WindowBound::Preceding,
                            start_value: Some(1),
                        },
                    }),
                    alias: None,
                },
            ],
        };

        let result = mem.query(&plan).expect("window query");
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].values[2], Value::F64(10.0));
        assert_eq!(result.rows[1].values[2], Value::F64(20.0));
        assert_eq!(result.rows[2].values[2], Value::F64(25.0));
    }
}

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::slice;

use crate::error::{Error, Result};
use crate::expr::{eval_predicate, Expr, RowAccess};
use crate::query::{
    infer_select_schema, AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, ResultSet,
    SelectExpr, WindowBound, WindowExpr, WindowSpec,
};
use crate::types::{ColumnSpec, ColumnType, Row, Schema, Value};

#[derive(Debug, Clone)]
struct Table {
    schema: Schema,
    columns: Vec<Vec<Value>>,
    row_count: usize,
}

#[derive(Clone, Copy)]
struct RowView<'a> {
    table: &'a Table,
    row: usize,
}

impl<'a> RowView<'a> {
    fn new(table: &'a Table, row: usize) -> Option<Self> {
        if row < table.row_count {
            Some(Self { table, row })
        } else {
            None
        }
    }
}

impl RowAccess for RowView<'_> {
    fn get_value(&self, idx: usize) -> Option<&Value> {
        self.table
            .columns
            .get(idx)
            .and_then(|col| col.get(self.row))
    }
}

#[derive(Clone, Copy)]
enum RowRef<'a> {
    Table(RowView<'a>),
    Joined(&'a Row),
}

impl RowAccess for RowRef<'_> {
    fn get_value(&self, idx: usize) -> Option<&Value> {
        match self {
            RowRef::Table(view) => view.get_value(idx),
            RowRef::Joined(row) => row.values.get(idx),
        }
    }
}

fn rowref_push_values(out: &mut Vec<Value>, row: &RowRef<'_>, width: usize) {
    for idx in 0..width {
        out.push(row.get_value(idx).cloned().unwrap_or(Value::Null));
    }
}

const INLINE_KEY_CAP: usize = 4;

struct KeyBuf {
    len: usize,
    inline: [MaybeUninit<Value>; INLINE_KEY_CAP],
    heap: Option<Vec<Value>>,
}

impl KeyBuf {
    fn new_empty() -> Self {
        Self {
            len: 0,
            inline: unsafe { MaybeUninit::uninit().assume_init() },
            heap: None,
        }
    }

    fn from_indices(row: &RowRef<'_>, indices: &[usize]) -> Self {
        if indices.is_empty() {
            return Self::new_empty();
        }
        let len = indices.len();
        if len <= INLINE_KEY_CAP {
            let mut inline: [MaybeUninit<Value>; INLINE_KEY_CAP] =
                unsafe { MaybeUninit::uninit().assume_init() };
            for (pos, idx) in indices.iter().enumerate() {
                let value = row.get_value(*idx).cloned().unwrap_or(Value::Null);
                inline[pos].write(value);
            }
            Self {
                len,
                inline,
                heap: None,
            }
        } else {
            let mut values = Vec::with_capacity(len);
            for idx in indices {
                values.push(row.get_value(*idx).cloned().unwrap_or(Value::Null));
            }
            Self {
                len,
                inline: unsafe { MaybeUninit::uninit().assume_init() },
                heap: Some(values),
            }
        }
    }

    fn from_values(values: &[Value]) -> Self {
        if values.is_empty() {
            return Self::new_empty();
        }
        let len = values.len();
        if len <= INLINE_KEY_CAP {
            let mut inline: [MaybeUninit<Value>; INLINE_KEY_CAP] =
                unsafe { MaybeUninit::uninit().assume_init() };
            for (pos, value) in values.iter().enumerate() {
                inline[pos].write(value.clone());
            }
            Self {
                len,
                inline,
                heap: None,
            }
        } else {
            Self {
                len,
                inline: unsafe { MaybeUninit::uninit().assume_init() },
                heap: Some(values.to_vec()),
            }
        }
    }

    fn as_slice(&self) -> &[Value] {
        match &self.heap {
            Some(values) => values.as_slice(),
            None => unsafe {
                slice::from_raw_parts(self.inline.as_ptr() as *const Value, self.len)
            },
        }
    }

    fn get(&self, idx: usize) -> Option<&Value> {
        self.as_slice().get(idx)
    }
}

impl Clone for KeyBuf {
    fn clone(&self) -> Self {
        Self::from_values(self.as_slice())
    }
}

impl Drop for KeyBuf {
    fn drop(&mut self) {
        if self.heap.is_some() {
            return;
        }
        for idx in 0..self.len {
            unsafe {
                self.inline[idx].assume_init_drop();
            }
        }
    }
}

impl PartialEq for KeyBuf {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for KeyBuf {}

impl Hash for KeyBuf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
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
            return Err(Error::InvalidData(format!("table already exists: {name}")));
        }
        self.tables.insert(
            name.to_string(),
            Table {
                schema,
                columns: Vec::new(),
                row_count: 0,
            },
        );
        if let Some(table) = self.tables.get_mut(name) {
            table.columns = vec![Vec::new(); table.schema.columns().len()];
        }
        Ok(())
    }

    pub fn create_table_as(&mut self, name: &str, plan: &QueryPlan) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(Error::InvalidData(format!("table already exists: {name}")));
        }
        let result = {
            let mem_ref: &MemStore = &*self;
            mem_ref.query(plan)?
        };
        let schema = result.schema.clone();
        self.create_table(name, schema)?;
        if !result.rows.is_empty() {
            let columns: Vec<String> = result
                .schema
                .columns()
                .iter()
                .map(|col| col.name.clone())
                .collect();
            let rows: Vec<Vec<Value>> = result.rows.into_iter().map(|row| row.values).collect();
            self.insert(name, &columns, &rows)?;
        }
        Ok(())
    }

    pub fn alter_add_column(&mut self, table: &str, column: ColumnSpec) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        table.schema.add_column(column)?;
        let idx = table.schema.columns().len().saturating_sub(1);
        let default_value = table
            .schema
            .columns()
            .get(idx)
            .and_then(|col| col.default.clone())
            .unwrap_or(Value::Null);
        table.columns.push(vec![default_value; table.row_count]);
        Ok(())
    }

    pub fn alter_drop_column(&mut self, table: &str, column: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        let idx = table.schema.remove_column(column)?;
        if idx < table.columns.len() {
            table.columns.remove(idx);
        }
        Ok(())
    }

    pub fn alter_rename_column(&mut self, table: &str, from: &str, to: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        table.schema.rename_column(from, to)?;
        Ok(())
    }

    pub fn alter_set_default(
        &mut self,
        table: &str,
        column: &str,
        default: Option<Value>,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {table}")))?;
        let idx = table
            .schema
            .column_index(column)
            .ok_or_else(|| Error::InvalidData(format!("unknown column name: {column}")))?;
        table.schema.set_column_default(idx, default)?;
        Ok(())
    }

    pub fn drop_table(&mut self, table: &str) -> Result<()> {
        if self.tables.remove(table).is_none() {
            return Err(Error::InvalidData(format!("table not found: {table}")));
        }
        Ok(())
    }

    pub fn rename_table(&mut self, from: &str, to: &str) -> Result<()> {
        if from == to {
            return Ok(());
        }
        if self.tables.contains_key(to) {
            return Err(Error::InvalidData(format!("table already exists: {to}")));
        }
        let table = self
            .tables
            .remove(from)
            .ok_or_else(|| Error::InvalidData(format!("table not found: {from}")))?;
        self.tables.insert(to.to_string(), table);
        Ok(())
    }

    pub fn insert(&mut self, table: &str, columns: &[String], rows: &[Vec<Value>]) -> Result<()> {
        let table = self
            .tables
            .entry(table.to_string())
            .or_insert_with(|| Table {
                schema: Schema::new(Vec::new()).expect("empty schema"),
                columns: Vec::new(),
                row_count: 0,
            });

        let mut col_indices = Vec::with_capacity(columns.len());
        for (col_pos, name) in columns.iter().enumerate() {
            let idx = match table.schema.column_index(name) {
                Some(idx) => idx,
                None => {
                    let col_type = infer_column_type_from_rows(rows, col_pos);
                    table.schema.add_column(ColumnSpec {
                        name: name.clone(),
                        col_type,
                        nullable: true,
                        default: None,
                    })?;
                    let idx = table.schema.columns().len() - 1;
                    table.columns.push(vec![Value::Null; table.row_count]);
                    idx
                }
            };
            col_indices.push(idx);
        }

        let schema_len = table.schema.columns().len();
        let mut value_pos = vec![None; schema_len];
        for (value_idx, col_idx) in col_indices.iter().enumerate() {
            if let Some(pos) = value_pos.get_mut(*col_idx) {
                *pos = Some(value_idx);
            }
        }
        let defaults = table.schema.default_row();
        for column in &mut table.columns {
            column.reserve(rows.len());
        }

        for values in rows {
            if values.len() != columns.len() {
                return Err(Error::InvalidData("insert row length mismatch".to_string()));
            }
            for (col_idx, column) in table.columns.iter_mut().enumerate() {
                let value = match value_pos.get(col_idx).and_then(|pos| *pos) {
                    Some(value_idx) => values.get(value_idx).unwrap_or(&Value::Null),
                    None => defaults.get(col_idx).unwrap_or(&Value::Null),
                };
                column.push(value.clone());
            }
            table.row_count += 1;
        }
        Ok(())
    }

    pub fn table_schema(&self, table: &str) -> Option<&Schema> {
        self.tables.get(table).map(|table| &table.schema)
    }

    pub fn table_row_count(&self, table: &str) -> Option<usize> {
        self.tables.get(table).map(|table| table.row_count)
    }

    pub fn table_rows(&self, table: &str) -> Option<Vec<Row>> {
        let table = self.tables.get(table)?;
        Some(table_rows(table))
    }

    pub fn table_row(&self, table: &str, index: usize) -> Option<Row> {
        let table = self.tables.get(table)?;
        table_row(table, index)
    }

    pub fn table_column(&self, table: &str, column: &str) -> Option<&[Value]> {
        let table = self.tables.get(table)?;
        let idx = table.schema.column_index(column)?;
        table.columns.get(idx).map(|col| col.as_slice())
    }

    pub fn table_column_at(&self, table: &str, index: usize) -> Option<&[Value]> {
        let table = self.tables.get(table)?;
        table.columns.get(index).map(|col| col.as_slice())
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
                    table.schema.add_column(ColumnSpec {
                        name: name.clone(),
                        col_type,
                        nullable: true,
                        default: None,
                    })?;
                    let idx = table.schema.columns().len() - 1;
                    table.columns.push(vec![Value::Null; table.row_count]);
                    idx
                }
            };
            assign_indices.push((idx, value.clone()));
        }

        if filter.is_none() {
            let mut updated = 0u64;
            for (idx, value) in &assign_indices {
                if let Some(column) = table.columns.get_mut(*idx) {
                    for cell in column.iter_mut() {
                        *cell = value.clone();
                    }
                }
            }
            updated += table.row_count as u64;
            return Ok(updated);
        }

        let expr = filter.expect("filter checked");
        let mut updated = 0u64;
        for row_idx in 0..table.row_count {
            let view = RowView {
                table,
                row: row_idx,
            };
            if !eval_predicate(expr, &view, &table.schema)? {
                continue;
            }
            for (idx, value) in &assign_indices {
                if let Some(column) = table.columns.get_mut(*idx) {
                    if let Some(cell) = column.get_mut(row_idx) {
                        *cell = value.clone();
                    }
                }
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
            let count = table.row_count as u64;
            for column in &mut table.columns {
                column.clear();
            }
            table.row_count = 0;
            return Ok(count);
        }
        let expr = filter.expect("filter checked");
        let mut keep_indices = Vec::with_capacity(table.row_count);
        for row_idx in 0..table.row_count {
            let view = RowView {
                table,
                row: row_idx,
            };
            match eval_predicate(expr, &view, &table.schema) {
                Ok(true) => {}
                Ok(false) => keep_indices.push(row_idx),
                Err(_) => keep_indices.push(row_idx),
            }
        }
        let removed = (table.row_count - keep_indices.len()) as u64;
        for column in &mut table.columns {
            let mut new_col = Vec::with_capacity(keep_indices.len());
            for idx in &keep_indices {
                if let Some(value) = column.get(*idx) {
                    new_col.push(value.clone());
                }
            }
            *column = new_col;
        }
        table.row_count = keep_indices.len();
        Ok(removed)
    }

    pub fn query(&self, plan: &QueryPlan) -> Result<ResultSet> {
        let left = self
            .tables
            .get(&plan.table)
            .ok_or_else(|| Error::InvalidData("table not found".to_string()))?;
        let mut schema = left.schema.clone();
        let left_rows: Vec<RowView> = (0..left.row_count)
            .filter_map(|idx| RowView::new(left, idx))
            .collect();

        let joined_rows = if let Some(join) = &plan.join {
            let (joined_schema, joined_rows) = self.apply_join(join, &schema, &left_rows)?;
            schema = joined_schema;
            Some(joined_rows)
        } else {
            None
        };

        let mut rows: Vec<RowRef> = if let Some(joined_rows) = &joined_rows {
            joined_rows.iter().map(RowRef::Joined).collect()
        } else {
            left_rows.iter().copied().map(RowRef::Table).collect()
        };

        if let Some(filter) = &plan.filter {
            rows.retain(|row| eval_predicate(filter, row, &schema).unwrap_or(false));
        }

        let has_window = plan
            .select
            .iter()
            .any(|item| matches!(item.expr, SelectExpr::Window(_)));
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
                    default: None,
                })
                .collect(),
        )?;

        let mut proj_ops = Vec::with_capacity(plan.select.len());
        let mut proj_capacity = 0usize;
        for item in &plan.select {
            match &item.expr {
                SelectExpr::Column(name) => {
                    let idx = schema
                        .column_index(name)
                        .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
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
                        out.push(row.get_value(*idx).cloned().unwrap_or(Value::Null));
                    }
                    ProjectOp::Literal(value) => out.push(value.clone()),
                    ProjectOp::Wildcard => {
                        rowref_push_values(&mut out, &row, schema.columns().len())
                    }
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
        left_rows: &[RowView<'_>],
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
        let left_width = left_schema.columns().len();
        let right_width = right.schema.columns().len();
        for left_row in left_rows {
            let mut matched = false;
            match join.join_type {
                JoinType::Inner | JoinType::LeftOuter => {
                    for right_idx in 0..right.row_count {
                        let right_row = RowView {
                            table: right,
                            row: right_idx,
                        };
                        let left_key = left_row.get_value(left_on).cloned().unwrap_or(Value::Null);
                        let right_key = right_row
                            .get_value(right_on)
                            .cloned()
                            .unwrap_or(Value::Null);
                        if left_key == right_key {
                            matched = true;
                            rows.push(merge_rows_view(
                                left_row,
                                &right_row,
                                left_width,
                                right_width,
                            ));
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

                    let mut best: Option<RowView> = None;
                    let mut best_ts: Option<i64> = None;
                    for right_idx in 0..right.row_count {
                        let right_row = RowView {
                            table: right,
                            row: right_idx,
                        };
                        let left_key = left_row.get_value(left_on).cloned().unwrap_or(Value::Null);
                        let right_key = right_row
                            .get_value(right_on)
                            .cloned()
                            .unwrap_or(Value::Null);
                        if left_key != right_key {
                            continue;
                        }
                        let left_ts = left_row
                            .get_value(left_ts_idx)
                            .and_then(Value::as_i64)
                            .unwrap_or(0);
                        let right_ts = right_row
                            .get_value(right_ts_idx)
                            .and_then(Value::as_i64)
                            .unwrap_or(0);
                        if right_ts <= left_ts {
                            if best_ts.map(|ts| right_ts > ts).unwrap_or(true) {
                                best_ts = Some(right_ts);
                                best = Some(right_row);
                            }
                        }
                    }
                    if let Some(best_row) = best {
                        matched = true;
                        rows.push(merge_rows_view(
                            left_row,
                            &best_row,
                            left_width,
                            right_width,
                        ));
                    }
                }
            }

            if !matched && join.join_type == JoinType::LeftOuter {
                rows.push(merge_left_with_row(
                    left_row,
                    &right_null,
                    left_width,
                    right_width,
                ));
            }
            if !matched && join.join_type == JoinType::Asof {
                rows.push(merge_left_with_row(
                    left_row,
                    &right_null,
                    left_width,
                    right_width,
                ));
            }
        }

        Ok((schema, rows))
    }

    fn apply_group_by(
        &self,
        plan: &QueryPlan,
        schema: &Schema,
        rows: &[RowRef<'_>],
    ) -> Result<ResultSet> {
        let mut groups: HashMap<KeyBuf, Vec<RowRef>> = HashMap::new();
        let mut key_indices = Vec::with_capacity(plan.group_by.len());
        for col in &plan.group_by {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| Error::InvalidData(format!("group by column not found: {col}")))?;
            key_indices.push(idx);
        }

        if key_indices.is_empty() {
            groups.insert(KeyBuf::new_empty(), rows.iter().copied().collect());
        } else {
            for row in rows {
                let key = KeyBuf::from_indices(row, &key_indices);
                groups.entry(key).or_default().push(*row);
            }
        }

        let result_schema = Schema::new(
            infer_select_schema(&plan.select, schema)
                .into_iter()
                .map(|(name, col_type)| ColumnSpec {
                    name,
                    col_type,
                    nullable: true,
                    default: None,
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
                        let idx = schema
                            .column_index(name)
                            .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
                        if key_indices.is_empty() {
                            group_rows
                                .first()
                                .and_then(|row| row.get_value(idx).cloned())
                                .unwrap_or(Value::Null)
                        } else {
                            let key_idx =
                                key_pos.get(idx).and_then(|pos| *pos).ok_or_else(|| {
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
        rows: &[RowRef<'_>],
    ) -> Result<ResultSet> {
        let result_schema = Schema::new(
            infer_select_schema(&plan.select, schema)
                .into_iter()
                .map(|(name, col_type)| ColumnSpec {
                    name,
                    col_type,
                    nullable: true,
                    default: None,
                })
                .collect(),
        )?;

        let mut partitions: HashMap<KeyBuf, Vec<(usize, RowRef)>> = HashMap::new();
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

        let order_idx =
            order_idx.ok_or_else(|| Error::InvalidData("window order missing".to_string()))?;

        if partition_indices.is_empty() {
            let bucket = partitions.entry(KeyBuf::new_empty()).or_default();
            for (idx, row) in rows.iter().enumerate() {
                bucket.push((idx, *row));
            }
        } else {
            for (idx, row) in rows.iter().enumerate() {
                let key = KeyBuf::from_indices(row, &partition_indices);
                partitions.entry(key).or_default().push((idx, *row));
            }
        }

        let mut out_rows = vec![Row { values: Vec::new() }; rows.len()];
        for (_key, mut part_rows) in partitions {
            part_rows.sort_by(|a, b| {
                let left = a.1.get_value(order_idx).cloned().unwrap_or(Value::Null);
                let right = b.1.get_value(order_idx).cloned().unwrap_or(Value::Null);
                left.cmp_for_order(&right)
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

fn merge_rows_view(
    left: &RowView<'_>,
    right: &RowView<'_>,
    left_width: usize,
    right_width: usize,
) -> Row {
    let mut values = Vec::with_capacity(left_width + right_width);
    for idx in 0..left_width {
        values.push(left.get_value(idx).cloned().unwrap_or(Value::Null));
    }
    for idx in 0..right_width {
        values.push(right.get_value(idx).cloned().unwrap_or(Value::Null));
    }
    Row { values }
}

fn merge_left_with_row(
    left: &RowView<'_>,
    right: &Row,
    left_width: usize,
    right_width: usize,
) -> Row {
    let mut values = Vec::with_capacity(left_width + right_width);
    for idx in 0..left_width {
        values.push(left.get_value(idx).cloned().unwrap_or(Value::Null));
    }
    values.extend_from_slice(&right.values);
    if right_width < right.values.len() {
        values.truncate(left_width + right_width);
    }
    Row { values }
}

fn table_row(table: &Table, index: usize) -> Option<Row> {
    if index >= table.row_count {
        return None;
    }
    let mut values = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        values.push(column.get(index).cloned().unwrap_or(Value::Null));
    }
    Some(Row { values })
}

fn table_rows(table: &Table) -> Vec<Row> {
    let mut rows = Vec::with_capacity(table.row_count);
    for idx in 0..table.row_count {
        if let Some(row) = table_row(table, idx) {
            rows.push(row);
        }
    }
    rows
}

fn aggregate_value(agg: &AggregateExpr, rows: &[RowRef<'_>], schema: &Schema) -> Result<Value> {
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

fn sum_values(agg: &AggregateExpr, rows: &[RowRef<'_>], schema: &Schema) -> Result<f64> {
    let mut sum = 0.0;
    for row in rows {
        let value = crate::expr::eval_value(&agg.arg, row, schema)?;
        if let Some(v) = value.as_f64() {
            sum += v;
        }
    }
    Ok(sum)
}

fn min_max_value(
    agg: &AggregateExpr,
    rows: &[RowRef<'_>],
    schema: &Schema,
    is_min: bool,
) -> Result<Value> {
    let mut best: Option<Value> = None;
    for row in rows {
        let value = crate::expr::eval_value(&agg.arg, row, schema)?;
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
    rows: &[(usize, RowRef<'a>)],
    plan: &QueryPlan,
) -> Result<Vec<RowRef<'a>>> {
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
    let mut out = Vec::with_capacity(pos.saturating_sub(start) + 1);
    for idx in start..=pos {
        out.push(rows[idx].1);
    }
    Ok(out)
}

fn project_with_window(
    plan: &QueryPlan,
    schema: &Schema,
    row: &RowRef<'_>,
    frame: &[RowRef<'_>],
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(plan.select.len());
    for item in &plan.select {
        match &item.expr {
            SelectExpr::Column(name) => {
                let idx = schema
                    .column_index(name)
                    .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
                out.push(row.get_value(idx).cloned().unwrap_or(Value::Null));
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
            SelectExpr::Wildcard => rowref_push_values(&mut out, row, schema.columns().len()),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::MemStore;
    use crate::expr::{BinaryOp, Expr};
    use crate::query::{
        AggFunc, AggregateExpr, JoinSpec, JoinType, QueryPlan, SelectExpr, SelectItem, WindowBound,
        WindowExpr, WindowFrameUnit, WindowSpec,
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
                default: None,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
                default: None,
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
        assert_eq!(result.rows[0].values[0], Value::String("hot".to_string()));
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
    fn alter_column_add_rename_drop() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
                default: None,
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

        mem.alter_add_column(
            "ticks",
            ColumnSpec {
                name: "lot".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: Some(Value::I64(100)),
            },
        )
        .expect("alter add");
        let schema = mem.table_schema("ticks").expect("schema");
        let lot_idx = schema.column_index("lot").expect("lot idx");
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values[lot_idx], Value::I64(100));

        mem.alter_rename_column("ticks", "lot", "lots")
            .expect("rename");
        let schema = mem.table_schema("ticks").expect("schema");
        assert!(schema.column_index("lot").is_none());
        assert!(schema.column_index("lots").is_some());

        mem.alter_drop_column("ticks", "price")
            .expect("drop column");
        let schema = mem.table_schema("ticks").expect("schema");
        assert!(schema.column_index("price").is_none());
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values.len(), schema.columns().len());
    }

    #[test]
    fn alter_set_default_applies_on_insert() {
        let mut mem = MemStore::new();
        let schema = Schema::new(vec![
            ColumnSpec {
                name: "symbol".to_string(),
                col_type: ColumnType::String,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "price".to_string(),
                col_type: ColumnType::F64,
                nullable: false,
                default: None,
            },
        ])
        .expect("schema");
        mem.create_table("ticks", schema).expect("create");
        mem.alter_set_default("ticks", "price", Some(Value::I64(7)))
            .expect("set default");
        mem.insert(
            "ticks",
            &["symbol".to_string()],
            &[vec![Value::String("AAPL".to_string())]],
        )
        .expect("insert");
        let schema = mem.table_schema("ticks").expect("schema");
        let price_idx = schema.column_index("price").expect("price idx");
        let row = mem.table_row("ticks", 0).expect("row");
        assert_eq!(row.values[price_idx], Value::F64(7.0));
    }

    #[test]
    fn create_table_as_select() {
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
                    expr: SelectExpr::Column("price".to_string()),
                    alias: None,
                },
            ],
        };
        mem.create_table_as("snap", &plan).expect("ctas");
        assert!(mem.table_schema("snap").is_some());
        assert_eq!(mem.table_row_count("snap"), Some(2));
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
            .find(|row| {
                row.values[0] == Value::String("AAPL".to_string()) && row.values[1] == Value::I64(3)
            })
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

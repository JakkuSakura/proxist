use crate::expr::Expr;
use crate::types::{ColumnType, Row, Schema, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AggFunc {
    Count = 1,
    Sum = 2,
    Avg = 3,
    Min = 4,
    Max = 5,
}

impl AggFunc {
    pub fn from_u8(value: u8) -> Result<Self, crate::error::Error> {
        match value {
            1 => Ok(AggFunc::Count),
            2 => Ok(AggFunc::Sum),
            3 => Ok(AggFunc::Avg),
            4 => Ok(AggFunc::Min),
            5 => Ok(AggFunc::Max),
            _ => Err(crate::error::Error::Protocol("unknown agg func")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JoinType {
    Inner = 1,
    LeftOuter = 2,
    Asof = 3,
}

impl JoinType {
    pub fn from_u8(value: u8) -> Result<Self, crate::error::Error> {
        match value {
            1 => Ok(JoinType::Inner),
            2 => Ok(JoinType::LeftOuter),
            3 => Ok(JoinType::Asof),
            _ => Err(crate::error::Error::Protocol("unknown join type")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub join_type: JoinType,
    pub right_table: String,
    pub left_on: String,
    pub right_on: String,
    pub left_ts: Option<String>,
    pub right_ts: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggFunc,
    pub arg: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WindowFrameUnit {
    Rows = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WindowBound {
    UnboundedPreceding = 1,
    CurrentRow = 2,
    Preceding = 3,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<String>,
    pub order_by: String,
    pub unit: WindowFrameUnit,
    pub start: WindowBound,
    pub start_value: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WindowExpr {
    pub func: AggFunc,
    pub arg: Expr,
    pub spec: WindowSpec,
}

#[derive(Debug, Clone)]
pub enum SelectExpr {
    Column(String),
    Literal(Value),
    Aggregate(AggregateExpr),
    Window(WindowExpr),
    Wildcard,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub table: String,
    pub join: Option<JoinSpec>,
    pub filter: Option<Expr>,
    pub group_by: Vec<String>,
    pub select: Vec<SelectItem>,
}

#[derive(Debug, Clone)]
pub struct ResultSet {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

pub fn infer_select_schema(select: &[SelectItem], schema: &Schema) -> Vec<(String, ColumnType)> {
    let mut out = Vec::new();
    for item in select {
        match &item.expr {
            SelectExpr::Column(name) => {
                let idx = schema.column_index(name).unwrap_or(0);
                let col = &schema.columns()[idx];
                let alias = item.alias.clone().unwrap_or_else(|| name.clone());
                out.push((alias, col.col_type));
            }
            SelectExpr::Literal(value) => {
                let alias = item.alias.clone().unwrap_or_else(|| "literal".to_string());
                out.push((alias, value.column_type()));
            }
            SelectExpr::Aggregate(agg) => {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", agg.func));
                let col_type = match agg.func {
                    AggFunc::Count => ColumnType::I64,
                    AggFunc::Sum | AggFunc::Avg => ColumnType::F64,
                    AggFunc::Min | AggFunc::Max => match &agg.arg {
                        Expr::Column(name) => schema
                            .column_index(name)
                            .map(|idx| schema.columns()[idx].col_type)
                            .unwrap_or(ColumnType::String),
                        _ => ColumnType::String,
                    },
                };
                out.push((alias, col_type));
            }
            SelectExpr::Window(window) => {
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", window.func));
                let col_type = match window.func {
                    AggFunc::Count => ColumnType::I64,
                    AggFunc::Sum | AggFunc::Avg => ColumnType::F64,
                    AggFunc::Min | AggFunc::Max => match &window.arg {
                        Expr::Column(name) => schema
                            .column_index(name)
                            .map(|idx| schema.columns()[idx].col_type)
                            .unwrap_or(ColumnType::String),
                        _ => ColumnType::String,
                    },
                };
                out.push((alias, col_type));
            }
            SelectExpr::Wildcard => {
                for col in schema.columns() {
                    out.push((col.name.clone(), col.col_type));
                }
            }
        }
    }
    out
}

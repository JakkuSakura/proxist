use fp_core::sql_ast::{
    BinaryOperator as BinOp, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, SelectItem,
    SetExpr, Statement, TableFactor, Value,
};

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub filters: Vec<Predicate>,
    pub order_by: Vec<OrderItem>,
    pub select_cols: Vec<String>,
    pub has_wildcard: bool,
    pub output_items: Vec<OutputItem>,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub supports_hot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    Count,
    Sum,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub kind: AggregateKind,
    pub column: Option<String>,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum OutputItem {
    Column(String),
    Aggregate(AggregateExpr),
}

pub fn rewrite_with_bounds(
    stmt: &Statement,
    order_col: &str,
    lower: Option<i64>,
    upper: Option<i64>,
    limit_hint: Option<usize>,
) -> Option<String> {
    let mut stmt = stmt.clone();

    let append_bounds = |selection: &mut Option<Expr>| {
        let ident = |name: &str| {
            Expr::Identifier(Ident {
                value: name.to_string(),
                quote_style: None,
            })
        };
        let mut clauses = Vec::new();

        if let Some(lo) = lower {
            clauses.push(Expr::BinaryOp {
                left: Box::new(ident(order_col)),
                op: BinOp::GtEq,
                right: Box::new(Expr::Value(Value::Number(lo.to_string(), false))),
            });
        }
        if let Some(hi) = upper {
            clauses.push(Expr::BinaryOp {
                left: Box::new(ident(order_col)),
                op: BinOp::LtEq,
                right: Box::new(Expr::Value(Value::Number(hi.to_string(), false))),
            });
        }
        if clauses.is_empty() {
            return;
        }

        let extra = clauses
            .into_iter()
            .reduce(|a, b| Expr::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            })
            .unwrap();

        if let Some(existing) = selection.take() {
            *selection = Some(Expr::BinaryOp {
                left: Box::new(existing),
                op: BinOp::And,
                right: Box::new(extra),
            });
        } else {
            *selection = Some(extra);
        }
    };

    match &mut stmt {
        Statement::Query(q) => {
            if let Some(limit) = limit_hint {
                q.limit = Some(Expr::Value(Value::Number(limit.to_string(), false)));
            } else {
                q.limit = None;
            }
            q.offset = None;

            if let SetExpr::Select(sel) = &mut *q.body {
                append_bounds(&mut sel.selection);
            } else {
                return None;
            }
        }
        _ => return None,
    }

    Some(stmt.to_string())
}

pub fn parse_table_schema_from_ddl(
    stmt: &Statement,
) -> Option<(String, Vec<(String, super::ColumnType)>)> {
    if let Statement::CreateTable { name, columns, .. } = stmt {
        let table = name.to_string().to_ascii_lowercase();
        let cols = columns
            .iter()
            .map(|col| {
                let name = col.name.value.to_ascii_lowercase();
                let ty = map_data_type(&col.data_type);
                (name, ty)
            })
            .collect::<Vec<_>>();
        return Some((table, cols));
    }
    None
}

fn map_data_type(data_type: &fp_core::sql_ast::DataType) -> super::ColumnType {
    let lowered = data_type.to_string().to_ascii_lowercase();
    if lowered.contains("int") {
        super::ColumnType::Int64
    } else if lowered.contains("float") || lowered.contains("double") {
        super::ColumnType::Float64
    } else if lowered.contains("bool") {
        super::ColumnType::Bool
    } else {
        super::ColumnType::Text
    }
}

#[derive(Debug, Clone)]
pub struct OrderItem {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum Predicate {
    Eq {
        column: String,
        value: String,
    },
    In {
        column: String,
        values: Vec<String>,
    },
    RangeLower {
        column: String,
        inclusive: bool,
        bound: i64,
    },
    RangeUpper {
        column: String,
        inclusive: bool,
        bound: i64,
    },
    Between {
        column: String,
        low: i64,
        high: i64,
    },
}

pub fn build_table_plan(stmt: &Statement) -> Option<TablePlan> {
    let query = match stmt {
        Statement::Query(query) => query,
        _ => return Some(TablePlan::cold()),
    };

    let select = match &*query.body {
        SetExpr::Select(sel) => sel,
        _ => return Some(TablePlan::cold()),
    };

    if select.from.len() != 1 {
        return Some(TablePlan::cold());
    }

    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string().to_ascii_lowercase(),
        _ => return Some(TablePlan::cold()),
    };

    if !select.from[0].joins.is_empty() || select.distinct.is_some() || select.having.is_some() {
        return Some(TablePlan::unsupported(table));
    }

    let group_by_cols = collect_group_by(&select.group_by)?;

    let mut filters = Vec::new();
    let selection_supported = if let Some(selection) = &select.selection {
        collect_predicates(selection, &mut filters)
    } else {
        true
    };

    let mut select_cols = Vec::new();
    let mut output_items = Vec::new();
    let mut aggregates = Vec::new();
    let mut has_wildcard = false;
    for item in &select.projection {
        match item {
            SelectItem::Wildcard => {
                if !group_by_cols.is_empty() {
                    return Some(TablePlan::unsupported(table));
                }
                has_wildcard = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(column) = order_expr_ident(expr) {
                    if !group_by_cols.is_empty()
                        && !group_by_cols
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(&column))
                    {
                        return Some(TablePlan::unsupported(table));
                    }
                    select_cols.push(column.clone());
                    output_items.push(OutputItem::Column(column));
                } else {
                    return Some(TablePlan::unsupported(table));
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(column) = order_expr_ident(expr) {
                    let alias_lower = alias.value.to_ascii_lowercase();
                    if alias_lower != column {
                        return Some(TablePlan::unsupported(table));
                    }
                    if !group_by_cols.is_empty()
                        && !group_by_cols
                            .iter()
                            .any(|c| c.eq_ignore_ascii_case(&column))
                    {
                        return Some(TablePlan::unsupported(table));
                    }
                    select_cols.push(column.clone());
                    output_items.push(OutputItem::Column(column));
                } else if let Expr::Function(func) = expr {
                    let alias_lower = alias.value.to_ascii_lowercase();
                    let Some((kind, column)) = parse_aggregate(func) else {
                        return Some(TablePlan::unsupported(table));
                    };
                    let aggregate = AggregateExpr {
                        kind,
                        column,
                        alias: alias_lower.clone(),
                    };
                    aggregates.push(aggregate.clone());
                    output_items.push(OutputItem::Aggregate(aggregate));
                } else {
                    return Some(TablePlan::unsupported(table));
                }
            }
        }
    }

    let mut order_supported = true;
    let mut order_by = Vec::with_capacity(query.order_by.len());
    for expr in &query.order_by {
        if let Some(column) = order_expr_ident(&expr.expr) {
            order_by.push(OrderItem {
                column,
                descending: matches!(expr.asc, Some(false)),
            });
        } else {
            order_supported = false;
            break;
        }
    }

    if !order_supported {
        return Some(TablePlan::unsupported(table));
    }

    let offset = query
        .offset
        .as_ref()
        .and_then(literal_i64)
        .map(|v| v.max(0) as usize);

    let limit = query
        .limit
        .as_ref()
        .and_then(|expr| literal_i64(expr))
        .map(|v| v.max(0) as usize);

    let supports_hot = selection_supported && filters.iter().all(predicate_supported);

    Some(TablePlan {
        table,
        filters,
        order_by,
        select_cols,
        has_wildcard,
        output_items,
        group_by: group_by_cols,
        aggregates,
        offset,
        limit,
        supports_hot,
    })
}

fn collect_group_by(group_by: &GroupByExpr) -> Option<Vec<String>> {
    match group_by {
        GroupByExpr::Expressions(exprs) => {
            let mut out = Vec::new();
            for expr in exprs {
                if let Some(name) = order_expr_ident(expr) {
                    out.push(name);
                } else {
                    return None;
                }
            }
            Some(out)
        }
    }
}

fn order_expr_ident(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => Some(value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
            parts.last().map(|ident| ident.value.to_ascii_lowercase())
        }
        _ => None,
    }
}

impl TablePlan {
    fn cold() -> Self {
        Self {
            table: String::new(),
            filters: Vec::new(),
            order_by: Vec::new(),
            select_cols: Vec::new(),
            has_wildcard: false,
            output_items: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            offset: None,
            limit: None,
            supports_hot: false,
        }
    }

    fn unsupported(table: String) -> Self {
        Self {
            table,
            filters: Vec::new(),
            order_by: Vec::new(),
            select_cols: Vec::new(),
            has_wildcard: false,
            output_items: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            offset: None,
            limit: None,
            supports_hot: false,
        }
    }
}

fn predicate_supported(predicate: &Predicate) -> bool {
    matches!(
        predicate,
        Predicate::Eq { .. }
            | Predicate::In { .. }
            | Predicate::RangeLower { .. }
            | Predicate::RangeUpper { .. }
            | Predicate::Between { .. }
    )
}

fn collect_predicates(expr: &Expr, out: &mut Vec<Predicate>) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => collect_predicates(left, out) && collect_predicates(right, out),
            BinOp::Eq => {
                if let (Some(col), Some(val)) = (column_ident(left), literal_scalar(right)) {
                    out.push(Predicate::Eq {
                        column: col,
                        value: val,
                    });
                    true
                } else if let (Some(col), Some(val)) = (column_ident(right), literal_scalar(left)) {
                    out.push(Predicate::Eq {
                        column: col,
                        value: val,
                    });
                    true
                } else {
                    false
                }
            }
            BinOp::Gt | BinOp::GtEq => {
                if let (Some(col), Some(bound)) = (column_ident(left), literal_i64(right)) {
                    out.push(Predicate::RangeLower {
                        column: col,
                        inclusive: matches!(op, BinOp::GtEq),
                        bound,
                    });
                    true
                } else {
                    false
                }
            }
            BinOp::Lt | BinOp::LtEq => {
                if let (Some(col), Some(bound)) = (column_ident(left), literal_i64(right)) {
                    out.push(Predicate::RangeUpper {
                        column: col,
                        inclusive: matches!(op, BinOp::LtEq),
                        bound,
                    });
                    true
                } else {
                    false
                }
            }
            _ => false,
        },
        Expr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            if *negated {
                return false;
            }
            if let Some(col) = column_ident(expr) {
                let mut values = Vec::with_capacity(list.len());
                for item in list {
                    if let Some(val) = literal_scalar(item) {
                        values.push(val);
                    } else {
                        return false;
                    }
                }
                if values.is_empty() {
                    false
                } else {
                    out.push(Predicate::In {
                        column: col,
                        values,
                    });
                    true
                }
            } else {
                false
            }
        }
        Expr::Nested(inner) => collect_predicates(inner, out),
        _ => false,
    }
}

fn column_ident(expr: &Expr) -> Option<String> {
    order_expr_ident(expr)
}

fn literal_scalar(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::Number(n, _)) => Some(n.clone()),
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::UnquotedString(s)) => Some(s.clone()),
        Expr::Value(Value::Boolean(b)) => Some(b.to_string()),
        Expr::Value(Value::Null) => Some(String::from("null")),
        _ => None,
    }
}

fn literal_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        Expr::Value(Value::SingleQuotedString(s)) => s.parse().ok(),
        Expr::Value(Value::DoubleQuotedString(s)) => s.parse().ok(),
        Expr::Value(Value::UnquotedString(s)) => s.parse().ok(),
        _ => None,
    }
}

fn parse_aggregate(func: &fp_core::sql_ast::Function) -> Option<(AggregateKind, Option<String>)> {
    let name = func.name.to_string().to_ascii_lowercase();
    match name.as_str() {
        "count" => {
            if func.args.is_empty() {
                return Some((AggregateKind::Count, None));
            }
            if func.args.len() != 1 {
                return None;
            }
            match &func.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    if let Some(col) = order_expr_ident(expr) {
                        Some((AggregateKind::Count, Some(col)))
                    } else {
                        None
                    }
                }
            }
        }
        "sum" => {
            if func.args.len() != 1 {
                return None;
            }
            match &func.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    order_expr_ident(expr).map(|col| (AggregateKind::Sum, Some(col)))
                }
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fp_core::query::SqlDialect;
    use fp_sql::sql_ast::parse_sql_ast;

    #[test]
    fn build_plan_captures_limit_offset() {
        let sql = "SELECT * FROM data WHERE key0 = 'A' AND key1 IN ('B', 'C') AND ts >= 0 AND ts <= 100 ORDER BY ts LIMIT 5 OFFSET 2";
        let mut stmts = parse_sql_ast(sql, SqlDialect::ClickHouse).expect("parse sql");
        let stmt = stmts.remove(0);
        let plan = build_table_plan(&stmt).expect("plan");

        assert_eq!(plan.table, "data");
        assert_eq!(plan.limit, Some(5));
        assert_eq!(plan.offset, Some(2));
        assert_eq!(plan.order_by.len(), 1);
        assert_eq!(plan.order_by[0].column, "ts");
        assert!(plan.supports_hot);

        assert!(plan.filters.iter().any(|predicate| matches!(
            predicate,
            Predicate::Eq { column, value } if column == "key0" && value == "A"
        )));
        assert!(plan.filters.iter().any(|predicate| matches!(
            predicate,
            Predicate::In { column, values } if column == "key1" && values.len() == 2
        )));
        assert!(plan.filters.iter().any(|predicate| matches!(
            predicate,
            Predicate::RangeLower { column, bound, .. } if column == "ts" && *bound == 0
        )));
        assert!(plan.filters.iter().any(|predicate| matches!(
            predicate,
            Predicate::RangeUpper { column, bound, .. } if column == "ts" && *bound == 100
        )));
    }

    #[test]
    fn rewrite_with_bounds_adds_filters_and_limit_hint() {
        let sql = "SELECT * FROM data ORDER BY ts";
        let mut stmts = parse_sql_ast(sql, SqlDialect::ClickHouse).expect("parse sql");
        let stmt = stmts.remove(0);
        let rewritten = rewrite_with_bounds(&stmt, "ts", Some(10), Some(20), Some(15))
            .expect("rewrite with bounds");
        let lower = rewritten.to_ascii_lowercase();
        assert!(lower.contains("ts >= 10"));
        assert!(lower.contains("ts <= 20"));
        assert!(lower.contains("limit 15"));
        assert!(!lower.contains("offset"));
    }
}

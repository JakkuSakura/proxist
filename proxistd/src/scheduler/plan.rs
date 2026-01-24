use fp_sql::{
    ast::{
        BinaryOperator as BinOp, Expr, GroupByExpr, Ident, SelectItem, SetExpr, Statement,
        TableFactor, Value, WildcardAdditionalOptions,
    },
    dialect::ClickHouseDialect,
    parser::Parser,
};

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub filters: Vec<Predicate>,
    pub order_by: Vec<OrderItem>,
    pub select_cols: Vec<String>,
    pub has_wildcard: bool,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
    pub supports_hot: bool,
}

pub fn rewrite_with_bounds(
    sql: &str,
    order_col: &str,
    lower: Option<i64>,
    upper: Option<i64>,
    limit_hint: Option<usize>,
) -> Option<String> {
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    if stmts.is_empty() {
        return None;
    }
    let mut stmt = stmts.remove(0);

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

pub fn parse_table_schema_from_ddl(sql: &str) -> Option<(String, Vec<String>)> {
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    for stmt in stmts.drain(..) {
        if let Statement::CreateTable { name, columns, .. } = stmt {
            let table = name.to_string().to_ascii_lowercase();
            let cols = columns
                .into_iter()
                .map(|col| col.name.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            return Some((table, cols));
        }
    }
    None
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

pub fn build_table_plan(sql: &str) -> Option<TablePlan> {
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    let stmt = stmts.drain(..).next()?;

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

    if query.with.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.from[0].joins.is_empty()
        || select.distinct.is_some()
        || has_group_by(&select.group_by)
        || select.having.is_some()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || !query.limit_by.is_empty()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
    {
        return Some(TablePlan::unsupported(table));
    }

    let mut filters = Vec::new();
    let selection_supported = if let Some(selection) = &select.selection {
        collect_predicates(selection, &mut filters)
    } else {
        true
    };

    let mut select_cols = Vec::new();
    let mut has_wildcard = false;
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(opts) | SelectItem::QualifiedWildcard(_, opts) => {
                if wildcard_has_options(opts) {
                    return Some(TablePlan::unsupported(table));
                }
                has_wildcard = true;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(column) = order_expr_ident(expr) {
                    select_cols.push(column);
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
                    select_cols.push(column);
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
        .and_then(|off| literal_i64(&off.value))
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
        offset,
        limit,
        supports_hot,
    })
}

fn has_group_by(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::All => true,
        GroupByExpr::Expressions(exprs) => !exprs.is_empty(),
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
            offset: None,
            limit: None,
            supports_hot: false,
        }
    }
}

fn wildcard_has_options(opts: &WildcardAdditionalOptions) -> bool {
    opts.opt_exclude.is_some()
        || opts.opt_except.is_some()
        || opts.opt_rename.is_some()
        || opts.opt_replace.is_some()
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
        Expr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            if *negated {
                return false;
            }
            if let (Some(col), Some(lo), Some(hi)) =
                (column_ident(expr), literal_i64(low), literal_i64(high))
            {
                out.push(Predicate::Between {
                    column: col,
                    low: lo,
                    high: hi,
                });
                true
            } else {
                false
            }
        }
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
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::SingleQuotedByteStringLiteral(s))
        | Expr::Value(Value::DoubleQuotedByteStringLiteral(s))
        | Expr::Value(Value::RawStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s))
        | Expr::Value(Value::HexStringLiteral(s))
        | Expr::Value(Value::UnQuotedString(s)) => Some(s.clone()),
        Expr::Value(Value::DollarQuotedString(dollar)) => Some(dollar.value.clone()),
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
        Expr::Value(Value::UnQuotedString(s)) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_plan_captures_limit_offset() {
        let sql = "SELECT * FROM data WHERE key0 = 'A' AND key1 IN ('B', 'C') AND ts >= 0 AND ts <= 100 ORDER BY ts LIMIT 5 OFFSET 2";
        let plan = build_table_plan(sql).expect("plan");

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
        let rewritten = rewrite_with_bounds(sql, "ts", Some(10), Some(20), Some(15))
            .expect("rewrite with bounds");
        let lower = rewritten.to_ascii_lowercase();
        assert!(lower.contains("ts >= 10"));
        assert!(lower.contains("ts <= 20"));
        assert!(lower.contains("limit 15"));
        assert!(!lower.contains("offset"));
    }
}

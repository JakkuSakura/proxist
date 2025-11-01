use fp_sql::{
    ast::{BinaryOperator as BinOp, Expr, Ident, SetExpr, Statement, TableFactor, Value},
    dialect::ClickHouseDialect,
    parser::Parser,
};

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub filter_predicates: Vec<Predicate>,
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
    let stmts = Parser::parse_sql(&ClickHouseDialect {}, sql).ok()?;
    let statement = stmts.first()?;

    let select = match statement {
        Statement::Query(query) => match &*query.body {
            SetExpr::Select(select) => select,
            _ => return None,
        },
        _ => return None,
    };

    if select.from.len() != 1 {
        return None;
    }

    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return None,
    };

    let mut predicates = Vec::new();
    if let Some(selection) = &select.selection {
        collect_predicates(selection, &mut predicates);
    }

    Some(TablePlan {
        table,
        filter_predicates: predicates,
    })
}

fn collect_predicates(expr: &Expr, out: &mut Vec<Predicate>) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                collect_predicates(left, out);
                collect_predicates(right, out);
            }
            BinOp::Eq => {
                if let (Some(col), Some(val)) = (column_ident(left), literal_string(right)) {
                    out.push(Predicate::Eq {
                        column: col,
                        value: val,
                    });
                }
            }
            BinOp::Gt | BinOp::GtEq => {
                if let (Some(col), Some(bound)) = (column_ident(left), literal_i64(right)) {
                    out.push(Predicate::RangeLower {
                        column: col,
                        inclusive: matches!(op, BinOp::GtEq),
                        bound,
                    });
                }
            }
            BinOp::Lt | BinOp::LtEq => {
                if let (Some(col), Some(bound)) = (column_ident(left), literal_i64(right)) {
                    out.push(Predicate::RangeUpper {
                        column: col,
                        inclusive: matches!(op, BinOp::LtEq),
                        bound,
                    });
                }
            }
            _ => {}
        },
        Expr::Between {
            expr, low, high, ..
        } => {
            if let (Some(col), Some(lo), Some(hi)) =
                (column_ident(expr), literal_i64(low), literal_i64(high))
            {
                out.push(Predicate::Between {
                    column: col,
                    low: lo,
                    high: hi,
                });
            }
        }
        Expr::InList { expr, list, .. } => {
            if let Some(col) = column_ident(expr) {
                let values: Vec<String> = list.iter().filter_map(literal_string).collect();
                if !values.is_empty() {
                    out.push(Predicate::In {
                        column: col,
                        values,
                    });
                }
            }
        }
        _ => {}
    }
}

fn column_ident(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => Some(value.to_ascii_lowercase()),
        _ => None,
    }
}

fn literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
        _ => None,
    }
}

fn literal_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        Expr::Value(Value::SingleQuotedString(s)) => s.parse().ok(),
        _ => None,
    }
}

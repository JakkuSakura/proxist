use crate::error::{Error, Result};
use crate::types::{Row, Schema, Value};

pub trait RowAccess {
    fn get_value(&self, idx: usize) -> Option<&Value>;
}

impl RowAccess for [Value] {
    fn get_value(&self, idx: usize) -> Option<&Value> {
        self.get(idx)
    }
}

impl RowAccess for Vec<Value> {
    fn get_value(&self, idx: usize) -> Option<&Value> {
        self.get(idx)
    }
}

impl RowAccess for Row {
    fn get_value(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BinaryOp {
    Eq = 1,
    NotEq = 2,
    Lt = 3,
    LtEq = 4,
    Gt = 5,
    GtEq = 6,
}

impl BinaryOp {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(BinaryOp::Eq),
            2 => Ok(BinaryOp::NotEq),
            3 => Ok(BinaryOp::Lt),
            4 => Ok(BinaryOp::LtEq),
            5 => Ok(BinaryOp::Gt),
            6 => Ok(BinaryOp::GtEq),
            _ => Err(Error::Protocol("unknown binary op")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(String),
    Literal(Value),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

pub fn eval_predicate<R: RowAccess + ?Sized>(
    expr: &Expr,
    row: &R,
    schema: &Schema,
) -> Result<bool> {
    match expr {
        Expr::Binary { op, left, right } => {
            let left_val = eval_value(left, row, schema)?;
            let right_val = eval_value(right, row, schema)?;
            Ok(compare_values(op, &left_val, &right_val)?)
        }
        Expr::And(left, right) => Ok(
            eval_predicate(left, row, schema)? && eval_predicate(right, row, schema)?,
        ),
        Expr::Or(left, right) => Ok(
            eval_predicate(left, row, schema)? || eval_predicate(right, row, schema)?,
        ),
        Expr::Not(inner) => Ok(!eval_predicate(inner, row, schema)?),
        Expr::Literal(Value::Bool(value)) => Ok(*value),
        _ => Err(Error::InvalidData(
            "predicate expression is not boolean".to_string(),
        )),
    }
}

pub fn eval_value<R: RowAccess + ?Sized>(
    expr: &Expr,
    row: &R,
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Column(name) => {
            let idx = schema
                .column_index(name)
                .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
            Ok(row
                .get_value(idx)
                .cloned()
                .ok_or_else(|| Error::InvalidData("column out of range".to_string()))?)
        }
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Binary { op, left, right } => {
            let left_val = eval_value(left, row, schema)?;
            let right_val = eval_value(right, row, schema)?;
            Ok(Value::Bool(compare_values(op, &left_val, &right_val)?))
        }
        Expr::And(_, _) | Expr::Or(_, _) | Expr::Not(_) => {
            Ok(Value::Bool(eval_predicate(expr, row, schema)?))
        }
    }
}

fn compare_values(op: &BinaryOp, left: &Value, right: &Value) -> Result<bool> {
    match op {
        BinaryOp::Eq => Ok(left == right),
        BinaryOp::NotEq => Ok(left != right),
        BinaryOp::Lt => compare_ordering(left, right, |o| o.is_lt()),
        BinaryOp::LtEq => compare_ordering(left, right, |o| o.is_le()),
        BinaryOp::Gt => compare_ordering(left, right, |o| o.is_gt()),
        BinaryOp::GtEq => compare_ordering(left, right, |o| o.is_ge()),
    }
}

fn compare_ordering<F>(left: &Value, right: &Value, pred: F) -> Result<bool>
where
    F: FnOnce(std::cmp::Ordering) -> bool,
{
    let ord = left
        .cmp_for_order(right)
        .ok_or_else(|| Error::InvalidData("values not comparable".to_string()))?;
    Ok(pred(ord))
}

#[cfg(test)]
mod tests {
    use super::{eval_predicate, eval_value, BinaryOp, Expr};
    use crate::types::{ColumnSpec, ColumnType, Schema, Value};

    fn base_schema() -> Schema {
        Schema::new(vec![
            ColumnSpec {
                name: "a".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "b".to_string(),
                col_type: ColumnType::I64,
                nullable: false,
                default: None,
            },
            ColumnSpec {
                name: "sym".to_string(),
                col_type: ColumnType::String,
                nullable: false,
                default: None,
            },
        ])
        .expect("schema")
    }

    #[test]
    fn eval_predicate_and_or_not() {
        let schema = base_schema();
        let row = vec![
            Value::I64(7),
            Value::I64(3),
            Value::String("AAPL".to_string()),
        ];

        let expr = Expr::And(
            Box::new(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column("a".to_string())),
                right: Box::new(Expr::Literal(Value::I64(1))),
            }),
            Box::new(Expr::Or(
                Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Column("sym".to_string())),
                    right: Box::new(Expr::Literal(Value::String("AAPL".to_string()))),
                }),
                Box::new(Expr::Not(Box::new(Expr::Binary {
                    op: BinaryOp::Lt,
                    left: Box::new(Expr::Column("b".to_string())),
                    right: Box::new(Expr::Literal(Value::I64(0))),
                }))),
            )),
        );

        let result = eval_predicate(&expr, &row, &schema).expect("predicate");
        assert!(result);

        let value_expr = Expr::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(Expr::Column("b".to_string())),
            right: Box::new(Expr::Literal(Value::I64(3))),
        };
        let value = eval_value(&value_expr, &row, &schema).expect("value");
        assert_eq!(value, Value::Bool(true));
    }

    #[test]
    fn eval_predicate_non_comparable() {
        let schema = base_schema();
        let row = vec![
            Value::I64(7),
            Value::I64(3),
            Value::String("AAPL".to_string()),
        ];

        let expr = Expr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expr::Column("sym".to_string())),
            right: Box::new(Expr::Column("a".to_string())),
        };
        let err = eval_predicate(&expr, &row, &schema).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("not comparable"),
            "unexpected error: {msg}"
        );
    }
}

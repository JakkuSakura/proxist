use crate::error::{Error, Result};
use crate::types::{Schema, Value, ValueRef};

pub trait ColumnAccess {
    fn value_at(&self, col_idx: usize, row_idx: usize) -> Option<ValueRef<'_>>;
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

pub fn eval_predicate_at<A: ColumnAccess + ?Sized>(
    expr: &Expr,
    access: &A,
    schema: &Schema,
    row_idx: usize,
) -> Result<bool> {
    match expr {
        Expr::Binary { op, left, right } => {
            let left_val = eval_value_ref(left, access, schema, row_idx)?;
            let right_val = eval_value_ref(right, access, schema, row_idx)?;
            Ok(compare_values(op, &left_val, &right_val)?)
        }
        Expr::And(left, right) => Ok(
            eval_predicate_at(left, access, schema, row_idx)?
                && eval_predicate_at(right, access, schema, row_idx)?,
        ),
        Expr::Or(left, right) => Ok(
            eval_predicate_at(left, access, schema, row_idx)?
                || eval_predicate_at(right, access, schema, row_idx)?,
        ),
        Expr::Not(inner) => Ok(!eval_predicate_at(inner, access, schema, row_idx)?),
        Expr::Literal(Value::Bool(value)) => Ok(*value),
        _ => Err(Error::InvalidData(
            "predicate expression is not boolean".to_string(),
        )),
    }
}

fn eval_value_ref<'a, A: ColumnAccess + ?Sized>(
    expr: &'a Expr,
    access: &'a A,
    schema: &Schema,
    row_idx: usize,
) -> Result<ValueRef<'a>> {
    match expr {
        Expr::Column(name) => {
            let idx = schema
                .column_index(name)
                .ok_or_else(|| Error::InvalidData(format!("unknown column {name}")))?;
            Ok(access
                .value_at(idx, row_idx)
                .ok_or_else(|| Error::InvalidData("column out of range".to_string()))?)
        }
        Expr::Literal(value) => Ok(ValueRef::from(value)),
        Expr::Binary { op, left, right } => {
            let left_val = eval_value_ref(left, access, schema, row_idx)?;
            let right_val = eval_value_ref(right, access, schema, row_idx)?;
            Ok(ValueRef::Bool(compare_values(op, &left_val, &right_val)?))
        }
        Expr::And(_, _) | Expr::Or(_, _) | Expr::Not(_) => {
            Ok(ValueRef::Bool(eval_predicate_at(
                expr, access, schema, row_idx,
            )?))
        }
    }
}

pub fn eval_value_at<A: ColumnAccess + ?Sized>(
    expr: &Expr,
    access: &A,
    schema: &Schema,
    row_idx: usize,
) -> Result<Value> {
    Ok(eval_value_ref(expr, access, schema, row_idx)?.to_value())
}

pub fn compare_values(op: &BinaryOp, left: &ValueRef<'_>, right: &ValueRef<'_>) -> Result<bool> {
    match op {
        BinaryOp::Eq => Ok(left == right),
        BinaryOp::NotEq => Ok(left != right),
        BinaryOp::Lt => compare_ordering(left, right, |o| o.is_lt()),
        BinaryOp::LtEq => compare_ordering(left, right, |o| o.is_le()),
        BinaryOp::Gt => compare_ordering(left, right, |o| o.is_gt()),
        BinaryOp::GtEq => compare_ordering(left, right, |o| o.is_ge()),
    }
}

fn compare_ordering<F>(left: &ValueRef<'_>, right: &ValueRef<'_>, pred: F) -> Result<bool>
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
    use super::{eval_predicate_at, eval_value_at, BinaryOp, ColumnAccess, Expr};
    use crate::types::{ColumnSpec, ColumnType, Schema, Value, ValueRef};

    struct SingleRow<'a> {
        row: &'a [Value],
    }

    impl<'a> ColumnAccess for SingleRow<'a> {
        fn value_at(&self, col_idx: usize, row_idx: usize) -> Option<ValueRef<'_>> {
            if row_idx != 0 {
                return None;
            }
            self.row.get(col_idx).map(ValueRef::from)
        }
    }

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

        let access = SingleRow { row: &row };
        let result = eval_predicate_at(&expr, &access, &schema, 0).expect("predicate");
        assert!(result);

        let value_expr = Expr::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(Expr::Column("b".to_string())),
            right: Box::new(Expr::Literal(Value::I64(3))),
        };
        let value = eval_value_at(&value_expr, &access, &schema, 0).expect("value");
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
        let access = SingleRow { row: &row };
        let err = eval_predicate_at(&expr, &access, &schema, 0).expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("not comparable"),
            "unexpected error: {msg}"
        );
    }
}

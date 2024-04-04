use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::{
    common::{tree_node::TreeNodeRewriter, DFSchema},
    error::DataFusionError,
    logical_expr::{expr_rewriter::rewrite_preserving_name, LogicalPlan, Operator},
    optimizer::{OptimizerConfig, OptimizerRule},
    prelude::{binary_expr, lit, Expr},
    scalar::ScalarValue,
};

/// Replaces InfluxDB expressions in where clause with a matcher for series_id if those
/// expressions can be matched to fewer than 100 series ids.
#[derive(Debug, Clone)]
pub struct WhereClauseToSeriesId {}

impl WhereClauseToSeriesId {
    /// Create new optimizer rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for WhereClauseToSeriesId {
    fn name(&self) -> &str {
        "where_clause_to_series_id"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        optimize(plan).map(Some)
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let mut schema =
        new_inputs
            .iter()
            .map(|input| input.schema())
            .fold(DFSchema::empty(), |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            });

    schema.merge(plan.schema());

    let mut expr_rewriter = WhereClauseToSeriesId {};

    let new_exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| rewrite_preserving_name(expr, &mut expr_rewriter))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    plan.with_new_exprs(new_exprs, new_inputs)
}

impl TreeNodeRewriter for WhereClauseToSeriesId {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        match expr {
            // Expr::ScalarFunction(ScalarFunction { func_def, mut args }) => {
            //     let name = func_def.name();
            //     if (args.len() == 2)
            //         && ((name == REGEX_MATCH_UDF_NAME) || (name == REGEX_NOT_MATCH_UDF_NAME))
            //     {
            //         if let Expr::Literal(ScalarValue::Utf8(Some(s))) = &args[1] {
            //             let s = clean_non_meta_escapes(s);
            //             let op = match name {
            //                 REGEX_MATCH_UDF_NAME => Operator::RegexMatch,
            //                 REGEX_NOT_MATCH_UDF_NAME => Operator::RegexNotMatch,
            //                 _ => unreachable!(),
            //             };
            //             return Ok(binary_expr(args.remove(0), op, lit(s)));
            //         }
            //     }
            //
            //     Ok(Expr::ScalarFunction(ScalarFunction { func_def, args }))
            // }
            _ => Ok(expr),
        }
    }
}

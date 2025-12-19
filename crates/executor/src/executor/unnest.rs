use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{PlanSchema, UnnestColumn};
use yachtsql_storage::Table;

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_unnest(
        &mut self,
        input: &ExecutorPlan,
        columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&input_schema);

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        for record in input_table.rows()? {
            let mut base_values = record.values().to_vec();

            if columns.is_empty() {
                result.push_row(base_values)?;
                continue;
            }

            let first_col = &columns[0];
            let array_val = evaluator.evaluate(&first_col.expr, &record)?;

            match array_val {
                Value::Array(elements) => {
                    if elements.is_empty() {
                        continue;
                    }
                    for (idx, elem) in elements.iter().enumerate() {
                        let mut row = base_values.clone();
                        row.push(elem.clone());
                        if first_col.with_offset {
                            row.push(Value::Int64(idx as i64));
                        }
                        result.push_row(row)?;
                    }
                }
                Value::Null => {
                    continue;
                }
                _ => {
                    return Err(Error::InvalidQuery("UNNEST requires array argument".into()));
                }
            }
        }

        Ok(result)
    }
}

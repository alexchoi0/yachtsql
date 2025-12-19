use yachtsql_common::error::{Error, Result};
use yachtsql_storage::Table;

use super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub fn execute_scan(&self, table_name: &str) -> Result<Table> {
        if let Some(cte_table) = self.cte_results.get(table_name) {
            return Ok(cte_table.clone());
        }

        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        Ok(table.clone())
    }
}

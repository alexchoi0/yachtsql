use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{AlterTableOp, ColumnDef, ExportOptions, FunctionArg, FunctionBody};
use yachtsql_storage::{Field, FieldMode, Schema, Table};

use super::PlanExecutor;
use crate::catalog::UserFunction;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<Table> {
        if self.catalog.get_table(table_name).is_some() {
            if or_replace {
                self.catalog.drop_table(table_name)?;
            } else if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }

        let mut schema = Schema::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            schema.add_field(Field::new(&col.name, col.data_type.clone(), mode));
        }

        let table = Table::empty(schema);
        self.catalog.insert_table(table_name, table)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<Table> {
        if self.catalog.get_table(table_name).is_none() {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        self.catalog.drop_table(table_name)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_table(
        &mut self,
        table_name: &str,
        operation: &AlterTableOp,
    ) -> Result<Table> {
        let _table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        match operation {
            AlterTableOp::AddColumn { column } => Err(Error::UnsupportedFeature(
                "ALTER TABLE ADD COLUMN not yet implemented".into(),
            )),
            AlterTableOp::DropColumn { name } => Err(Error::UnsupportedFeature(
                "ALTER TABLE DROP COLUMN not yet implemented".into(),
            )),
            AlterTableOp::RenameColumn { old_name, new_name } => Err(Error::UnsupportedFeature(
                "ALTER TABLE RENAME COLUMN not yet implemented".into(),
            )),
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::SetOptions { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::AlterColumn { .. } => Err(Error::UnsupportedFeature(
                "ALTER TABLE ALTER COLUMN not yet implemented".into(),
            )),
        }
    }

    pub fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_view(
        &mut self,
        name: &str,
        query: &ExecutorPlan,
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        let query_str = format!("{:?}", query);
        self.catalog
            .create_view(name, query_str, Vec::new(), or_replace, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<Table> {
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_schema(
        &mut self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
    ) -> Result<Table> {
        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: false,
        };
        self.catalog.create_function(func, or_replace)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_function(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!("Function not found: {}", name)));
        }
        self.catalog.drop_function(name)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_export(
        &mut self,
        options: &ExportOptions,
        query: &ExecutorPlan,
    ) -> Result<Table> {
        let _data = self.execute_plan(query)?;
        Err(Error::UnsupportedFeature(
            "EXPORT DATA not yet implemented in new executor".into(),
        ))
    }
}

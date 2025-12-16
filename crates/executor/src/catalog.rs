//! In-memory catalog for storing table metadata and data.

use std::collections::HashMap;

use sqlparser::ast::{ConditionalStatements, Expr, OperateFunctionArg, ProcedureParam};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_storage::{Schema, Table};

#[derive(Debug, Clone)]
pub struct UserFunction {
    pub name: String,
    pub parameters: Vec<OperateFunctionArg>,
    pub return_type: DataType,
    pub body: Expr,
    pub is_temporary: bool,
}

#[derive(Debug, Clone)]
pub struct UserProcedure {
    pub name: String,
    pub parameters: Vec<ProcedureParam>,
    pub body: ConditionalStatements,
}

#[derive(Debug, Default)]
pub struct Catalog {
    tables: HashMap<String, Table>,
    functions: HashMap<String, UserFunction>,
    procedures: HashMap<String, UserProcedure>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            functions: HashMap::new(),
            procedures: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<()> {
        let key = name.to_uppercase();
        if self.tables.contains_key(&key) {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                name
            )));
        }
        self.tables.insert(key, Table::new(schema));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.tables.remove(&key).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(&name.to_uppercase())
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(&name.to_uppercase())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_uppercase())
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let old_key = old_name.to_uppercase();
        let new_key = new_name.to_uppercase();

        if !self.tables.contains_key(&old_key) {
            return Err(Error::TableNotFound(old_name.to_string()));
        }
        if self.tables.contains_key(&new_key) {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                new_name
            )));
        }

        if let Some(table) = self.tables.remove(&old_key) {
            self.tables.insert(new_key, table);
        }
        Ok(())
    }

    pub fn create_function(&mut self, func: UserFunction, or_replace: bool) -> Result<()> {
        let key = func.name.to_uppercase();
        if self.functions.contains_key(&key) && !or_replace {
            return Err(Error::invalid_query(format!(
                "Function already exists: {}",
                func.name
            )));
        }
        self.functions.insert(key, func);
        Ok(())
    }

    pub fn drop_function(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.functions.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Function not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_function(&self, name: &str) -> Option<&UserFunction> {
        self.functions.get(&name.to_uppercase())
    }

    pub fn function_exists(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_uppercase())
    }

    pub fn create_procedure(&mut self, proc: UserProcedure, or_replace: bool) -> Result<()> {
        let key = proc.name.to_uppercase();
        if self.procedures.contains_key(&key) && !or_replace {
            return Err(Error::invalid_query(format!(
                "Procedure already exists: {}",
                proc.name
            )));
        }
        self.procedures.insert(key, proc);
        Ok(())
    }

    pub fn drop_procedure(&mut self, name: &str) -> Result<()> {
        let key = name.to_uppercase();
        if self.procedures.remove(&key).is_none() {
            return Err(Error::invalid_query(format!(
                "Procedure not found: {}",
                name
            )));
        }
        Ok(())
    }

    pub fn get_procedure(&self, name: &str) -> Option<&UserProcedure> {
        self.procedures.get(&name.to_uppercase())
    }

    pub fn procedure_exists(&self, name: &str) -> bool {
        self.procedures.contains_key(&name.to_uppercase())
    }

    pub fn get_functions(&self) -> &HashMap<String, UserFunction> {
        &self.functions
    }
}

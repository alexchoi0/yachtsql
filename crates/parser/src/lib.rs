mod error;
mod expr_planner;
mod planner;

pub use error::PlannerError;
pub use planner::Planner;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use yachtsql_common::error::Result;
use yachtsql_ir::LogicalPlan;
use yachtsql_storage::Schema;

pub struct ViewDefinition {
    pub query: String,
    pub column_aliases: Vec<String>,
}

pub trait CatalogProvider {
    fn get_table_schema(&self, name: &str) -> Option<Schema>;
    fn get_view(&self, name: &str) -> Option<ViewDefinition>;
}

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    let dialect = BigQueryDialect {};
    Parser::parse_sql(&dialect, sql)
        .map_err(|e| yachtsql_common::error::Error::parse_error(e.to_string()))
}

pub fn plan_statement<C: CatalogProvider>(
    stmt: &sqlparser::ast::Statement,
    catalog: &C,
) -> Result<LogicalPlan> {
    let planner = Planner::new(catalog);
    planner.plan_statement(stmt)
}

pub fn parse_and_plan<C: CatalogProvider>(sql: &str, catalog: &C) -> Result<LogicalPlan> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("CREATE SNAPSHOT TABLE") {
        return parse_create_snapshot(trimmed);
    }
    if upper.starts_with("DROP SNAPSHOT TABLE") {
        return parse_drop_snapshot(trimmed);
    }

    let statements = parse_sql(sql)?;

    if statements.is_empty() {
        return Err(yachtsql_common::error::Error::parse_error(
            "Empty SQL statement",
        ));
    }

    if statements.len() > 1 {
        return Err(yachtsql_common::error::Error::parse_error(
            "Multiple statements not supported",
        ));
    }

    plan_statement(&statements[0], catalog)
}

fn parse_create_snapshot(sql: &str) -> Result<LogicalPlan> {
    let upper = sql.to_uppercase();
    let if_not_exists = upper.contains("IF NOT EXISTS");

    let rest = if if_not_exists {
        let idx = upper.find("IF NOT EXISTS").unwrap() + 13;
        sql[idx..].trim()
    } else {
        let idx = upper.find("CREATE SNAPSHOT TABLE").unwrap() + 21;
        sql[idx..].trim()
    };

    let clone_idx = rest.to_uppercase().find("CLONE").ok_or_else(|| {
        yachtsql_common::error::Error::parse_error("CREATE SNAPSHOT TABLE requires CLONE clause")
    })?;

    let snapshot_name = rest[..clone_idx].trim().to_string();
    let after_clone = rest[clone_idx + 5..].trim();

    let source_name = if let Some(for_idx) = after_clone.to_uppercase().find("FOR SYSTEM_TIME") {
        after_clone[..for_idx].trim().to_string()
    } else if let Some(opt_idx) = after_clone.to_uppercase().find("OPTIONS") {
        after_clone[..opt_idx].trim().to_string()
    } else {
        after_clone.to_string()
    };

    Ok(LogicalPlan::CreateSnapshot {
        snapshot_name,
        source_name,
        if_not_exists,
    })
}

fn parse_drop_snapshot(sql: &str) -> Result<LogicalPlan> {
    let upper = sql.to_uppercase();
    let if_exists = upper.contains("IF EXISTS");

    let rest = if if_exists {
        let idx = upper.find("IF EXISTS").unwrap() + 9;
        sql[idx..].trim()
    } else {
        let idx = upper.find("DROP SNAPSHOT TABLE").unwrap() + 19;
        sql[idx..].trim()
    };

    let snapshot_name = rest.trim().to_string();

    Ok(LogicalPlan::DropSnapshot {
        snapshot_name,
        if_exists,
    })
}

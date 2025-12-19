use std::cell::RefCell;
use std::collections::HashMap;

use sqlparser::ast::{self, SetExpr, Statement, TableFactor};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, Expr, JoinType, LogicalPlan, PlanField,
    PlanSchema, SetOperationType, SortExpr,
};
use yachtsql_storage::Schema;

use crate::CatalogProvider;
use crate::expr_planner::ExprPlanner;

pub struct Planner<'a, C: CatalogProvider> {
    catalog: &'a C,
    cte_schemas: RefCell<HashMap<String, PlanSchema>>,
}

impl<'a, C: CatalogProvider> Planner<'a, C> {
    pub fn new(catalog: &'a C) -> Self {
        Self {
            catalog,
            cte_schemas: RefCell::new(HashMap::new()),
        }
    }

    pub fn plan_statement(&self, stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(query),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.plan_update(table, assignments, selection.as_ref()),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.plan_drop(object_type, names, *if_exists),
            Statement::Truncate { table_names, .. } => self.plan_truncate(table_names),
            Statement::AlterTable {
                name, operations, ..
            } => self.plan_alter_table(name, operations),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => self.plan_create_schema(schema_name, *if_not_exists),
            _ => Err(Error::unsupported(format!(
                "Unsupported statement: {:?}",
                stmt
            ))),
        }
    }

    fn plan_query(&self, query: &ast::Query) -> Result<LogicalPlan> {
        let ctes = if let Some(ref with_clause) = query.with {
            Some(self.plan_ctes(with_clause)?)
        } else {
            None
        };

        let mut plan = self.plan_set_expr(&query.body)?;

        if let Some(ref order_by) = query.order_by {
            plan = match plan {
                LogicalPlan::Project {
                    input,
                    expressions,
                    schema,
                } => {
                    let sorted =
                        self.plan_order_by_with_aliases(*input, order_by, &expressions, &schema)?;
                    LogicalPlan::Project {
                        input: Box::new(sorted),
                        expressions,
                        schema,
                    }
                }
                _ => self.plan_order_by(plan, order_by)?,
            };
        }

        if let Some(ref limit_clause) = query.limit_clause {
            let (limit_val, offset_val) = match limit_clause {
                ast::LimitClause::LimitOffset { limit, offset, .. } => {
                    let l = limit
                        .as_ref()
                        .map(|e| self.extract_limit_value(e))
                        .transpose()?;
                    let o = offset
                        .as_ref()
                        .map(|o| self.extract_offset_value(o))
                        .transpose()?;
                    (l, o)
                }
                ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                    let l = self.extract_limit_value(limit)?;
                    let o = self.extract_limit_value(offset)?;
                    (Some(l), Some(o))
                }
            };
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: limit_val,
                offset: offset_val,
            };
        }

        if let Some(ctes) = ctes {
            for cte in &ctes {
                self.cte_schemas.borrow_mut().remove(&cte.name);
            }

            plan = LogicalPlan::WithCte {
                ctes,
                body: Box::new(plan),
            };
        }

        Ok(plan)
    }

    fn plan_ctes(&self, with_clause: &ast::With) -> Result<Vec<CteDefinition>> {
        let mut ctes = Vec::new();
        for cte in &with_clause.cte_tables {
            let name = cte.alias.name.value.to_uppercase();
            let columns: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();
            let columns = if columns.is_empty() {
                None
            } else {
                Some(columns)
            };
            let cte_query = self.plan_query(&cte.query)?;

            let cte_schema = cte_query.schema().clone();
            self.cte_schemas
                .borrow_mut()
                .insert(name.clone(), cte_schema);

            let materialized = cte
                .materialized
                .as_ref()
                .map(|m| matches!(m, ast::CteAsMaterialized::Materialized));
            ctes.push(CteDefinition {
                name,
                columns,
                query: Box::new(cte_query),
                recursive: with_clause.recursive,
                materialized,
            });
        }
        Ok(ctes)
    }

    fn plan_set_expr(&self, set_expr: &SetExpr) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(select) => self.plan_select(select),
            SetExpr::Values(values) => self.plan_values(values),
            SetExpr::Query(query) => self.plan_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_plan = self.plan_set_expr(left)?;
                let right_plan = self.plan_set_expr(right)?;

                let ir_op = match op {
                    ast::SetOperator::Union => SetOperationType::Union,
                    ast::SetOperator::Intersect => SetOperationType::Intersect,
                    ast::SetOperator::Except | ast::SetOperator::Minus => SetOperationType::Except,
                };

                let all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::AllByName
                );

                let schema = left_plan.schema().clone();

                Ok(LogicalPlan::SetOperation {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    op: ir_op,
                    all,
                    schema,
                })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported set expression: {:?}",
                set_expr
            ))),
        }
    }

    fn plan_select(&self, select: &ast::Select) -> Result<LogicalPlan> {
        let mut plan = self.plan_from(&select.from)?;

        if let Some(ref selection) = select.selection {
            let subquery_planner = |query: &ast::Query| self.plan_query(query);
            let predicate = ExprPlanner::plan_expr_with_subquery(
                selection,
                plan.schema(),
                Some(&subquery_planner),
            )?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let has_aggregates = self.has_aggregates(&select.projection);
        let has_group_by =
            !matches!(select.group_by, ast::GroupByExpr::Expressions(ref e, _) if e.is_empty());

        if has_aggregates || has_group_by {
            plan = self.plan_aggregate(plan, select)?;
        } else {
            plan = self.plan_projection(plan, &select.projection)?;
        }

        if let Some(ref qualify) = select.qualify {
            let predicate = ExprPlanner::plan_expr(qualify, plan.schema())?;
            plan = LogicalPlan::Qualify {
                input: Box::new(plan),
                predicate,
            };
        }

        if select.distinct.is_some() {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    fn plan_from(&self, from: &[ast::TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Ok(LogicalPlan::Empty {
                schema: PlanSchema::new(),
            });
        }

        let first = &from[0];
        let mut plan = self.plan_table_factor(&first.relation)?;

        for join in &first.joins {
            let right = self.plan_table_factor(&join.relation)?;
            plan = self.plan_join(plan, right, &join.join_operator)?;
        }

        for table_with_joins in from.iter().skip(1) {
            let right = self.plan_table_factor(&table_with_joins.relation)?;
            let combined_schema = plan.schema().clone().merge(right.schema().clone());
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: JoinType::Cross,
                condition: None,
                schema: combined_schema,
            };

            for join in &table_with_joins.joins {
                let right = self.plan_table_factor(&join.relation)?;
                plan = self.plan_join(plan, right, &join.join_operator)?;
            }
        }

        Ok(plan)
    }

    fn plan_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan> {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_name_upper = table_name.to_uppercase();

                if let Some(cte_schema) = self.cte_schemas.borrow().get(&table_name_upper) {
                    let alias_name = alias.as_ref().map(|a| a.name.value.as_str());
                    let schema = if let Some(alias) = alias_name {
                        self.rename_schema(cte_schema, alias)
                    } else {
                        cte_schema.clone()
                    };

                    return Ok(LogicalPlan::Scan {
                        table_name: table_name_upper,
                        schema,
                        projection: None,
                    });
                }

                let storage_schema = self
                    .catalog
                    .get_table_schema(&table_name)
                    .ok_or_else(|| Error::table_not_found(&table_name))?;

                let alias_name = alias.as_ref().map(|a| a.name.value.as_str());
                let schema = self.storage_schema_to_plan_schema(
                    &storage_schema,
                    alias_name.or(Some(&table_name)),
                );

                Ok(LogicalPlan::Scan {
                    table_name,
                    schema,
                    projection: None,
                })
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let plan = self.plan_query(subquery)?;
                if let Some(a) = alias {
                    let schema = self.rename_schema(plan.schema(), &a.name.value);
                    Ok(LogicalPlan::Project {
                        input: Box::new(plan.clone()),
                        expressions: plan
                            .schema()
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| Expr::Column {
                                table: None,
                                name: f.name.clone(),
                                index: Some(i),
                            })
                            .collect(),
                        schema,
                    })
                } else {
                    Ok(plan)
                }
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported table factor: {:?}",
                factor
            ))),
        }
    }

    fn plan_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_op: &ast::JoinOperator,
    ) -> Result<LogicalPlan> {
        let (join_type, condition) = match join_op {
            ast::JoinOperator::Inner(constraint) => (
                JoinType::Inner,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => (
                JoinType::Left,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => (
                JoinType::Right,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::FullOuter(constraint) => (
                JoinType::Full,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            ast::JoinOperator::CrossJoin(_) => (JoinType::Cross, None),
            ast::JoinOperator::Join(constraint) => (
                JoinType::Inner,
                self.extract_join_condition(constraint, &left, &right)?,
            ),
            _ => {
                return Err(Error::unsupported(format!(
                    "Unsupported join type: {:?}",
                    join_op
                )));
            }
        };

        let schema = left.schema().clone().merge(right.schema().clone());

        Ok(LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            schema,
        })
    }

    fn extract_join_condition(
        &self,
        constraint: &ast::JoinConstraint,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Option<Expr>> {
        match constraint {
            ast::JoinConstraint::On(expr) => {
                let combined_schema = left.schema().clone().merge(right.schema().clone());
                Ok(Some(ExprPlanner::plan_expr(expr, &combined_schema)?))
            }
            ast::JoinConstraint::None => Ok(None),
            _ => Err(Error::unsupported(format!(
                "Unsupported join constraint: {:?}",
                constraint
            ))),
        }
    }

    fn plan_projection(
        &self,
        input: LogicalPlan,
        items: &[ast::SelectItem],
    ) -> Result<LogicalPlan> {
        let mut expressions = Vec::new();
        let mut fields = Vec::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);

        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let planned_expr = ExprPlanner::plan_expr_with_subquery(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                    )?;
                    let name = self.expr_name(expr);
                    let data_type = self.infer_expr_type(&planned_expr, input.schema());
                    fields.push(PlanField::new(name, data_type));
                    expressions.push(planned_expr);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let planned_expr = ExprPlanner::plan_expr_with_subquery(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                    )?;
                    let data_type = self.infer_expr_type(&planned_expr, input.schema());
                    fields.push(PlanField::new(alias.value.clone(), data_type));
                    expressions.push(planned_expr);
                }
                ast::SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    for (i, field) in input.schema().fields.iter().enumerate() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            expressions.push(Expr::Column {
                                table: field.table.clone(),
                                name: field.name.clone(),
                                index: Some(i),
                            });
                            fields.push(field.clone());
                        }
                    }
                }
                ast::SelectItem::QualifiedWildcard(name, _) => {
                    let table_name = name.to_string();
                    for (i, field) in input.schema().fields.iter().enumerate() {
                        if field.table.as_ref().is_some_and(|t| t == &table_name) {
                            expressions.push(Expr::Column {
                                table: field.table.clone(),
                                name: field.name.clone(),
                                index: Some(i),
                            });
                            fields.push(field.clone());
                        }
                    }
                }
            }
        }

        let mut window_exprs = Vec::new();
        let mut window_expr_indices = Vec::new();
        for (i, expr) in expressions.iter().enumerate() {
            if Self::expr_has_window(expr) {
                window_exprs.push(expr.clone());
                window_expr_indices.push(i);
            }
        }

        if window_exprs.is_empty() {
            return Ok(LogicalPlan::Project {
                input: Box::new(input),
                expressions,
                schema: PlanSchema::from_fields(fields),
            });
        }

        let input_field_count = input.schema().fields.len();
        let mut window_schema_fields = input.schema().fields.clone();
        for (j, idx) in window_expr_indices.iter().enumerate() {
            window_schema_fields.push(PlanField::new(
                format!("__window_{}", j),
                fields[*idx].data_type.clone(),
            ));
        }
        let window_schema = PlanSchema::from_fields(window_schema_fields);

        let window_plan = LogicalPlan::Window {
            input: Box::new(input),
            window_exprs,
            schema: window_schema.clone(),
        };

        let mut new_expressions = Vec::new();
        let mut window_offset = 0usize;
        for (i, expr) in expressions.iter().enumerate() {
            if window_expr_indices.contains(&i) {
                let col_idx = input_field_count + window_offset;
                new_expressions.push(Expr::Column {
                    table: None,
                    name: format!("__window_{}", window_offset),
                    index: Some(col_idx),
                });
                window_offset += 1;
            } else {
                new_expressions.push(Self::remap_column_indices(expr.clone(), &window_schema));
            }
        }

        Ok(LogicalPlan::Project {
            input: Box::new(window_plan),
            expressions: new_expressions,
            schema: PlanSchema::from_fields(fields),
        })
    }

    fn expr_has_window(expr: &Expr) -> bool {
        match expr {
            Expr::Window { .. } | Expr::AggregateWindow { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_window(left) || Self::expr_has_window(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expr_has_window(expr),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|e| Self::expr_has_window(e))
                    || when_clauses.iter().any(|w| {
                        Self::expr_has_window(&w.condition) || Self::expr_has_window(&w.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::expr_has_window(e))
            }
            Expr::Cast { expr, .. } => Self::expr_has_window(expr),
            Expr::ScalarFunction { args, .. } => args.iter().any(Self::expr_has_window),
            Expr::Alias { expr, .. } => Self::expr_has_window(expr),
            _ => false,
        }
    }

    fn remap_column_indices(expr: Expr, schema: &PlanSchema) -> Expr {
        match expr {
            Expr::Column { table, name, .. } => {
                let idx = schema
                    .fields
                    .iter()
                    .position(|f| f.name == name && (table.is_none() || f.table == table));
                Expr::Column {
                    table,
                    name,
                    index: idx,
                }
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::remap_column_indices(*left, schema)),
                op,
                right: Box::new(Self::remap_column_indices(*right, schema)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
            },
            Expr::Cast {
                expr,
                data_type,
                safe,
            } => Expr::Cast {
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
                data_type,
                safe,
            },
            Expr::ScalarFunction { name, args } => Expr::ScalarFunction {
                name,
                args: args
                    .into_iter()
                    .map(|a| Self::remap_column_indices(a, schema))
                    .collect(),
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(Self::remap_column_indices(*expr, schema)),
                name,
            },
            other => other,
        }
    }

    fn plan_aggregate(&self, input: LogicalPlan, select: &ast::Select) -> Result<LogicalPlan> {
        let mut group_by_exprs = Vec::new();
        let mut aggregate_exprs = Vec::new();
        let mut agg_fields = Vec::new();
        let mut agg_canonical_names: Vec<String> = Vec::new();
        let subquery_planner = |query: &ast::Query| self.plan_query(query);

        match &select.group_by {
            ast::GroupByExpr::All(_) => {}
            ast::GroupByExpr::Expressions(exprs, _) => {
                for expr in exprs {
                    let planned = ExprPlanner::plan_expr_with_subquery(
                        expr,
                        input.schema(),
                        Some(&subquery_planner),
                    )?;
                    let name = self.expr_name(expr);
                    let data_type = self.infer_expr_type(&planned, input.schema());
                    agg_fields.push(PlanField::new(name, data_type));
                    group_by_exprs.push(planned);
                }
            }
        }

        let mut final_projection_exprs: Vec<Expr> = Vec::new();
        let mut final_projection_fields: Vec<PlanField> = Vec::new();
        let group_by_count = group_by_exprs.len();

        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let output_name = match item {
                        ast::SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                        _ => self.expr_name(expr),
                    };

                    if self.is_aggregate_expr(expr) {
                        if Self::is_pure_aggregate_expr(expr) {
                            let planned = ExprPlanner::plan_expr_with_subquery(
                                expr,
                                input.schema(),
                                Some(&subquery_planner),
                            )?;
                            let canonical = Self::canonical_agg_name(expr);
                            let data_type = self.infer_expr_type(&planned, input.schema());
                            agg_fields.push(PlanField::new(canonical.clone(), data_type.clone()));
                            aggregate_exprs.push(planned);
                            agg_canonical_names.push(canonical.clone());

                            let col_idx = group_by_count + aggregate_exprs.len() - 1;
                            final_projection_exprs.push(Expr::Column {
                                table: None,
                                name: canonical,
                                index: Some(col_idx),
                            });
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        } else {
                            let planned = ExprPlanner::plan_expr_with_subquery(
                                expr,
                                input.schema(),
                                Some(&subquery_planner),
                            )?;
                            let (replaced_expr, _extracted_aggs) = self
                                .extract_aggregates_from_expr(
                                    &planned,
                                    &mut agg_canonical_names,
                                    &mut aggregate_exprs,
                                    &mut agg_fields,
                                    input.schema(),
                                    group_by_count,
                                );
                            let data_type = self.infer_expr_type(&planned, input.schema());
                            final_projection_exprs.push(replaced_expr);
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        }
                    } else {
                        let col_name = self.expr_name(expr);
                        if let Some(idx) = agg_fields.iter().position(|f| f.name == col_name) {
                            let data_type = agg_fields[idx].data_type.clone();
                            final_projection_exprs.push(Expr::Column {
                                table: None,
                                name: col_name.clone(),
                                index: Some(idx),
                            });
                            final_projection_fields.push(PlanField::new(output_name, data_type));
                        }
                    }
                }
                ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _) => {
                    for (i, field) in agg_fields.iter().enumerate() {
                        final_projection_exprs.push(Expr::Column {
                            table: field.table.clone(),
                            name: field.name.clone(),
                            index: Some(i),
                        });
                        final_projection_fields.push(field.clone());
                    }
                }
            }
        }

        if let Some(ref having) = select.having {
            Self::collect_having_aggregates(
                having,
                input.schema(),
                &mut agg_canonical_names,
                &mut aggregate_exprs,
                &mut agg_fields,
            )?;
        }

        let agg_schema = PlanSchema::from_fields(agg_fields);
        let mut agg_plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: group_by_exprs,
            aggregates: aggregate_exprs,
            schema: agg_schema.clone(),
        };

        if let Some(ref having) = select.having {
            let predicate = self.plan_having_expr(having, &agg_schema)?;
            agg_plan = LogicalPlan::Filter {
                input: Box::new(agg_plan),
                predicate,
            };
        }

        if !final_projection_exprs.is_empty() {
            Ok(LogicalPlan::Project {
                input: Box::new(agg_plan),
                expressions: final_projection_exprs,
                schema: PlanSchema::from_fields(final_projection_fields),
            })
        } else {
            Ok(agg_plan)
        }
    }

    fn is_pure_aggregate_expr(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                Self::is_aggregate_function_name(&name)
            }
            ast::Expr::Nested(inner) => Self::is_pure_aggregate_expr(inner),
            _ => false,
        }
    }

    fn extract_aggregates_from_expr(
        &self,
        expr: &Expr,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        agg_fields: &mut Vec<PlanField>,
        input_schema: &PlanSchema,
        group_by_count: usize,
    ) -> (Expr, Vec<String>) {
        let mut extracted = Vec::new();
        let replaced = self.replace_aggregates_with_columns(
            expr,
            agg_names,
            agg_exprs,
            agg_fields,
            input_schema,
            group_by_count,
            &mut extracted,
        );
        (replaced, extracted)
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_aggregates_with_columns(
        &self,
        expr: &Expr,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        agg_fields: &mut Vec<PlanField>,
        input_schema: &PlanSchema,
        group_by_count: usize,
        extracted: &mut Vec<String>,
    ) -> Expr {
        match expr {
            Expr::Aggregate { .. } => {
                let canonical = format!("_agg_{}", agg_exprs.len());
                if let Some(idx) = agg_names.iter().position(|n| n == &canonical) {
                    return Expr::Column {
                        table: None,
                        name: canonical,
                        index: Some(group_by_count + idx),
                    };
                }
                let data_type = self.infer_expr_type(expr, input_schema);
                agg_fields.push(PlanField::new(canonical.clone(), data_type));
                agg_exprs.push(expr.clone());
                agg_names.push(canonical.clone());
                extracted.push(canonical.clone());
                Expr::Column {
                    table: None,
                    name: canonical,
                    index: Some(group_by_count + agg_exprs.len() - 1),
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let new_left = self.replace_aggregates_with_columns(
                    left,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                );
                let new_right = self.replace_aggregates_with_columns(
                    right,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                );
                Expr::BinaryOp {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                }
            }
            Expr::UnaryOp { op, expr: inner } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                );
                Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(new_inner),
                }
            }
            Expr::ScalarFunction { name, args } => {
                let new_args: Vec<Expr> = args
                    .iter()
                    .map(|a| {
                        self.replace_aggregates_with_columns(
                            a,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                        )
                    })
                    .collect();
                Expr::ScalarFunction {
                    name: name.clone(),
                    args: new_args,
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
                safe,
            } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                );
                Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                    safe: *safe,
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                let new_operand = operand.as_ref().map(|o| {
                    Box::new(self.replace_aggregates_with_columns(
                        o,
                        agg_names,
                        agg_exprs,
                        agg_fields,
                        input_schema,
                        group_by_count,
                        extracted,
                    ))
                });
                let new_whens: Vec<yachtsql_ir::WhenClause> = when_clauses
                    .iter()
                    .map(|w| yachtsql_ir::WhenClause {
                        condition: self.replace_aggregates_with_columns(
                            &w.condition,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                        ),
                        result: self.replace_aggregates_with_columns(
                            &w.result,
                            agg_names,
                            agg_exprs,
                            agg_fields,
                            input_schema,
                            group_by_count,
                            extracted,
                        ),
                    })
                    .collect();
                let new_else = else_result.as_ref().map(|e| {
                    Box::new(self.replace_aggregates_with_columns(
                        e,
                        agg_names,
                        agg_exprs,
                        agg_fields,
                        input_schema,
                        group_by_count,
                        extracted,
                    ))
                });
                Expr::Case {
                    operand: new_operand,
                    when_clauses: new_whens,
                    else_result: new_else,
                }
            }
            Expr::Alias { expr: inner, name } => {
                let new_inner = self.replace_aggregates_with_columns(
                    inner,
                    agg_names,
                    agg_exprs,
                    agg_fields,
                    input_schema,
                    group_by_count,
                    extracted,
                );
                Expr::Alias {
                    expr: Box::new(new_inner),
                    name: name.clone(),
                }
            }
            _ => expr.clone(),
        }
    }

    fn canonical_agg_name(expr: &ast::Expr) -> String {
        format!("{}", expr).to_uppercase().replace(' ', "")
    }

    fn collect_having_aggregates(
        expr: &ast::Expr,
        input_schema: &PlanSchema,
        agg_names: &mut Vec<String>,
        agg_exprs: &mut Vec<Expr>,
        fields: &mut Vec<PlanField>,
    ) -> Result<()> {
        match expr {
            ast::Expr::Function(func)
                if Self::is_aggregate_function_name(&func.name.to_string()) =>
            {
                let canonical = Self::canonical_agg_name(expr);
                if !agg_names.contains(&canonical) {
                    let planned = ExprPlanner::plan_expr(expr, input_schema)?;
                    let data_type = Self::compute_expr_type(&planned, input_schema);
                    fields.push(PlanField::new(canonical.clone(), data_type));
                    agg_exprs.push(planned);
                    agg_names.push(canonical);
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::collect_having_aggregates(left, input_schema, agg_names, agg_exprs, fields)?;
                Self::collect_having_aggregates(right, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::UnaryOp { expr: inner, .. } => {
                Self::collect_having_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            ast::Expr::Nested(inner) => {
                Self::collect_having_aggregates(inner, input_schema, agg_names, agg_exprs, fields)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn is_aggregate_function_name(name: &str) -> bool {
        let name_upper = name.to_uppercase();
        matches!(
            name_upper.as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "ARRAY_AGG"
                | "STRING_AGG"
                | "ANY_VALUE"
                | "COUNTIF"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
                | "LOGICAL_AND"
                | "LOGICAL_OR"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "CORR"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "APPROX_COUNT_DISTINCT"
                | "APPROX_QUANTILES"
                | "APPROX_TOP_COUNT"
                | "APPROX_TOP_SUM"
                | "GROUPING"
        )
    }

    #[allow(clippy::only_used_in_recursion)]
    fn plan_having_expr(&self, expr: &ast::Expr, agg_schema: &PlanSchema) -> Result<Expr> {
        match expr {
            ast::Expr::Function(func)
                if Self::is_aggregate_function_name(&func.name.to_string()) =>
            {
                let canonical = Self::canonical_agg_name(expr);
                if let Some(idx) = agg_schema.field_index(&canonical) {
                    Ok(Expr::Column {
                        table: None,
                        name: canonical,
                        index: Some(idx),
                    })
                } else {
                    for (idx, field) in agg_schema.fields.iter().enumerate() {
                        if Self::canonical_agg_name_matches(&field.name, &canonical) {
                            return Ok(Expr::Column {
                                table: None,
                                name: field.name.clone(),
                                index: Some(idx),
                            });
                        }
                    }
                    ExprPlanner::plan_expr(expr, agg_schema)
                }
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.plan_having_expr(left, agg_schema)?;
                let right_expr = self.plan_having_expr(right, agg_schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op: ExprPlanner::plan_binary_op(op)?,
                    right: Box::new(right_expr),
                })
            }
            ast::Expr::UnaryOp { op, expr: inner } => {
                let inner_expr = self.plan_having_expr(inner, agg_schema)?;
                Ok(Expr::UnaryOp {
                    op: ExprPlanner::plan_unary_op(op)?,
                    expr: Box::new(inner_expr),
                })
            }
            ast::Expr::Nested(inner) => self.plan_having_expr(inner, agg_schema),
            _ => ExprPlanner::plan_expr(expr, agg_schema),
        }
    }

    fn canonical_agg_name_matches(name: &str, canonical: &str) -> bool {
        let name_normalized = name.to_uppercase().replace(' ', "");
        name_normalized == canonical
    }

    fn plan_order_by(&self, input: LogicalPlan, order_by: &ast::OrderBy) -> Result<LogicalPlan> {
        let mut sort_exprs = Vec::new();

        let exprs = match &order_by.kind {
            ast::OrderByKind::All(_) => return Err(Error::unsupported("ORDER BY ALL")),
            ast::OrderByKind::Expressions(exprs) => exprs,
        };

        for order_expr in exprs {
            let expr = ExprPlanner::plan_expr(&order_expr.expr, input.schema())?;
            let asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first,
            });
        }

        Ok(LogicalPlan::Sort {
            input: Box::new(input),
            sort_exprs,
        })
    }

    fn plan_order_by_with_aliases(
        &self,
        input: LogicalPlan,
        order_by: &ast::OrderBy,
        projection_exprs: &[Expr],
        projection_schema: &PlanSchema,
    ) -> Result<LogicalPlan> {
        let mut sort_exprs = Vec::new();

        let exprs = match &order_by.kind {
            ast::OrderByKind::All(_) => return Err(Error::unsupported("ORDER BY ALL")),
            ast::OrderByKind::Expressions(exprs) => exprs,
        };

        for order_expr in exprs {
            let expr = if let ast::Expr::Identifier(ident) = &order_expr.expr {
                let name = ident.value.to_uppercase();
                let mut found_alias = None;
                for (i, proj_expr) in projection_exprs.iter().enumerate() {
                    if let Expr::Alias {
                        name: alias_name,
                        expr: inner,
                    } = proj_expr
                        && alias_name.to_uppercase() == name
                    {
                        found_alias = Some(inner.as_ref().clone());
                        break;
                    }
                    if i < projection_schema.fields.len()
                        && projection_schema.fields[i].name.to_uppercase() == name
                    {
                        found_alias = Some(proj_expr.clone());
                        break;
                    }
                }
                match found_alias {
                    Some(e) => e,
                    None => ExprPlanner::plan_expr(&order_expr.expr, input.schema())?,
                }
            } else {
                ExprPlanner::plan_expr(&order_expr.expr, input.schema())?
            };

            let asc = order_expr.options.asc.unwrap_or(true);
            let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first,
            });
        }

        Ok(LogicalPlan::Sort {
            input: Box::new(input),
            sort_exprs,
        })
    }

    fn plan_values(&self, values: &ast::Values) -> Result<LogicalPlan> {
        let mut rows = Vec::new();
        let empty_schema = PlanSchema::new();

        for row in &values.rows {
            let mut exprs = Vec::new();
            for expr in row {
                exprs.push(ExprPlanner::plan_expr(expr, &empty_schema)?);
            }
            rows.push(exprs);
        }

        let schema = if let Some(first_row) = rows.first() {
            let num_cols = first_row.len();
            let mut field_types: Vec<DataType> = vec![DataType::Unknown; num_cols];

            for row in &rows {
                for (i, expr) in row.iter().enumerate() {
                    if i < num_cols && field_types[i] == DataType::Unknown {
                        let data_type = self.infer_expr_type(expr, &empty_schema);
                        if data_type != DataType::Unknown {
                            field_types[i] = data_type;
                        }
                    }
                }
            }

            let fields: Vec<PlanField> = field_types
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| PlanField::new(format!("column{}", i + 1), data_type))
                .collect();
            PlanSchema::from_fields(fields)
        } else {
            PlanSchema::new()
        };

        Ok(LogicalPlan::Values {
            values: rows,
            schema,
        })
    }

    fn plan_insert(&self, insert: &ast::Insert) -> Result<LogicalPlan> {
        let table_name = insert.table.to_string();

        let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

        let source = if let Some(ref src) = insert.source {
            self.plan_query(src)?
        } else {
            return Err(Error::parse_error("INSERT requires a source"));
        };

        Ok(LogicalPlan::Insert {
            table_name,
            columns,
            source: Box::new(source),
        })
    }

    fn plan_update(
        &self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: Option<&ast::Expr>,
    ) -> Result<LogicalPlan> {
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(Error::parse_error("UPDATE requires a table name")),
        };

        let storage_schema = self
            .catalog
            .get_table_schema(&table_name)
            .ok_or_else(|| Error::table_not_found(&table_name))?;
        let schema = self.storage_schema_to_plan_schema(&storage_schema, Some(&table_name));

        let mut plan_assignments = Vec::new();
        for assign in assignments {
            let column = match &assign.target {
                ast::AssignmentTarget::ColumnName(names) => names.to_string(),
                ast::AssignmentTarget::Tuple(parts) => parts
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            };
            let value = ExprPlanner::plan_expr(&assign.value, &schema)?;
            plan_assignments.push(Assignment { column, value });
        }

        let filter = selection
            .map(|s| ExprPlanner::plan_expr(s, &schema))
            .transpose()?;

        Ok(LogicalPlan::Update {
            table_name,
            assignments: plan_assignments,
            filter,
        })
    }

    fn plan_delete(&self, delete: &ast::Delete) -> Result<LogicalPlan> {
        let table_name = match &delete.from {
            ast::FromTable::WithFromKeyword(tables) => {
                tables.first().and_then(|t| match &t.relation {
                    TableFactor::Table { name, .. } => Some(name.to_string()),
                    _ => None,
                })
            }
            ast::FromTable::WithoutKeyword(tables) => {
                tables.first().and_then(|t| match &t.relation {
                    TableFactor::Table { name, .. } => Some(name.to_string()),
                    _ => None,
                })
            }
        }
        .ok_or_else(|| Error::parse_error("DELETE requires a table name"))?;

        let storage_schema = self
            .catalog
            .get_table_schema(&table_name)
            .ok_or_else(|| Error::table_not_found(&table_name))?;
        let schema = self.storage_schema_to_plan_schema(&storage_schema, Some(&table_name));

        let filter = delete
            .selection
            .as_ref()
            .map(|s| ExprPlanner::plan_expr(s, &schema))
            .transpose()?;

        Ok(LogicalPlan::Delete { table_name, filter })
    }

    fn plan_create_table(&self, create: &ast::CreateTable) -> Result<LogicalPlan> {
        let table_name = create.name.to_string();

        let columns: Vec<ColumnDef> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type);
                let nullable = !col
                    .options
                    .iter()
                    .any(|o| matches!(o.option, ast::ColumnOption::NotNull));
                ColumnDef {
                    name: col.name.value.clone(),
                    data_type,
                    nullable,
                }
            })
            .collect();

        Ok(LogicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists: create.if_not_exists,
            or_replace: create.or_replace,
        })
    }

    fn plan_drop(
        &self,
        object_type: &ast::ObjectType,
        names: &[ast::ObjectName],
        if_exists: bool,
    ) -> Result<LogicalPlan> {
        match object_type {
            ast::ObjectType::Table => {
                let table_name = names
                    .first()
                    .map(|n| n.to_string())
                    .ok_or_else(|| Error::parse_error("DROP TABLE requires a table name"))?;

                Ok(LogicalPlan::DropTable {
                    table_name,
                    if_exists,
                })
            }
            ast::ObjectType::Schema => {
                let name = names
                    .first()
                    .map(|n| n.to_string())
                    .ok_or_else(|| Error::parse_error("DROP SCHEMA requires a schema name"))?;

                Ok(LogicalPlan::DropSchema {
                    name,
                    if_exists,
                    cascade: false,
                })
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported DROP object type: {:?}",
                object_type
            ))),
        }
    }

    fn plan_truncate(&self, table_names: &[ast::TruncateTableTarget]) -> Result<LogicalPlan> {
        let table_name = table_names
            .first()
            .map(|t| t.name.to_string())
            .ok_or_else(|| Error::parse_error("TRUNCATE requires a table name"))?;

        Ok(LogicalPlan::Truncate { table_name })
    }

    fn plan_create_schema(
        &self,
        schema_name: &ast::SchemaName,
        if_not_exists: bool,
    ) -> Result<LogicalPlan> {
        let name = match schema_name {
            ast::SchemaName::Simple(name) => name.to_string(),
            ast::SchemaName::UnnamedAuthorization(auth) => auth.value.clone(),
            ast::SchemaName::NamedAuthorization(name, _) => name.to_string(),
        };
        Ok(LogicalPlan::CreateSchema {
            name,
            if_not_exists,
        })
    }

    fn plan_alter_table(
        &self,
        name: &ast::ObjectName,
        operations: &[ast::AlterTableOperation],
    ) -> Result<LogicalPlan> {
        let table_name = name.to_string();

        let operation = operations
            .first()
            .ok_or_else(|| Error::parse_error("ALTER TABLE requires an operation"))?;

        let op = match operation {
            ast::AlterTableOperation::AddColumn { column_def, .. } => {
                let data_type = self.sql_type_to_data_type(&column_def.data_type);
                let nullable = !column_def
                    .options
                    .iter()
                    .any(|o| matches!(o.option, ast::ColumnOption::NotNull));
                AlterTableOp::AddColumn {
                    column: ColumnDef {
                        name: column_def.name.value.clone(),
                        data_type,
                        nullable,
                    },
                }
            }
            ast::AlterTableOperation::DropColumn { column_names, .. } => {
                let name = column_names
                    .first()
                    .map(|c| c.value.clone())
                    .unwrap_or_default();
                AlterTableOp::DropColumn { name }
            }
            ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => AlterTableOp::RenameColumn {
                old_name: old_column_name.value.clone(),
                new_name: new_column_name.value.clone(),
            },
            ast::AlterTableOperation::RenameTable {
                table_name: new_name,
            } => AlterTableOp::RenameTable {
                new_name: new_name.to_string(),
            },
            _ => {
                return Err(Error::unsupported(format!(
                    "Unsupported ALTER TABLE operation: {:?}",
                    operation
                )));
            }
        };

        Ok(LogicalPlan::AlterTable {
            table_name,
            operation: op,
        })
    }

    fn storage_schema_to_plan_schema(&self, schema: &Schema, table: Option<&str>) -> PlanSchema {
        let fields = schema
            .fields()
            .iter()
            .map(|f| PlanField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.is_nullable(),
                table: table.map(String::from),
            })
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn rename_schema(&self, schema: &PlanSchema, new_table: &str) -> PlanSchema {
        let fields = schema
            .fields
            .iter()
            .map(|f| PlanField {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                table: Some(new_table.to_string()),
            })
            .collect();
        PlanSchema::from_fields(fields)
    }

    fn sql_type_to_data_type(&self, sql_type: &ast::DataType) -> DataType {
        Self::convert_sql_type(sql_type)
    }

    fn convert_sql_type(sql_type: &ast::DataType) -> DataType {
        match sql_type {
            ast::DataType::Boolean | ast::DataType::Bool => DataType::Bool,
            ast::DataType::Int64 | ast::DataType::BigInt(_) => DataType::Int64,
            ast::DataType::Float64 | ast::DataType::Double(_) => DataType::Float64,
            ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
                let ps = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ast::ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ast::ExactNumberInfo::None => None,
                };
                DataType::Numeric(ps)
            }
            ast::DataType::BigNumeric(_) => DataType::BigNumeric,
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                DataType::String
            }
            ast::DataType::Bytes(_) | ast::DataType::Bytea => DataType::Bytes,
            ast::DataType::Date => DataType::Date,
            ast::DataType::Time(..) => DataType::Time,
            ast::DataType::Datetime(_) => DataType::DateTime,
            ast::DataType::Timestamp(..) => DataType::Timestamp,
            ast::DataType::JSON | ast::DataType::JSONB => DataType::Json,
            ast::DataType::Array(inner) => {
                let element_type = match inner {
                    ast::ArrayElemTypeDef::AngleBracket(dt) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::SquareBracket(dt, _) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::Parenthesis(dt) => Self::convert_sql_type(dt),
                    ast::ArrayElemTypeDef::None => DataType::Unknown,
                };
                DataType::Array(Box::new(element_type))
            }
            ast::DataType::Interval { .. } => DataType::Interval,
            ast::DataType::Custom(name, _) => {
                let type_name = name.to_string().to_uppercase();
                match type_name.as_str() {
                    "GEOGRAPHY" => DataType::Geography,
                    _ => DataType::Unknown,
                }
            }
            _ => DataType::Unknown,
        }
    }

    fn has_aggregates(&self, items: &[ast::SelectItem]) -> bool {
        items.iter().any(|item| match item {
            ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                self.is_aggregate_expr(expr)
            }
            _ => false,
        })
    }

    fn is_aggregate_expr(&self, expr: &ast::Expr) -> bool {
        Self::check_aggregate_expr(expr)
    }

    fn check_aggregate_expr(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                let is_agg = matches!(
                    name.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "ARRAY_AGG"
                        | "STRING_AGG"
                        | "ANY_VALUE"
                        | "COUNTIF"
                        | "COUNT_IF"
                        | "GROUPING"
                        | "LOGICAL_AND"
                        | "BOOL_AND"
                        | "LOGICAL_OR"
                        | "BOOL_OR"
                        | "BIT_AND"
                        | "BIT_OR"
                        | "BIT_XOR"
                        | "APPROX_COUNT_DISTINCT"
                        | "APPROX_QUANTILES"
                        | "APPROX_TOP_COUNT"
                        | "APPROX_TOP_SUM"
                        | "CORR"
                        | "COVAR_POP"
                        | "COVAR_SAMP"
                        | "STDDEV"
                        | "STDDEV_POP"
                        | "STDDEV_SAMP"
                        | "VARIANCE"
                        | "VAR"
                        | "VAR_POP"
                        | "VAR_SAMP"
                );
                if is_agg {
                    return true;
                }
                if let ast::FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg
                            && Self::check_aggregate_expr(e)
                        {
                            return true;
                        }
                    }
                }
                false
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::check_aggregate_expr(left) || Self::check_aggregate_expr(right)
            }
            ast::Expr::UnaryOp { expr, .. } => Self::check_aggregate_expr(expr),
            ast::Expr::Nested(inner) => Self::check_aggregate_expr(inner),
            ast::Expr::Cast { expr, .. } => Self::check_aggregate_expr(expr),
            _ => false,
        }
    }

    fn expr_name(&self, expr: &ast::Expr) -> String {
        match expr {
            ast::Expr::Identifier(ident) => ident.value.clone(),
            ast::Expr::CompoundIdentifier(parts) => {
                parts.last().map(|p| p.value.clone()).unwrap_or_default()
            }
            ast::Expr::Function(func) => func.name.to_string(),
            _ => format!("{}", expr),
        }
    }

    fn infer_expr_type(&self, expr: &Expr, schema: &PlanSchema) -> DataType {
        Self::compute_expr_type(expr, schema)
    }

    fn compute_expr_type(expr: &Expr, schema: &PlanSchema) -> DataType {
        match expr {
            Expr::Literal(lit) => lit.data_type(),
            Expr::Column { name, index, .. } => if let Some(idx) = index {
                schema.fields.get(*idx).map(|f| f.data_type.clone())
            } else {
                schema.field(name).map(|f| f.data_type.clone())
            }
            .unwrap_or(DataType::Unknown),
            Expr::BinaryOp { left, op, right } => {
                use yachtsql_ir::BinaryOp;
                match op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::And
                    | BinaryOp::Or => DataType::Bool,
                    BinaryOp::Concat => DataType::String,
                    _ => {
                        let left_type = Self::compute_expr_type(left, schema);
                        let right_type = Self::compute_expr_type(right, schema);
                        if left_type == DataType::Float64 || right_type == DataType::Float64 {
                            DataType::Float64
                        } else if left_type == DataType::Int64 || right_type == DataType::Int64 {
                            DataType::Int64
                        } else {
                            left_type
                        }
                    }
                }
            }
            Expr::UnaryOp { op, expr } => {
                use yachtsql_ir::UnaryOp;
                match op {
                    UnaryOp::Not => DataType::Bool,
                    _ => Self::compute_expr_type(expr, schema),
                }
            }
            Expr::Aggregate { func, .. } => {
                use yachtsql_ir::AggregateFunction;
                match func {
                    AggregateFunction::Count
                    | AggregateFunction::CountIf
                    | AggregateFunction::Grouping => DataType::Int64,
                    AggregateFunction::Avg
                    | AggregateFunction::Sum
                    | AggregateFunction::Min
                    | AggregateFunction::Max
                    | AggregateFunction::Stddev
                    | AggregateFunction::StddevPop
                    | AggregateFunction::StddevSamp
                    | AggregateFunction::Variance
                    | AggregateFunction::VarPop
                    | AggregateFunction::VarSamp
                    | AggregateFunction::Corr
                    | AggregateFunction::CovarPop
                    | AggregateFunction::CovarSamp => DataType::Float64,
                    AggregateFunction::ArrayAgg | AggregateFunction::ApproxQuantiles => {
                        DataType::Array(Box::new(DataType::Unknown))
                    }
                    AggregateFunction::StringAgg => DataType::String,
                    AggregateFunction::AnyValue => DataType::Unknown,
                    AggregateFunction::LogicalAnd | AggregateFunction::LogicalOr => DataType::Bool,
                    AggregateFunction::BitAnd
                    | AggregateFunction::BitOr
                    | AggregateFunction::BitXor => DataType::Int64,
                    AggregateFunction::ApproxCountDistinct => DataType::Int64,
                    AggregateFunction::ApproxTopCount | AggregateFunction::ApproxTopSum => {
                        DataType::Array(Box::new(DataType::Struct(vec![])))
                    }
                }
            }
            Expr::Cast { data_type, .. } => data_type.clone(),
            Expr::IsNull { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::IsDistinctFrom { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::InUnnest { .. } => DataType::Bool,
            Expr::Alias { expr, .. } => Self::compute_expr_type(expr, schema),
            Expr::Window { func, args, .. } => {
                use yachtsql_ir::WindowFunction;
                match func {
                    WindowFunction::RowNumber
                    | WindowFunction::Rank
                    | WindowFunction::DenseRank
                    | WindowFunction::Ntile => DataType::Int64,
                    WindowFunction::PercentRank | WindowFunction::CumeDist => DataType::Float64,
                    WindowFunction::Lead
                    | WindowFunction::Lag
                    | WindowFunction::FirstValue
                    | WindowFunction::LastValue
                    | WindowFunction::NthValue => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Unknown
                        }
                    }
                }
            }
            Expr::AggregateWindow { func, .. } => {
                use yachtsql_ir::AggregateFunction;
                match func {
                    AggregateFunction::Count | AggregateFunction::CountIf => DataType::Int64,
                    AggregateFunction::Avg
                    | AggregateFunction::Sum
                    | AggregateFunction::Min
                    | AggregateFunction::Max => DataType::Float64,
                    _ => DataType::Unknown,
                }
            }
            Expr::ScalarFunction { name, args } => {
                use yachtsql_ir::ScalarFunction;
                match name {
                    ScalarFunction::CurrentDate
                    | ScalarFunction::Date
                    | ScalarFunction::DateAdd
                    | ScalarFunction::DateSub
                    | ScalarFunction::DateTrunc
                    | ScalarFunction::DateFromUnixDate
                    | ScalarFunction::LastDay
                    | ScalarFunction::ParseDate => DataType::Date,

                    ScalarFunction::CurrentTime
                    | ScalarFunction::Time
                    | ScalarFunction::TimeTrunc
                    | ScalarFunction::ParseTime => DataType::Time,

                    ScalarFunction::CurrentDatetime
                    | ScalarFunction::Datetime
                    | ScalarFunction::DatetimeTrunc
                    | ScalarFunction::ParseDatetime => DataType::DateTime,

                    ScalarFunction::CurrentTimestamp
                    | ScalarFunction::Timestamp
                    | ScalarFunction::TimestampTrunc
                    | ScalarFunction::TimestampMicros
                    | ScalarFunction::TimestampMillis
                    | ScalarFunction::TimestampSeconds
                    | ScalarFunction::ParseTimestamp => DataType::Timestamp,

                    ScalarFunction::DateDiff
                    | ScalarFunction::UnixDate
                    | ScalarFunction::UnixMicros
                    | ScalarFunction::UnixMillis
                    | ScalarFunction::UnixSeconds
                    | ScalarFunction::Length
                    | ScalarFunction::ByteLength
                    | ScalarFunction::CharLength
                    | ScalarFunction::Strpos
                    | ScalarFunction::Instr
                    | ScalarFunction::Ascii
                    | ScalarFunction::ArrayLength
                    | ScalarFunction::Sign
                    | ScalarFunction::FarmFingerprint
                    | ScalarFunction::Int64FromJson
                    | ScalarFunction::BitCount => DataType::Int64,

                    ScalarFunction::Abs
                    | ScalarFunction::Sqrt
                    | ScalarFunction::Power
                    | ScalarFunction::Pow
                    | ScalarFunction::Exp
                    | ScalarFunction::Ln
                    | ScalarFunction::Log
                    | ScalarFunction::Log10
                    | ScalarFunction::Sin
                    | ScalarFunction::Cos
                    | ScalarFunction::Tan
                    | ScalarFunction::Asin
                    | ScalarFunction::Acos
                    | ScalarFunction::Atan
                    | ScalarFunction::Atan2
                    | ScalarFunction::Pi
                    | ScalarFunction::Rand
                    | ScalarFunction::RandCanonical
                    | ScalarFunction::SafeDivide
                    | ScalarFunction::IeeeDivide
                    | ScalarFunction::Float64FromJson => DataType::Float64,

                    ScalarFunction::Floor
                    | ScalarFunction::Ceil
                    | ScalarFunction::Round
                    | ScalarFunction::Trunc
                    | ScalarFunction::Mod
                    | ScalarFunction::Div
                    | ScalarFunction::Greatest
                    | ScalarFunction::Least
                    | ScalarFunction::SafeMultiply
                    | ScalarFunction::SafeAdd
                    | ScalarFunction::SafeSubtract
                    | ScalarFunction::SafeNegate => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Float64
                        }
                    }

                    ScalarFunction::Upper
                    | ScalarFunction::Lower
                    | ScalarFunction::Trim
                    | ScalarFunction::LTrim
                    | ScalarFunction::RTrim
                    | ScalarFunction::Substr
                    | ScalarFunction::Concat
                    | ScalarFunction::Replace
                    | ScalarFunction::Reverse
                    | ScalarFunction::Left
                    | ScalarFunction::Right
                    | ScalarFunction::Repeat
                    | ScalarFunction::Lpad
                    | ScalarFunction::Rpad
                    | ScalarFunction::Initcap
                    | ScalarFunction::Format
                    | ScalarFunction::FormatDate
                    | ScalarFunction::FormatDatetime
                    | ScalarFunction::FormatTimestamp
                    | ScalarFunction::FormatTime
                    | ScalarFunction::String
                    | ScalarFunction::ToJsonString
                    | ScalarFunction::Chr
                    | ScalarFunction::ToBase64
                    | ScalarFunction::ToHex
                    | ScalarFunction::Md5
                    | ScalarFunction::Sha1
                    | ScalarFunction::Sha256
                    | ScalarFunction::Sha512
                    | ScalarFunction::GenerateUuid
                    | ScalarFunction::ArrayToString
                    | ScalarFunction::RegexpExtract
                    | ScalarFunction::RegexpReplace
                    | ScalarFunction::JsonValue
                    | ScalarFunction::JsonExtractScalar
                    | ScalarFunction::TypeOf => DataType::String,

                    ScalarFunction::StartsWith
                    | ScalarFunction::EndsWith
                    | ScalarFunction::Contains
                    | ScalarFunction::RegexpContains
                    | ScalarFunction::IsNan
                    | ScalarFunction::IsInf
                    | ScalarFunction::BoolFromJson => DataType::Bool,

                    ScalarFunction::Split
                    | ScalarFunction::ArrayConcat
                    | ScalarFunction::ArrayReverse
                    | ScalarFunction::GenerateArray
                    | ScalarFunction::GenerateDateArray
                    | ScalarFunction::GenerateTimestampArray => {
                        DataType::Array(Box::new(DataType::Unknown))
                    }

                    ScalarFunction::FromBase64 | ScalarFunction::FromHex => DataType::Bytes,

                    ScalarFunction::ToJson
                    | ScalarFunction::ParseJson
                    | ScalarFunction::JsonQuery
                    | ScalarFunction::JsonExtract => DataType::Json,

                    ScalarFunction::MakeInterval
                    | ScalarFunction::JustifyDays
                    | ScalarFunction::JustifyHours
                    | ScalarFunction::JustifyInterval => DataType::Interval,

                    ScalarFunction::Coalesce
                    | ScalarFunction::IfNull
                    | ScalarFunction::Ifnull
                    | ScalarFunction::NullIf
                    | ScalarFunction::Nvl
                    | ScalarFunction::Nvl2
                    | ScalarFunction::If => {
                        if let Some(first_arg) = args.first() {
                            Self::compute_expr_type(first_arg, schema)
                        } else {
                            DataType::Unknown
                        }
                    }

                    ScalarFunction::Cast
                    | ScalarFunction::SafeCast
                    | ScalarFunction::SafeConvert => DataType::Unknown,

                    ScalarFunction::Custom(name) => {
                        let upper = name.to_uppercase();
                        match upper.as_str() {
                            "ST_CONTAINS" | "ST_WITHIN" | "ST_INTERSECTS" | "ST_COVERS"
                            | "ST_COVEREDBY" | "ST_DISJOINT" | "ST_TOUCHES" | "ST_EQUALS"
                            | "ST_DWITHIN" | "ST_ISCLOSED" | "ST_ISEMPTY" | "ST_ISCOLLECTION"
                            | "ST_ISRING" => DataType::Bool,

                            "ST_GEOGFROMTEXT"
                            | "ST_GEOGPOINT"
                            | "ST_GEOGFROMGEOJSON"
                            | "ST_CENTROID"
                            | "ST_BUFFER"
                            | "ST_CONVEXHULL"
                            | "ST_SIMPLIFY"
                            | "ST_SNAPTOGRID"
                            | "ST_BOUNDARY"
                            | "ST_STARTPOINT"
                            | "ST_ENDPOINT"
                            | "ST_POINTN"
                            | "ST_CLOSESTPOINT"
                            | "ST_UNION"
                            | "ST_INTERSECTION"
                            | "ST_DIFFERENCE"
                            | "ST_MAKELINE"
                            | "ST_MAKEPOLYGON"
                            | "ST_BUFFERWITHTOLERANCE"
                            | "ST_GEOGPOINTFROMGEOHASH" => DataType::Geography,

                            "ST_ASTEXT" | "ST_GEOMETRYTYPE" | "ST_GEOHASH" => DataType::String,

                            "ST_ASGEOJSON" => DataType::Json,

                            "ST_ASBINARY" => DataType::Bytes,

                            "ST_X" | "ST_Y" | "ST_AREA" | "ST_LENGTH" | "ST_PERIMETER"
                            | "ST_DISTANCE" | "ST_MAXDISTANCE" => DataType::Float64,

                            "ST_DIMENSION" | "ST_NUMPOINTS" => DataType::Int64,

                            "NET.IP_IN_NET" | "NET.IP_IS_PRIVATE" => DataType::Bool,

                            "NET.IP_FROM_STRING"
                            | "NET.IPV4_FROM_INT64"
                            | "NET.IP_TRUNC"
                            | "NET.IP_NET_MASK" => DataType::Bytes,

                            "NET.IP_TO_STRING" | "NET.HOST" | "NET.PUBLIC_SUFFIX"
                            | "NET.REG_DOMAIN" => DataType::String,

                            "NET.IPV4_TO_INT64" => DataType::Int64,

                            "BOOL" | "RANGE_CONTAINS" | "RANGE_OVERLAPS" => DataType::Bool,

                            "INT64" | "SAFE_CAST_INT64" | "BIT_COUNT" | "INT64_FROM_JSON" => {
                                DataType::Int64
                            }

                            "FLOAT64" | "SAFE_CAST_FLOAT64" | "FLOAT64_FROM_JSON" => {
                                DataType::Float64
                            }

                            "STRING" | "SAFE_CAST_STRING" | "STRING_FROM_JSON" | "JSON_VALUE" => {
                                DataType::String
                            }

                            "STRUCT" => DataType::Struct(vec![]),

                            "JSON_TYPE" | "JSON" | "PARSE_JSON" | "JSON_QUERY" | "JSON_EXTRACT"
                            | "JSON_SET" | "JSON_REMOVE" | "JSON_STRIP_NULLS" => DataType::Json,

                            "JSON_ARRAY" | "JSON_QUERY_ARRAY" | "JSON_VALUE_ARRAY"
                            | "JSON_EXTRACT_ARRAY" => DataType::Array(Box::new(DataType::Unknown)),

                            "TIMESTAMP_DIFF" => DataType::Int64,

                            _ => DataType::Unknown,
                        }
                    }

                    _ => DataType::Unknown,
                }
            }
            Expr::Case {
                when_clauses,
                else_result,
                ..
            } => {
                if let Some(first_clause) = when_clauses.first() {
                    Self::compute_expr_type(&first_clause.result, schema)
                } else if let Some(else_expr) = else_result {
                    Self::compute_expr_type(else_expr, schema)
                } else {
                    DataType::Unknown
                }
            }
            Expr::Extract { .. } => DataType::Int64,
            Expr::TypedString { data_type, .. } => data_type.clone(),
            Expr::Array { element_type, .. } => {
                DataType::Array(Box::new(element_type.clone().unwrap_or(DataType::Unknown)))
            }
            Expr::ArrayAccess { array, .. } => {
                let array_type = Self::compute_expr_type(array, schema);
                match array_type {
                    DataType::Array(inner) => *inner,
                    _ => DataType::Unknown,
                }
            }
            Expr::Struct { .. } => DataType::Struct(vec![]),
            Expr::StructAccess { expr, field } => {
                Self::resolve_struct_field_type(expr, field, schema)
            }
            _ => DataType::Unknown,
        }
    }

    fn resolve_struct_field_type(expr: &Expr, field: &str, schema: &PlanSchema) -> DataType {
        match expr {
            Expr::Struct { fields } => {
                for (name, field_expr) in fields {
                    if name.as_deref() == Some(field) {
                        return Self::compute_expr_type(field_expr, schema);
                    }
                }
                DataType::Unknown
            }
            Expr::StructAccess {
                expr: inner_expr,
                field: inner_field,
            } => {
                let inner_result = Self::find_struct_field_expr(inner_expr, inner_field);
                if let Some(inner_struct_expr) = inner_result {
                    Self::resolve_struct_field_type(&inner_struct_expr, field, schema)
                } else {
                    DataType::Unknown
                }
            }
            _ => DataType::Unknown,
        }
    }

    fn find_struct_field_expr(expr: &Expr, field: &str) -> Option<Expr> {
        match expr {
            Expr::Struct { fields } => {
                for (name, field_expr) in fields {
                    if name.as_deref() == Some(field) {
                        return Some(field_expr.clone());
                    }
                }
                None
            }
            Expr::StructAccess {
                expr: inner_expr,
                field: inner_field,
            } => {
                let inner_result = Self::find_struct_field_expr(inner_expr, inner_field);
                inner_result
                    .and_then(|inner_struct| Self::find_struct_field_expr(&inner_struct, field))
            }
            _ => None,
        }
    }

    fn extract_limit_value(&self, limit: &ast::Expr) -> Result<usize> {
        match limit {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n
                    .parse()
                    .map_err(|_| Error::parse_error(format!("Invalid LIMIT value: {}", n))),
                _ => Err(Error::parse_error("LIMIT must be a number")),
            },
            _ => Err(Error::parse_error("LIMIT must be a literal number")),
        }
    }

    fn extract_offset_value(&self, offset: &ast::Offset) -> Result<usize> {
        match &offset.value {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n
                    .parse()
                    .map_err(|_| Error::parse_error(format!("Invalid OFFSET value: {}", n))),
                _ => Err(Error::parse_error("OFFSET must be a number")),
            },
            _ => Err(Error::parse_error("OFFSET must be a literal number")),
        }
    }

    fn get_except_columns(
        opts: &ast::WildcardAdditionalOptions,
    ) -> std::collections::HashSet<String> {
        opts.opt_except
            .as_ref()
            .map(|except| {
                let mut cols = std::collections::HashSet::new();
                cols.insert(except.first_element.value.to_lowercase());
                for ident in &except.additional_elements {
                    cols.insert(ident.value.to_lowercase());
                }
                cols
            })
            .unwrap_or_default()
    }
}

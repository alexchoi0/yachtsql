use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReferentialAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Deferrable {
    InitiallyImmediate,
    InitiallyDeferred,
}

impl Deferrable {
    pub fn default_timing(&self) -> crate::transaction::ConstraintTiming {
        use crate::transaction::ConstraintTiming;
        match self {
            Deferrable::InitiallyImmediate => ConstraintTiming::Immediate,
            Deferrable::InitiallyDeferred => ConstraintTiming::Deferred,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub name: Option<String>,
    pub child_columns: Vec<String>,
    pub parent_table: String,
    pub parent_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
    pub deferrable: Option<Deferrable>,
    pub enforced: bool,
}

impl ForeignKey {
    pub fn new(
        child_columns: Vec<String>,
        parent_table: String,
        parent_columns: Vec<String>,
    ) -> Self {
        Self {
            name: None,
            child_columns,
            parent_table,
            parent_columns,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            deferrable: None,
            enforced: true,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_on_delete(mut self, action: ReferentialAction) -> Self {
        self.on_delete = action;
        self
    }

    pub fn with_on_update(mut self, action: ReferentialAction) -> Self {
        self.on_update = action;
        self
    }

    pub fn with_deferrable(mut self, deferrable: Deferrable) -> Self {
        self.deferrable = Some(deferrable);
        self
    }

    pub fn with_enforced(mut self, enforced: bool) -> Self {
        self.enforced = enforced;
        self
    }

    pub fn is_enforced(&self) -> bool {
        self.enforced
    }

    pub fn constraint_name(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| format!("fk_{}", self.child_columns.join("_")))
    }

    pub fn is_deferrable(&self) -> bool {
        self.deferrable.is_some()
    }

    pub fn initial_timing(&self) -> crate::transaction::ConstraintTiming {
        use crate::transaction::ConstraintTiming;
        self.deferrable
            .as_ref()
            .map(|d| d.default_timing())
            .unwrap_or(ConstraintTiming::Immediate)
    }

    pub fn is_composite(&self) -> bool {
        self.child_columns.len() > 1
    }

    pub fn is_self_referencing(&self, table_name: &str) -> bool {
        self.parent_table == table_name
    }

    pub fn validate_column_count(&self) -> Result<(), String> {
        if self.child_columns.len() != self.parent_columns.len() {
            return Err(format!(
                "Foreign key column count mismatch: {} child columns vs {} parent columns",
                self.child_columns.len(),
                self.parent_columns.len()
            ));
        }
        Ok(())
    }

    pub fn validate_not_empty(&self) -> Result<(), String> {
        if self.child_columns.is_empty() {
            return Err("Foreign key must reference at least one column".to_string());
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_not_empty()?;
        self.validate_column_count()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_foreign_key_creation() {
        let fk = ForeignKey::new(
            vec!["dept_id".to_string()],
            "departments".to_string(),
            vec!["id".to_string()],
        );

        assert_eq!(fk.child_columns, vec!["dept_id".to_string()]);
        assert_eq!(fk.parent_table, "departments");
        assert_eq!(fk.parent_columns, vec!["id".to_string()]);
        assert_eq!(fk.on_delete, ReferentialAction::NoAction);
        assert!(!fk.is_composite());
    }

    #[test]
    fn test_foreign_key_with_actions() {
        let fk = ForeignKey::new(
            vec!["dept_id".to_string()],
            "departments".to_string(),
            vec!["id".to_string()],
        )
        .with_name("fk_emp_dept".to_string())
        .with_on_delete(ReferentialAction::Cascade)
        .with_on_update(ReferentialAction::SetNull);

        assert_eq!(fk.name, Some("fk_emp_dept".to_string()));
        assert_eq!(fk.on_delete, ReferentialAction::Cascade);
        assert_eq!(fk.on_update, ReferentialAction::SetNull);
    }

    #[test]
    fn test_composite_foreign_key() {
        let fk = ForeignKey::new(
            vec!["product_id".to_string(), "variant_id".to_string()],
            "products".to_string(),
            vec!["id".to_string(), "variant".to_string()],
        );

        assert!(fk.is_composite());
        assert_eq!(fk.child_columns.len(), 2);
    }

    #[test]
    fn test_self_referencing() {
        let fk = ForeignKey::new(
            vec!["manager_id".to_string()],
            "employees".to_string(),
            vec!["emp_id".to_string()],
        );

        assert!(fk.is_self_referencing("employees"));
        assert!(!fk.is_self_referencing("departments"));
    }

    #[test]
    fn test_validation_valid_fk() {
        let fk = ForeignKey::new(
            vec!["dept_id".to_string()],
            "departments".to_string(),
            vec!["id".to_string()],
        );

        assert!(fk.validate().is_ok());
    }

    #[test]
    fn test_validation_column_count_mismatch() {
        let fk = ForeignKey::new(
            vec!["dept_id".to_string()],
            "departments".to_string(),
            vec!["id".to_string(), "extra".to_string()],
        );

        let result = fk.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("column count mismatch"));
    }

    #[test]
    fn test_validation_empty_columns() {
        let fk = ForeignKey::new(vec![], "departments".to_string(), vec![]);

        let result = fk.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one column"));
    }

    #[test]
    fn test_with_name_accepts_string_types() {
        let fk = ForeignKey::new(
            vec!["dept_id".to_string()],
            "departments".to_string(),
            vec!["id".to_string()],
        )
        .with_name("my_fk");

        assert_eq!(fk.name, Some("my_fk".to_string()));
    }
}

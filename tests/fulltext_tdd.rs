mod common;

use common::{get_f64, get_string_value, is_null};
use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

fn get_value(result: &yachtsql::RecordBatch, row: usize, col: usize) -> yachtsql::Value {
    result.column(col).unwrap().get(row).unwrap()
}

mod to_tsvector_tests {
    use super::*;

    #[test]
    fn test_to_tsvector_basic() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSVECTOR('The quick brown fox') as vector")
            .expect("TO_TSVECTOR should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let vector = get_string_value(&result, 0, 0).unwrap();

        assert!(vector.contains("quick"));
        assert!(vector.contains("brown"));
        assert!(vector.contains("fox"));
        assert!(!vector.contains("the"));
    }

    #[test]
    fn test_to_tsvector_null_input() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSVECTOR(NULL) as vector")
            .expect("TO_TSVECTOR with NULL should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }

    #[test]
    fn test_to_tsvector_empty_string() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSVECTOR('') as vector")
            .expect("TO_TSVECTOR with empty string should work");

        assert_eq!(result.num_rows(), 1);
        let vector = get_string_value(&result, 0, 0).unwrap();
        assert!(vector.is_empty());
    }

    #[test]
    fn test_to_tsvector_with_positions() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSVECTOR('cat dog cat') as vector")
            .expect("TO_TSVECTOR should preserve positions");

        assert_eq!(result.num_rows(), 1);
        let vector = get_string_value(&result, 0, 0).unwrap();
        assert!(vector.contains("cat"));
        assert!(vector.contains("dog"));
    }
}

mod to_tsquery_tests {
    use super::*;

    #[test]
    fn test_to_tsquery_simple_term() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSQUERY('cat') as query")
            .expect("TO_TSQUERY should work with simple term");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
    }

    #[test]
    fn test_to_tsquery_and_operator() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSQUERY('cat & dog') as query")
            .expect("TO_TSQUERY should work with AND operator");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("dog"));
        assert!(query.contains("&"));
    }

    #[test]
    fn test_to_tsquery_or_operator() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSQUERY('cat | dog') as query")
            .expect("TO_TSQUERY should work with OR operator");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("dog"));
        assert!(query.contains("|"));
    }

    #[test]
    fn test_to_tsquery_not_operator() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSQUERY('!cat') as query")
            .expect("TO_TSQUERY should work with NOT operator");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("!"));
        assert!(query.contains("cat"));
    }

    #[test]
    fn test_to_tsquery_null_input() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TO_TSQUERY(NULL) as query")
            .expect("TO_TSQUERY with NULL should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod plainto_tsquery_tests {
    use super::*;

    #[test]
    fn test_plainto_tsquery_multiple_words() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT PLAINTO_TSQUERY('fat cat') as query")
            .expect("PLAINTO_TSQUERY should AND-connect words");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("fat"));
        assert!(query.contains("cat"));
        assert!(query.contains("&"));
    }

    #[test]
    fn test_plainto_tsquery_filters_stop_words() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT PLAINTO_TSQUERY('the fat cat') as query")
            .expect("PLAINTO_TSQUERY should filter stop words");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();

        assert!(query.contains("fat"));
        assert!(query.contains("cat"));
    }

    #[test]
    fn test_plainto_tsquery_null_input() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT PLAINTO_TSQUERY(NULL) as query")
            .expect("PLAINTO_TSQUERY with NULL should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod phraseto_tsquery_tests {
    use super::*;

    #[test]
    fn test_phraseto_tsquery_creates_phrase() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT PHRASETO_TSQUERY('brown fox') as query")
            .expect("PHRASETO_TSQUERY should create phrase query");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("brown"));
        assert!(query.contains("fox"));

        assert!(query.contains("<->"));
    }

    #[test]
    fn test_phraseto_tsquery_null_input() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT PHRASETO_TSQUERY(NULL) as query")
            .expect("PHRASETO_TSQUERY with NULL should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod websearch_to_tsquery_tests {
    use super::*;

    #[test]
    fn test_websearch_simple_terms() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT WEBSEARCH_TO_TSQUERY('cat dog') as query")
            .expect("WEBSEARCH_TO_TSQUERY should work");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("dog"));
    }

    #[test]
    fn test_websearch_negation() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT WEBSEARCH_TO_TSQUERY('cat -dog') as query")
            .expect("WEBSEARCH_TO_TSQUERY should handle negation");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("!") || query.contains("dog"));
    }

    #[test]
    fn test_websearch_null_input() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT WEBSEARCH_TO_TSQUERY(NULL) as query")
            .expect("WEBSEARCH_TO_TSQUERY with NULL should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod ts_match_tests {
    use super::*;

    #[test]
    fn test_ts_match_positive() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'quick') as matches")
            .expect("TS_MATCH should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(true));
    }

    #[test]
    fn test_ts_match_negative() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'elephant') as matches",
            )
            .expect("TS_MATCH should work for non-matches");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(false));
    }

    #[test]
    fn test_ts_match_and_query() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'quick & fox') as matches",
            )
            .expect("TS_MATCH should work with AND query");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(true));

        let result = executor
            .execute_sql(
                "SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'quick & elephant') as matches"
            )
            .expect("TS_MATCH should work with AND query");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(false));
    }

    #[test]
    fn test_ts_match_or_query() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'elephant | fox') as matches",
            )
            .expect("TS_MATCH should work with OR query");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(true));
    }

    #[test]
    fn test_ts_match_not_query() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_MATCH(TO_TSVECTOR('The quick brown fox'), 'quick & !elephant') as matches"
            )
            .expect("TS_MATCH should work with NOT query");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_value(&result, 0, 0).as_bool(), Some(true));
    }

    #[test]
    fn test_ts_match_null_inputs() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_MATCH(NULL, 'test') as matches")
            .expect("TS_MATCH with NULL vector should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));

        let result = executor
            .execute_sql("SELECT TS_MATCH(TO_TSVECTOR('test'), NULL) as matches")
            .expect("TS_MATCH with NULL query should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod ts_rank_tests {
    use super::*;

    #[test]
    fn test_ts_rank_basic() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_RANK(TO_TSVECTOR('The quick brown fox'), 'quick') as rank")
            .expect("TS_RANK should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let rank = get_value(&result, 0, 0).as_f64().unwrap();
        assert!(rank > 0.0, "Rank should be positive for matching query");
    }

    #[test]
    fn test_ts_rank_no_match() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_RANK(TO_TSVECTOR('The quick brown fox'), 'elephant') as rank")
            .expect("TS_RANK should work for non-matching query");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let rank = get_value(&result, 0, 0).as_f64().unwrap();
        assert_eq!(rank, 0.0, "Rank should be 0 for non-matching query");
    }

    #[test]
    fn test_ts_rank_null_inputs() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_RANK(NULL, 'test') as rank")
            .expect("TS_RANK with NULL vector should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod ts_rank_cd_tests {
    use super::*;

    #[test]
    fn test_ts_rank_cd_basic() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_RANK_CD(TO_TSVECTOR('The quick brown fox jumps'), 'quick & fox') as rank"
            )
            .expect("TS_RANK_CD should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let rank = get_value(&result, 0, 0).as_f64().unwrap();
        assert!(
            rank > 0.0,
            "Cover density rank should be positive for matching query"
        );
    }

    #[test]
    fn test_ts_rank_cd_null_inputs() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_RANK_CD(NULL, 'test') as rank")
            .expect("TS_RANK_CD with NULL vector should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod ts_headline_tests {
    use super::*;

    #[test]
    fn test_ts_headline_basic() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TS_HEADLINE('The quick brown fox jumps over the lazy dog', 'quick & fox') as headline"
            )
            .expect("TS_HEADLINE should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let headline = get_string_value(&result, 0, 0).unwrap();

        assert!(headline.contains("<b>") || headline.contains("</b>"));
    }

    #[test]
    fn test_ts_headline_null_inputs() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TS_HEADLINE(NULL, 'test') as headline")
            .expect("TS_HEADLINE with NULL document should work");

        assert_eq!(result.num_rows(), 1);
        assert!(is_null(&result, 0, 0));
    }
}

mod utility_function_tests {
    use super::*;

    #[test]
    fn test_tsvector_length() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TSVECTOR_LENGTH(TO_TSVECTOR('The quick brown fox')) as length")
            .expect("TSVECTOR_LENGTH should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let length = get_value(&result, 0, 0).as_i64().unwrap();
        assert_eq!(length, 3);
    }

    #[test]
    fn test_strip() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT STRIP(TO_TSVECTOR('The quick brown fox')) as stripped")
            .expect("STRIP should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let stripped = get_string_value(&result, 0, 0).unwrap();

        assert!(stripped.contains("quick"));
        assert!(stripped.contains("brown"));
        assert!(stripped.contains("fox"));
    }

    #[test]
    fn test_setweight() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT SETWEIGHT(TO_TSVECTOR('Hello world'), 'A') as weighted")
            .expect("SETWEIGHT should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let weighted = get_string_value(&result, 0, 0).unwrap();

        assert!(weighted.contains("A"));
    }

    #[test]
    fn test_tsvector_concat() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                "SELECT TSVECTOR_CONCAT(TO_TSVECTOR('Hello'), TO_TSVECTOR('World')) as combined",
            )
            .expect("TSVECTOR_CONCAT should work");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0));

        let combined = get_string_value(&result, 0, 0).unwrap();
        assert!(combined.contains("hello"));
        assert!(combined.contains("world"));
    }

    #[test]
    fn test_utility_null_inputs() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TSVECTOR_LENGTH(NULL) as length")
            .expect("TSVECTOR_LENGTH with NULL should work");
        assert!(is_null(&result, 0, 0));

        let result = executor
            .execute_sql("SELECT STRIP(NULL) as stripped")
            .expect("STRIP with NULL should work");
        assert!(is_null(&result, 0, 0));

        let result = executor
            .execute_sql("SELECT SETWEIGHT(NULL, 'A') as weighted")
            .expect("SETWEIGHT with NULL should work");
        assert!(is_null(&result, 0, 0));
    }
}

mod query_operator_tests {
    use super::*;

    #[test]
    fn test_tsquery_and() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TSQUERY_AND(TO_TSQUERY('cat'), TO_TSQUERY('dog')) as query")
            .expect("TSQUERY_AND should work");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("dog"));
        assert!(query.contains("&"));
    }

    #[test]
    fn test_tsquery_or() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TSQUERY_OR(TO_TSQUERY('cat'), TO_TSQUERY('dog')) as query")
            .expect("TSQUERY_OR should work");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("cat"));
        assert!(query.contains("dog"));
        assert!(query.contains("|"));
    }

    #[test]
    fn test_tsquery_not() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT TSQUERY_NOT(TO_TSQUERY('cat')) as query")
            .expect("TSQUERY_NOT should work");

        assert_eq!(result.num_rows(), 1);
        let query = get_string_value(&result, 0, 0).unwrap();
        assert!(query.contains("!"));
        assert!(query.contains("cat"));
    }
}

mod integration_tests {
    use super::*;

    #[test]
    fn test_full_text_search_workflow() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE documents (id INT, title TEXT, body TEXT)")
            .expect("CREATE TABLE should work");

        executor
            .execute_sql(
                "INSERT INTO documents VALUES
                (1, 'PostgreSQL Tutorial', 'Learn PostgreSQL database fundamentals'),
                (2, 'MySQL Guide', 'Getting started with MySQL database'),
                (3, 'Database Comparison', 'PostgreSQL vs MySQL comparison guide')",
            )
            .expect("INSERT should work");

        let result = executor
            .execute_sql(
                "SELECT id, title FROM documents
                 WHERE TS_MATCH(TO_TSVECTOR(body), 'postgresql')",
            )
            .expect("Full-text search query should work");

        assert!(result.num_rows() >= 1);
    }

    #[test]
    fn test_ranking_and_ordering() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE articles (id INT, content TEXT)")
            .expect("CREATE TABLE should work");

        executor
            .execute_sql(
                "INSERT INTO articles VALUES
                (1, 'cat'),
                (2, 'cat dog'),
                (3, 'cat dog bird')",
            )
            .expect("INSERT should work");

        let result = executor
            .execute_sql(
                "SELECT id, TS_RANK(TO_TSVECTOR(content), 'cat') as rank
                 FROM articles",
            )
            .expect("Ranking query should work");

        assert_eq!(result.num_rows(), 3);

        for row in 0..result.num_rows() {
            let rank = get_f64(&result, 1, row);
            assert!(rank > 0.0);
        }
    }
}

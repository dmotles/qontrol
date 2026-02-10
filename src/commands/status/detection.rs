use serde_json::Value;

use super::types::ClusterType;

/// Detect the cluster platform type from node model numbers.
///
/// - Any node with model_number containing "AWS" → CnqAws
/// - Any node with model_number containing "Azure" → AnqAzure
/// - Otherwise → OnPrem with unique model numbers listed
pub fn detect_cluster_type(nodes: &[Value]) -> ClusterType {
    let mut has_aws = false;
    let mut has_azure = false;
    let mut on_prem_models = std::collections::BTreeSet::new();

    for node in nodes {
        let model = node["model_number"].as_str().unwrap_or("");
        if model.contains("AWS") {
            has_aws = true;
        } else if model.contains("Azure") {
            has_azure = true;
        } else if !model.is_empty() {
            on_prem_models.insert(model.to_string());
        }
    }

    if has_aws {
        ClusterType::CnqAws
    } else if has_azure {
        ClusterType::AnqAzure
    } else {
        ClusterType::OnPrem(on_prem_models.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_detect_aws() {
        let nodes = vec![
            json!({"model_number": "AWS", "node_status": "online"}),
            json!({"model_number": "AWS", "node_status": "online"}),
        ];
        assert_eq!(detect_cluster_type(&nodes), ClusterType::CnqAws);
    }

    #[test]
    fn test_detect_azure() {
        let nodes = vec![json!({"model_number": "Azure", "node_status": "online"})];
        assert_eq!(detect_cluster_type(&nodes), ClusterType::AnqAzure);
    }

    #[test]
    fn test_detect_on_prem_single_model() {
        let nodes = vec![
            json!({"model_number": "Q0626", "node_status": "online"}),
            json!({"model_number": "Q0626", "node_status": "online"}),
        ];
        assert_eq!(
            detect_cluster_type(&nodes),
            ClusterType::OnPrem(vec!["Q0626".to_string()])
        );
    }

    #[test]
    fn test_detect_on_prem_multiple_models() {
        let nodes = vec![
            json!({"model_number": "Q0626", "node_status": "online"}),
            json!({"model_number": "K_432_S", "node_status": "online"}),
        ];
        let result = detect_cluster_type(&nodes);
        match result {
            ClusterType::OnPrem(models) => {
                assert_eq!(models.len(), 2);
                assert!(models.contains(&"K_432_S".to_string()));
                assert!(models.contains(&"Q0626".to_string()));
            }
            _ => panic!("expected OnPrem"),
        }
    }

    #[test]
    fn test_detect_empty_nodes() {
        assert_eq!(detect_cluster_type(&[]), ClusterType::OnPrem(vec![]));
    }

    #[test]
    fn test_detect_missing_model_number() {
        let nodes = vec![json!({"node_status": "online"})];
        assert_eq!(detect_cluster_type(&nodes), ClusterType::OnPrem(vec![]));
    }
}

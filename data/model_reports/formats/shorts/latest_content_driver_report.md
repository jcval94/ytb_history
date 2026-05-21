# Content Driver Models Report

Generated at: 2026-05-21T13:46:39.350348+00:00

## Targets evaluados
- future_log_views_delta_7d
- future_relative_views_delta_7d
- future_engagement_delta_7d
- content_value_score_7d

## Modelos entrenados
- random_forest_regressor
- linear_regularized_regressor
- shallow_tree_regressor

## Mejor modelo por target
- content_value_score_7d: random_forest_regressor (spearman=0.589168)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: random_forest_regressor (spearman=0.907337)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.446129)

## Top variables por target
- **content_value_score_7d**: opportunity_score, alpha_score, packaging_problem_score, decision_score, hook_semantic_type__warning
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: alpha_score, views_delta, views_delta, packaging_problem_score, channel_relative_success_score
- **future_relative_views_delta_7d**: alpha_score, hybrid_decision_score, trend_burst_score, decision_score, opportunity_score

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 150.20088067
- content_value_score_7d / linear_regularized_regressor / engagement_context: 71.44290944
- future_relative_views_delta_7d / linear_regularized_regressor / model_decision: 68.27797965
- future_relative_views_delta_7d / linear_regularized_regressor / channel_context: 26.49581418
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 16.83397374
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 16.38702663
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 10.48426877
- content_value_score_7d / linear_regularized_regressor / title_style: 9.39613433
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 8.84781586
- content_value_score_7d / linear_regularized_regressor / semantic_scores: 7.26421522

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
# Content Driver Models Report

Generated at: 2026-05-14T12:12:53.927043+00:00

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
- content_value_score_7d: random_forest_regressor (spearman=0.744)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: random_forest_regressor (spearman=0.961897)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.655613)

## Top variables por target
- **content_value_score_7d**: opportunity_score, alpha_score, decision_score, hybrid_decision_score, engagement_rate
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: alpha_score, opportunity_score, views_delta, decision_score, views_delta
- **future_relative_views_delta_7d**: hybrid_decision_score, opportunity_score, decision_score, alpha_score, tutorial_semantic_score

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 92.58226763
- future_relative_views_delta_7d / linear_regularized_regressor / model_decision: 62.65355757
- content_value_score_7d / linear_regularized_regressor / engagement_context: 61.18943883
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 50.04335057
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 49.67790441
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 41.18577876
- content_value_score_7d / linear_regularized_regressor / model_decision: 28.68194581
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 14.57281194
- future_log_views_delta_7d / linear_regularized_regressor / engagement_context: 10.22235574
- content_value_score_7d / linear_regularized_regressor / title_style: 10.18814835

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
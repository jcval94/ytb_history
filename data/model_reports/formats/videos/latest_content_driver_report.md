# Content Driver Models Report

Generated at: 2026-07-13T13:05:17.429451+00:00

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
- content_value_score_7d: random_forest_regressor (spearman=0.876852)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: random_forest_regressor (spearman=0.973116)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.798421)

## Top variables por target
- **content_value_score_7d**: topic_saturation_score, has_question, title_has_question, topic_secondary__unknown, topic_primary__unknown
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: alpha_score, views_delta, opportunity_score, topic_secondary__unknown, topic_secondary__ai_tools
- **future_relative_views_delta_7d**: topic_saturation_score, has_question, title_has_question, topic_secondary__unknown, topic_primary__unknown

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 1015.84022156
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 391.3329967
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 343.59271299
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 201.43038387
- content_value_score_7d / linear_regularized_regressor / topic_metrics: 151.49283535
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 126.27828912
- future_relative_views_delta_7d / linear_regularized_regressor / channel_context: 81.20155338
- content_value_score_7d / linear_regularized_regressor / title_style: 52.93629272
- content_value_score_7d / linear_regularized_regressor / engagement_context: 45.59567413
- content_value_score_7d / linear_regularized_regressor / semantic_cluster: 38.47793876

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
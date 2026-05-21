# Content Driver Models Report

Generated at: 2026-05-21T13:47:20.957966+00:00

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
- content_value_score_7d: linear_regularized_regressor (spearman=0.694913)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: random_forest_regressor (spearman=0.944241)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.531162)

## Top variables por target
- **content_value_score_7d**: has_question, title_has_question, topic_primary__unknown, topic_saturation_score, opportunity_score
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: views_delta, has_question, title_has_question, topic_saturation_score, alpha_score
- **future_relative_views_delta_7d**: topic_primary__tutorial, tutorial_semantic_score, has_question, title_has_question, topic_saturation_score

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 518.65685508
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 212.01893211
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 186.72419883
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 186.10606214
- content_value_score_7d / linear_regularized_regressor / topic_metrics: 105.07556421
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 53.53388462
- content_value_score_7d / linear_regularized_regressor / engagement_context: 50.2685822
- content_value_score_7d / linear_regularized_regressor / title_style: 41.56424723
- future_relative_views_delta_7d / linear_regularized_regressor / channel_context: 34.8214475
- future_relative_views_delta_7d / linear_regularized_regressor / model_decision: 32.55341158

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
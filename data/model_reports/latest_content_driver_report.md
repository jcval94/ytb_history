# Content Driver Models Report

Generated at: 2026-05-11T13:26:03.006651+00:00

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
- content_value_score_7d: random_forest_regressor (spearman=0.729681)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: random_forest_regressor (spearman=0.954704)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.617364)

## Top variables por target
- **content_value_score_7d**: opportunity_score, decision_score, alpha_score, hybrid_decision_score, engagement_rate
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: alpha_score, opportunity_score, views_delta, views_delta, decision_score
- **future_relative_views_delta_7d**: trend_burst_score, hybrid_decision_score, channel_relative_success_score, hook_semantic_type__warning, decision_score

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 72.8218846
- content_value_score_7d / linear_regularized_regressor / engagement_context: 65.68153803
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 55.15135555
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 40.86479393
- content_value_score_7d / linear_regularized_regressor / model_decision: 40.36808764
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 36.92363726
- future_relative_views_delta_7d / linear_regularized_regressor / model_decision: 32.62041077
- future_relative_views_delta_7d / linear_regularized_regressor / channel_context: 20.06936535
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 15.14180661
- content_value_score_7d / linear_regularized_regressor / title_style: 10.21937196

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
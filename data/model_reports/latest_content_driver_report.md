# Content Driver Models Report

Generated at: 2026-05-07T12:18:48.623161+00:00

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
- content_value_score_7d: random_forest_regressor (spearman=0.602619)
- future_engagement_delta_7d: random_forest_regressor (spearman=1.0)
- future_log_views_delta_7d: linear_regularized_regressor (spearman=0.915719)
- future_relative_views_delta_7d: random_forest_regressor (spearman=0.566562)

## Top variables por target
- **content_value_score_7d**: opportunity_score, decision_score, hybrid_decision_score, alpha_score, channel_relative_success_score
- **future_engagement_delta_7d**: ai_semantic_score, alpha_score, channel_momentum_score, channel_relative_success_score, curiosity_semantic_score
- **future_log_views_delta_7d**: alpha_score, opportunity_score, views_delta, decision_score, views_delta
- **future_relative_views_delta_7d**: decision_score, hybrid_decision_score, opportunity_score, alpha_score, topic_confidence

## Variables con dirección (positive/negative/mixed)
Ver `latest_content_driver_feature_direction.csv` para detalle por feature.

## Importancia por grupo
- future_relative_views_delta_7d / linear_regularized_regressor / engagement_context: 68.86953273
- future_relative_views_delta_7d / linear_regularized_regressor / model_decision: 56.11742795
- content_value_score_7d / linear_regularized_regressor / engagement_context: 43.2146943
- future_relative_views_delta_7d / linear_regularized_regressor / title_style: 34.63663841
- content_value_score_7d / linear_regularized_regressor / model_decision: 33.11569692
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_scores: 33.03748821
- future_relative_views_delta_7d / linear_regularized_regressor / topic_metrics: 32.6628003
- future_relative_views_delta_7d / linear_regularized_regressor / semantic_cluster: 19.35748617
- content_value_score_7d / linear_regularized_regressor / title_style: 15.72119804
- content_value_score_7d / linear_regularized_regressor / topic_metrics: 9.33089101

**Estas importancias son predictivas, no causales.**

**En RF, la dirección se estima con directional analysis, no con impurity importance.**
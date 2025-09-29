CREATE MATERIALIZED VIEW mv_placement_rates AS
SELECT
    ch.cohort_id,
    ch.cohort_name,
    p.region,
    COUNT(DISTINCT cand.candidate_id) AS total_candidates,
    COUNT(DISTINCT CASE WHEN pl.placement_status ILIKE '%placed%' THEN cand.candidate_id END) AS placed_candidates,
    ROUND(
        COUNT(DISTINCT CASE WHEN pl.placement_status ILIKE '%placed%' THEN cand.candidate_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT cand.candidate_id), 0), 2
    ) AS placement_rate
FROM dim_candidates cand
JOIN dim_cohorts ch ON cand.cohort_id = ch.cohort_id
JOIN dim_provinces p ON cand.province_id = p.province_id
LEFT JOIN fact_placements pl ON cand.candidate_id = pl.candidate_id
GROUP BY ch.cohort_id, ch.cohort_name, p.region;

-- Cohort performance summary
CREATE MATERIALIZED VIEW mv_cohort_performance AS
SELECT 
    ch.cohort_id,
    ch.cohort_name,
    COUNT(DISTINCT cand.candidate_id) AS total_candidates,
    COUNT(DISTINCT pl.candidate_id) AS placed_candidates,
    COUNT(DISTINCT cr.candidate_id) AS candidates_with_courses,
    COUNT(DISTINCT cr.progress_id) AS total_course_completions
    
FROM dim_cohorts ch
LEFT JOIN dim_candidates cand ON ch.cohort_id = cand.cohort_id
LEFT JOIN fact_placements pl ON cand.candidate_id = pl.candidate_id 
    AND pl.placement_status ILIKE '%placed%'
LEFT JOIN fact_coursera cr ON cand.candidate_id = cr.candidate_id
GROUP BY ch.cohort_id, ch.cohort_name;

-- Refresh function
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_placement_rates;
    REFRESH MATERIALIZED VIEW mv_cohort_performance;
END;
$$ LANGUAGE plpgsql;
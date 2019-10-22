COPY admob_report(
    filedate,
    filename,
    app,
    country,
    date,
    active_impressions,
    measurable_impressions,
    measurable_impressions_perc,
    viewable_impressions,
    viewable_impressions_perc,
    admob_request_usd,
    admob_request,
    clicks,
    estimated_earnings_usd,
    impressions_ctr_perc,
    impressions_rpm_usd,
    impressions,
    match_rate_perc,
    matched_requests,
    rewarded_completes,
    show_rate
)
FROM 'admob-report.csv' DELIMITER ',' CSV HEADER;

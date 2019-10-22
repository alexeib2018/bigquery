CREATE TABLE admob_report (
    id SERIAL NOT NULL PRIMARY KEY,
    filedate                        DATE,           -- Date
    filename                        VARCHAR(64),    -- Filename
    app                             VARCHAR(64),    -- App
    country                         VARCHAR(64),    -- Country
    date                            DATE,           -- Date
    active_impressions              INT,            -- Active View-eligible impressions
    measurable_impressions          INT,            -- Measurable impressions
    measurable_impressions_perc     VARCHAR(7),     -- % Measurable impressions (%)
    viewable_impressions            INT,            -- Viewable impressions
    viewable_impressions_perc       VARCHAR(7),     -- % Viewable impressions (%)
    admob_request_usd               NUMERIC(5,2),   -- AdMob Network request RPM (USD)
    admob_request                   INT,            -- AdMob Network requests
    clicks                          INT,            -- Clicks
    estimated_earnings_usd          NUMERIC(5,2),   -- Estimated earnings (USD)
    impressions_ctr_perc            VARCHAR(7),     -- Impressions CTR (%)
    impressions_rpm_usd             VARCHAR(7),     -- Impression RPM (USD)
    impressions                     INT,            -- Impressions
    match_rate_perc                 VARCHAR(7),     -- Match rate (%)
    matched_requests                INT,            -- Matched requests
    rewarded_completes              INT,            -- Rewarded completes
    show_rate                       VARCHAR(7)      -- Show rate (%)
);

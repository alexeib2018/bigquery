CREATE TABLE user_locations (
    id SERIAL NOT NULL PRIMARY KEY,
    location            VARCHAR(32),        -- Country/Territory (User location)
    location_type       VARCHAR(32),        -- Location type
    campaign            VARCHAR(32),        -- Campaign
    currency            VARCHAR(3),         -- Currency
    clicks              INT,                -- Clicks
    impressions         INT,                -- Impressions
    ctr                 NUMERIC(5,2),       -- CTR
    avg_cpc             NUMERIC(5,2),       -- Avg. CPC
    cost                NUMERIC(5,2),       -- Cost
    impr_abs_top        NUMERIC(5,2),       -- Impr. (Abs. Top) %
    impr_top            NUMERIC(5,2),       -- Impr. (Top) %
    conversions         INT,                -- Conversions
    view_through_conv   INT,                -- View-through conv.
    cost_conv           NUMERIC(5,2),       -- Cost / conv.
    conv_rate           NUMERIC(5,2)        -- Conv. Rate
);

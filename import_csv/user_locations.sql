COPY user_locations(date,
                    filename,
                    period,
                    location,
                    location_type,
                    campaign,
                    currency,
                    clicks,
                    impressions,
                    ctr,
                    avg_cpc,
                    cost,
                    impr_abs_top,
                    impr_top,
                    conversions,
                    view_through_conv,
                    cost_conv,
                    conv_rate)
FROM 'user_locations.csv' DELIMITER ',' CSV HEADER;
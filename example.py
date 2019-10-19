from google.cloud import bigquery
client = bigquery.Client()

query = (
	#"SELECT * FROM `track-money-bank-balance-lite.analytics_187332759.events_intraday_20191017` LIMIT 10"
	"SELECT * FROM `test-alexei.test_dataset.test_table`"
)
query_job = client.query(
    query,
    # Location must match that of the dataset(s) referenced in the query.
    location="US",
)  # API request - starts the query

for row in query_job:  # API request - fetches results
    # Row values can be accessed by field name or index
    # assert row[0] == row.name == row["name"]
    print(row)

print('Script finished OK\n')

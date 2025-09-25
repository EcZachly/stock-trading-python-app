import requests
import csv
import os
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000


def run_stock_job():
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        print(data)
        for ticker in data['results']:
            tickers.append(ticker)

    # Write tickers to CSV
    if tickers:  # Check if tickers list is not empty
        fieldnames = list(tickers[0].keys())
        output_csv = 'tickers.csv'
        with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for t in tickers:
                row = {key: t.get(key, '') for key in fieldnames}
                writer.writerow(row)
        print(f'Wrote {len(tickers)} rows to {output_csv}')
    else:
        print("No tickers retrieved to write to CSV.")

if __name__ == '__main__':
    run_stock_job()










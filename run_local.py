import argparse
from plugins.pipelines.lake.equity.equity_price_pipeline import EquityPricePipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", choices=["fetch", "validate", "load"], required=True)
    parser.add_argument("--country", default="US")
    parser.add_argument("--date", default="2025-09-30")
    args = parser.parse_args()

    pipeline = EquityPricePipeline()

    if args.task == "fetch":
        data = pipeline.fetch(country=args.country, date=args.date)
        print("Fetched records:", len(data))
    elif args.task == "validate":
        data = pipeline.fetch(country=args.country, date=args.date)
        print(pipeline.validate(data))
    elif args.task == "load":
        data = pipeline.fetch(country=args.country, date=args.date)
        pipeline.validate(data)
        print(pipeline.load(data))

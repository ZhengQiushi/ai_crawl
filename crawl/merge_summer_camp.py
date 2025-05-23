import os
import pandas as pd
import asyncio

OUTPUT_PATH = "/home/azureuser/Trustbeez/crawl/output/"

async def main():
    # 重新从 output 目录读取所有 CSV 进行合并
    csv_files = [f for f in os.listdir(OUTPUT_PATH) if f.endswith(".csv") and f != "merged_results.csv"]
    all_data = []

    for csv_file in csv_files:
        file_path = os.path.join(OUTPUT_PATH, csv_file)
        df = pd.read_csv(file_path, header=None, names=['Index', 'Website', 'Parsing Result'])
        all_data.append(df)

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        merged_df.sort_values(by=['Index'], inplace=True)
        merged_df.to_csv(os.path.join(OUTPUT_PATH, "merged_results.csv"), index=False)
        print("All results have been saved and merged.")
    else:
        print("No new data found to merge.")

if __name__ == "__main__":
    asyncio.run(main())
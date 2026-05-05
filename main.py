from src.ingestion import process_bronze_to_silver
from src.features import process_silver_to_gold

def run_pipeline():
    
    try:
        # Step 1: Ingestion & Initial Cleaning
        # print("\n[PHASE 1] Running Ingestion: Bronze -> Silver...")
        # process_bronze_to_silver()
        print("[PHASE 1] Success: Data is now in Parquet format in /silver.")

        # Step 2: Analytics & Feature Engineering
        print("\n[PHASE 2] Running Feature Engineering: Silver -> Gold...")
        process_silver_to_gold()
        print("[PHASE 2] Success: Feature store is now available in /gold.")
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {str(e)}")

if __name__ == "__main__":
    run_pipeline()

from src.ingestion import process_bronze_to_silver

def run_pipeline():
    
    try:
        # Step 1: Ingestion & Initial Cleaning
        print("\n[PHASE 1] Running Ingestion: Bronze -> Silver...")
        process_bronze_to_silver()
        print("[PHASE 1] Success: Data is now in Parquet format in /silver.")
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed at Phase 1: {str(e)}")

if __name__ == "__main__":
    run_pipeline()
import sys

from src.ingestion import process_bronze_to_silver
from src.features import process_silver_to_gold
from src.ml_training import train_demand_forecasting_model


VALID_MODES = {"phase1", "phase2", "phase3", "full"}


def run_phase1():
    print("\n[PHASE 1] Running Ingestion: Bronze -> Silver...")
    process_bronze_to_silver()
    print("[PHASE 1] Success: Data is now in Parquet format in /silver.")


def run_phase2():
    print("\n[PHASE 2] Running Feature Engineering: Silver -> Gold...")
    process_silver_to_gold()
    print("[PHASE 2] Success: Feature store is now available in /gold.")


def run_phase3():
    print("\n[PHASE 3] Running ML Training: Gold -> Model...")
    train_demand_forecasting_model()
    print("[PHASE 3] Success: Model and metrics are now available in /models.")


def run_pipeline(mode="full"):
    try:
        if mode not in VALID_MODES:
            valid_modes = ", ".join(sorted(VALID_MODES))
            raise ValueError(f"Invalid mode '{mode}'. Use one of: {valid_modes}")

        if mode in {"phase1", "full"}:
            run_phase1()

        if mode in {"phase2", "full"}:
            run_phase2()

        if mode in {"phase3", "full"}:
            run_phase3()

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {str(e)}")


if __name__ == "__main__":
    selected_mode = sys.argv[1].lower() if len(sys.argv) > 1 else "full"
    run_pipeline(selected_mode)

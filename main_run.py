import subprocess
import time
import sys
from datetime import datetime, timedelta

def run_pipeline():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n" + "="*50)
    print(f"STARTING PIPELINE RUN: {now}")
    print("="*50)
    
    try:
        # STEP 2 Part: Ingest (Prices into MySQL)
        print("\n[1/2] Starting Ingest (Binance & CMC)...")
        # check=True ensures the script stops if a sub-script fails
        subprocess.run([sys.executable, "Step2/2v3_daily_delta_load.py"], check=True)

        # STEP 3 Part: Snowflake Bridge (Sync to Cloud)
        print("\n[2/2] Starting Snowflake Bridge (Kimball Sync)...")
        subprocess.run([sys.executable, "Step3/3_snowflake_kimball_bridge.py"], check=True)
        
        print("\n" + " SUCCESS ".center(50, "-"))
        print(f"Pipeline completed successfully at {datetime.now().strftime('%H:%M:%S')}")

    except subprocess.CalledProcessError as e:
        print(f"\n!!! ERROR in sub-process: {e.cmd}")
        print("Pipeline failed. It will retry automatically at the next interval.")

if __name__ == "__main__":
    print("Crypto Data Pipeline 'Live-Orchestrator' initialized...")
    
    while True:
        run_pipeline()
        
        next_run = datetime.now() + timedelta(hours=24)
        print(f"\nNext scheduled run: {next_run.strftime('%d.%m.%Y at %H:%M:%S')}")
        print("Waiting 24 hours... (Press CTRL+C to terminate)")
        
        # Wait for 24 hours (86,400 seconds)
        time.sleep(86400)
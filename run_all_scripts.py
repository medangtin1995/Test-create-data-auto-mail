import subprocess
import os
from calendar import monthrange

def run_script(script_name):
    import sys
    # Pass arguments year, month, day to script
    result = subprocess.run([sys.executable, script_name, year, month, day_str, sheetID], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
        return False
    print(f"Output of {script_name}: {result.stdout}")
    return True

if __name__ == "__main__":
    # Nhập tháng và năm muốn chạy
    year = input("Nhập năm (YYYY): ")
    month = input("Nhập tháng (MM): ")
    sheetID = input("Nhập SHEET_ID: ")
    scripts = [
        "0.download_item.py",
        "1.download_parquet.py",
        "2.beautify.py",
        "3.pivot.py"
    ]

    # Số ngày trong tháng
    num_days = monthrange(int(year), int(month))[1]

    for day in range(1, num_days + 1):
        day_str = f"{day:02d}"
        print(f"\n=== Đang xử lý ngày {year}-{month}-{day_str} ===")
        print(f"Running scripts: {scripts}")
        for script in scripts:
            if not run_script(script):
                print(f"Stopping execution due to error in {script}")
                break

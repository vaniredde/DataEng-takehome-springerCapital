
# Springer Capital - Data Engineer Take-Home Test (Pandas solution)

## Contents
- `script.py` - main processing script (Pandas). Produces `/output/final_referral_report.csv` and `/output/final_referral_report_46.csv`.
- `Dockerfile` - to build the container.
- `requirements.txt` - Python dependencies.
- `profiling_report.csv` - profiling results.
- `/output` - contains generated final reports after running the script.

## How to run locally
1. Place the dataset folder `DE Dataset - intern` inside `data_extract` in the project root (the script expects `/mnt/data/data_extract/DE Dataset - intern` in this environment).
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Run the script:
```
python your_script.py
```
4. Output files will be in `/output/`:
- `final_referral_report.csv` (all rows)
- `final_referral_report_46.csv` (aggregated 46-row report)

## How to run with Docker
1. Build the image:
```
docker build -t springer-de-takehome -f Dockerfile .
```
2. Run the container (mount the data directory to `/mnt/data`):
```
docker run --rm -v /path/to/local/data:/mnt/data springer-de-takehome
```
3. After running, the outputs will be in your local `/path/to/local/data/output` directory.

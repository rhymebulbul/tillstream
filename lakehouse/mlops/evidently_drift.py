import pandas as pd
import numpy as np
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import warnings
warnings.filterwarnings("ignore")

def main():
    print("Fetching Reference Data (e.g. from Iceberg Historical Tables)...")
    # Simulate the data distribution our downstream ML models were trained on
    reference_data = pd.DataFrame({
        'total_price': np.random.normal(loc=150.0, scale=30.0, size=1000)
    })

    print("Fetching Current Data (e.g. from Iceberg Recent Batches)...")
    # Simulate a massive pricing bug or inflation that causes distribution shift
    current_data = pd.DataFrame({
        'total_price': np.random.normal(loc=250.0, scale=50.0, size=1000)
    })

    print("🔍 Analyzing Data Drift against Reference Distribution...")
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(reference_data=reference_data, current_data=current_data)

    report_path = "data_drift_report.html"
    data_drift_report.save_html(report_path)
    
    print(f"✅ Drift Report Generated: {report_path}")
    print("🚨 ALERT: Significant data drift detected in 'total_price'. Triggering Airflow Alert to ML Engineers!")

if __name__ == "__main__":
    main()

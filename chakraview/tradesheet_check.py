from analysis.calculate_metrics import CalculateMetrics
import pandas as pd


df = pd.read_parquet(r'C:\Users\admin\VSCode\research\research_framework\tradesheets\pricema_atr\PRICEMAATR_niftyfut_63_25_False.parquet')
metrics = CalculateMetrics()
metrics_dict = metrics.calculate_metrics(df, 230000, 0)
print(metrics_dict)
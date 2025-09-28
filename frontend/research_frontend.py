import streamlit as st
import threading
import plotly.express as px
import pandas as pd
import os
import plotly.io as pio
import numpy as np
import sys
from pathlib import Path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(BASE_DIR, "108 TEST LOGO.png")
# Add repo root to sys.path if it's not already there
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

# Now you can safely import your analysis module
from analysis.calculate_metrics import CalculateMetrics
pio.renderers.default = "browser"

def homepage():
    st.set_page_config(
        page_title="108 Capital Dashboard",
        page_icon=r"C:\Users\admin\Desktop\108 TEST LOGO.png",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'About': 'This is a Research Dashboard For the 108 Capital Team.'
        }
    )
    st.title("108 Capital Research Dashboard")
    st.write("Welcome to the Research Dashboard.")
    st.sidebar.title("Strategies Toggle")
    st.logo(logo_path)
    st.divider()


def calculate_metrics(folder_path, initial_margin):
    calc = CalculateMetrics()
    folder_path = folder_path
    metrics_list = []
    for file in os.listdir(folder_path):
        try:
            df = pd.read_parquet(folder_path + file)
            df_metrics, metrics = calc.calculate_metrics(df, initial_margin)
            uid = file.split('.')[0]
            metrics['uid'] = uid
            metrics_list.append(metrics)
        except Exception as e:
            print(f'ERROR in {file}: {e}')
    metrics_df = pd.DataFrame(metrics_list)
    st.dataframe(metrics_df)


def plot_all_eq_curves(folder_path, initial_margin):
    calc = CalculateMetrics()
    df_list = []

    for file in os.listdir(folder_path):
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(folder_path, file))
            df_metrics, metrics = calc.calculate_metrics(df, initial_margin)
            temp_df = pd.DataFrame({
                'Trade': df_metrics['timestamp'],
                'Equity Curve': df_metrics['Equity Curve'],
                'Strategy': file.replace('.parquet', '')
            })
            df_list.append(temp_df)

    # Combine all equity curves into a single DataFrame

    combined_df = pd.concat(df_list, ignore_index=True)

    # Plot with Plotly Express
    fig = px.line(
        combined_df,
        x='Trade',
        y='Equity Curve',
        color='Strategy',
        title='Equity Curves for Multiple Strategies',
        markers=True
    )

    # Display in Streamlit
    st.plotly_chart(fig, use_container_width=True)

def display_multi_select_strats(folder_path, initial_margin):
    combined_df = pd.DataFrame()
    portfolio_list = st.multiselect("Combined Portfolio Metrics", [file for file in os.listdir(folder_path)])
    for file in portfolio_list:
        portfolio_df = pd.read_parquet(folder_path + file)
        portfolio_df['date'] = pd.to_datetime(portfolio_df['timestamp']).dt.date
        if not combined_df.empty:
            combined_df = combined_df._append(portfolio_df)
        else:
            combined_df = portfolio_df

    if not combined_df.empty:
        combined_df = combined_df.sort_values(by='date', ascending=True)
        combined_df['Daily P/L'] = combined_df.groupby('date')['P/L'].transform(
            lambda x: [np.nan] * (len(x) - 1) + [x.sum()]
        )
        portfolio = combined_df[pd.notna(combined_df['Daily P/L'])]
        portfolio['Daily EQ'] = portfolio['Daily P/L'].cumsum()
        portfolio['eq curve'] = portfolio['Daily EQ'] + (initial_margin * len(portfolio_list))
        days = (portfolio['date'].iloc[-1] - portfolio['date'].iloc[0]).days
        cagr = ((portfolio['eq curve'].iloc[-1] / portfolio['eq curve'].iloc[0]) ** (365 / days)) - 1
        running_max = portfolio['eq curve'].cummax()
        drawdown = (portfolio['eq curve'] - running_max) / running_max
        mdd = drawdown.min()
        calmar = cagr / abs(mdd)
        metrics_dict = {
            'Portfolio CAGR': round(cagr*100, 2),
            'Portfolio MDD': round(mdd*100, 2),
            'Portfolio Calmar': round(calmar, 2)
        }
        metrics_df = pd.DataFrame([metrics_dict])
        st.write("Portfolio Metrics")
        st.dataframe(metrics_df)
        st.divider()

        fig = px.line(portfolio, x='date', y='eq curve', title='Portfolio Equity Curve')
        fig.update_traces(line_color='white')

        # Display in Streamlit
        st.plotly_chart(fig, use_container_width=True)


def pcco_driver(folder_path):
    strategies = ['PCCO_SPOT']
    selected_strat = st.sidebar.radio('Select A Strategy', strategies)
    if selected_strat == 'PCCO_SPOT':
        initial_margin = st.number_input('Initial Margin', 1, 100000000)
        plot_all_eq_curves(folder_path, initial_margin)
        calculate_metrics(folder_path, initial_margin)
        display_multi_select_strats(folder_path, initial_margin)


# C:/Users/admin/VSCode/tradesheets/pcco/

def main():
    homepage()
    pcco_driver(f'C:/Users/admin/VSCode/tradesheets/pcco/')
    


if __name__ == '__main__':
    main()

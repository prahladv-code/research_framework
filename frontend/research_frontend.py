import os
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"
import streamlit as st
import threading
import plotly.express as px
import pandas as pd
import plotly.io as pio
import numpy as np
import sys
from pathlib import Path
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(BASE_DIR, "108 TEST LOGO.png")
# Add repo root to sys.path if it's not already there
if REPO_ROOT not in sys.path:
    sys.path.append(REPO_ROOT)

# Now you can safely import your analysis module
from analysis.calculate_metrics import CalculateMetrics
from auth import verify_user
calc = CalculateMetrics()
pio.renderers.default = "browser"

# ---------- AUTH SESSION STATE ----------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = None


def login_page():
    st.set_page_config(
        page_title="108 Capital Login",
        page_icon="🔐",
        layout="centered"
    )

    st.title("🔐 108 Capital Dashboard Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if verify_user(username.strip(), password.strip()):
            st.session_state.authenticated = True
            st.session_state.username = username
            st.success("Login successful")
            st.rerun()
        else:
            st.error("Invalid username or password")



def homepage():
    st.set_page_config(
        page_title="108 Capital Dashboard",
        page_icon=f"./frontend/108 LOGO BLACK.png",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'About': 'This is a Research Dashboard For the 108 Capital Team.'
        }
    )
    st.title("108 Capital Research Dashboard")
    st.write("Welcome to the Research Dashboard.")
    st.logo('./frontend/108 LOGO BLACK.png', size='large')
    st.sidebar.title("Strategies Toggle")

    st.sidebar.markdown("---")
    st.sidebar.write(f"👤 Logged in as **{st.session_state.username}**")

    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.rerun()

    st.divider()


def calculate_metrics(folder_path, initial_margin, slippage_pct):
    folder_path = folder_path
    metrics_list = []
    for file in os.listdir(folder_path):
        try:
            df = pd.read_parquet(folder_path + file)
            df_metrics, metrics = calc.calculate_metrics(df, initial_margin, slippage_pct)
            uid = file.split('.parquet')[0]
            metrics['uid'] = uid
            print(metrics)
            metrics_list.append(metrics)
        except Exception as e:
            print(f'ERROR in {file}: {e}')
    metrics_df = pd.DataFrame(metrics_list)
    st.dataframe(metrics_df)
    return metrics_df


def calculate_pl_distribution(folder_path, initial_margin):
    def plot_pl_distribution(df):
        df_perc = calc.calculate_pl_distribution(df, initial_margin)
        dist = df_perc[df_perc['percentage_pl'].notna()]['percentage_pl']

        fig = px.histogram(
            df_perc,
            x='percentage_pl',
            nbins=200,
            title='Frequency Distribution Of P/L (Returns)',
            labels={'percentage_pl': '% Profit/Loss'},
            opacity=0.7,
            # histnorm='probability density',     # keep or remove based on your scale preference
        )

        fig.update_layout(
            xaxis_title='P/L',
            yaxis_title='Frequency',
            template='plotly_dark'
        )

        st.plotly_chart(fig, use_container_width=True)

    # --- existing UI logic ---
    uids = []
    for file in os.listdir(folder_path):
        uids.append(file[:-8])

    strat = st.selectbox(
        "Select A UID to Generate P/L Distribution",
        uids
    )

    if strat:
        df = pd.read_parquet(f"{folder_path}{strat}.parquet")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        start_date, end_date = st.date_input(
            "Select date range:",
            value=(df['timestamp'].min().date(), df['timestamp'].max().date()),
            min_value=df['timestamp'].min().date(),
            max_value=df['timestamp'].max().date()
        )

        filtered_df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]
        plot_pl_distribution(filtered_df)



def downloads_section():

    folder_path = ''

    if st.sidebar.checkbox("Go to Downloads"):
        strat = st.selectbox(
            "Select a Strat to Download Tradebooks:",
            ["PCCO_SPOT", "PCCO_OPT", "PRICEMA"]
        )

        if strat == "PCCO_SPOT":
            folder_path = './tradesheets/pcco/'
        elif strat == "PCCO_OPT":
            folder_path = './tradesheets/pcco_opt/'
        elif strat == "PRICEMA":
            folder_path = './tradesheets/pricema/'
        
        # Check if folder exists
        if os.path.exists(folder_path):
            parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

            if parquet_files:
                st.write(f"Found {len(parquet_files)} Parquet file(s):")

                # Loop through all parquet files
                for file in parquet_files:
                    file_path = os.path.join(folder_path, file)

                    try:
                        # Read parquet file
                        df = pd.read_parquet(file_path)

                        # Convert to CSV bytes
                        csv_bytes = df.to_csv(index=False).encode('utf-8')

                        # Download button
                        st.download_button(
                            label=f"⬇️ Download {file.replace('.parquet', '.csv')}",
                            data=csv_bytes,
                            file_name=file.replace('.parquet', '.csv'),
                            mime='text/csv'
                        )
                    except Exception as e:
                        st.error(f"❌ Failed to read {file}: {e}")
            else:
                st.info("No Parquet files found in this folder.")
        else:
            st.warning("⚠️ Folder path does not exist.")


def plot_all_eq_curves(folder_path, initial_margin, slippage_pct):

    df_list = []

    for file in os.listdir(folder_path):
        if file.endswith('.parquet'):
            df = pd.read_parquet(os.path.join(folder_path, file))
            df_metrics, metrics = calc.calculate_metrics(df, initial_margin, slippage_pct)
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

def display_multi_select_strats(folder_path, initial_margin, slippage_pct):
    portfolio_list = st.multiselect("Combined Portfolio Metrics", [file.split('.parquet')[0] for file in os.listdir(folder_path)])
    if portfolio_list:
        metrics_df, portfolio = calc.calculate_portfolio_metrics(portfolio_list, folder_path, initial_margin, slippage_pct)
        st.write("Portfolio Metrics")
        st.dataframe(metrics_df)
        st.divider()

        if not portfolio.empty:
            fig = px.line(portfolio, x='date', y='eq curve', title='Portfolio Equity Curve')
            fig.update_traces(line_color='white')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No equity curve data available for the selected portfolio.")
    else:
        st.info("Select one or more UIDs to generate combined portfolio metrics.")

def display_correlation_matrix(folder_path):
    portfolio_list = st.multiselect("Choose UIDs to generate correlation matrix.", [file.split('.parquet')[0] for file in os.listdir(folder_path)])
    if portfolio_list:
        corr_matrix = calc.calculate_correlation_matrix(portfolio_list, folder_path)
        st.write("Correlation Matrix")
        # Style the correlation matrix for color intensity
        styled_corr = (
            corr_matrix.style
            .background_gradient(cmap='RdBu_r', vmin=-1, vmax=1)  # intense color near ±1
            .format("{:.2f}")
        )

        # Display the styled DataFrame
        st.dataframe(styled_corr, use_container_width=True)
    else:
        st.info("Select one or More UIDs to generate correlation matrix.")


def calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct):
    if st.checkbox("Compute Average Of Optimizations"):
        metrics_df = calculate_metrics(folder_path, initial_margin, slippage_pct)
        def calculate_componentwise_averages(metrics_df: pd.DataFrame) -> pd.DataFrame:
            df = metrics_df.copy()

            # Get numeric columns
            numeric_cols = df.select_dtypes(include='number').columns.tolist()

            # Extract all unique UID components
            components = set()
            for uid in df['uid']:
                components.update(uid.split('_'))

            # Compute averages for each unique component
            data = []
            for comp in sorted(components):
                # Create mask without regex — check if comp in split(uid)
                mask = df['uid'].apply(lambda x: comp in x.split('_'))

                if not mask.any():
                    continue

                avg_values = df.loc[mask, numeric_cols].mean().to_dict()
                avg_values['component'] = comp
                data.append(avg_values)

            result_df = pd.DataFrame(data)

            if result_df.empty:
                st.warning("No components matched. Check UID structure or values.")
                return pd.DataFrame()

            # Put 'component' column first
            result_df = result_df[['component'] + [col for col in result_df.columns if col != 'component']]
            return result_df

        # Step 2: Get averages
        avg_df = calculate_componentwise_averages(metrics_df)

        # Step 3: Sort by CAGR descending
        if not avg_df.empty and 'cagr' in avg_df.columns:

            # Step 4: Show in Streamlit with color gradient
            st.dataframe(
                avg_df.style.background_gradient(
                    cmap='coolwarm_r'  # blue = higher metrics, red = lower
                ).format(precision=4)
            )
        else:
            st.warning("No CAGR column found or dataframe is empty.")

        return avg_df
    else:
        st.info("Select The Checkbox Above to Display Average Of Optimizations.")



def concat_all_uids(folder_path, uid_list, strat_weightage, debug=False):
    """
    Concatenate multiple parquet files based on uid_list.
    
    Args:
        folder_path: Path to folder containing parquet files
        uid_list: List of filenames (with or without extensions) to concatenate
        debug: If True, print debug information
    
    Returns:
        Combined DataFrame from all matching files
    """
    all_dfs = []
    matched_files = []
    
    # Normalize uid_list to handle both formats (with/without .parquet extension)
    normalized_uids = set()
    for uid in uid_list:
        if uid.endswith('.parquet'):
            normalized_uids.add(uid.replace('.parquet', ''))
        else:
            normalized_uids.add(uid)
    
    if debug:
        print(f"Looking for UIDs: {normalized_uids}")
        print(f"In folder: {folder_path}")
    
    for file in os.listdir(folder_path):
        if file.endswith('.parquet'):
            uid = file.replace('.parquet', '')
            
            if uid in normalized_uids:
                file_path = os.path.join(folder_path, file)
                df = pd.read_parquet(file_path)
                all_dfs.append(df)
                matched_files.append(file)
                if debug:
                    print(f"✓ Matched: {file}")
    
    # Check if we found any matching files
    if not all_dfs:
        available_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        raise ValueError(
            f"No matching files found in {folder_path}\n"
            f"Requested UIDs: {list(normalized_uids)}\n"
            f"Available files: {available_files[:10]}"  # Show first 10
        )
    
    if debug:
        print(f"Total files matched: {len(matched_files)}")
    
    strat_df = pd.concat(all_dfs, ignore_index=True)
    strat_df['P/L'] = strat_df['P/L'] * strat_weightage
    return strat_df


def calculate_monthly_returns(df, initial_margin):
    monthly_returns_tble = calc.calculate_monthly_returns(df, initial_margin)
    return monthly_returns_tble


def portfolios_driver():
    folder_paths = {
        'PCCO_SPOT': './tradesheets/pcco/',
        'PCCO_OPT': './tradesheets/pcco_opt/',
        'PRICEMA': './tradesheets/pricema/',
        'PRICEMA_ATR': './tradesheets/pricema_atr/',
        'PRICEMA_TRAIL': './tradesheets/pricema_atr_exit/',
        'IVIX': './tradesheets/ivix/'
    }
    strategies = ['PCCO_SPOT', 'PCCO_OPT', 'PRICEMA', 'PRICEMA_ATR', 'PRICEMA_TRAIL', 'IVIX']
    
    # Select strategies
    strats = st.multiselect('Strategies', strategies)
    
    # Input parameters
    initial_margin = st.number_input('Initial Margin', 1, 100000000, key='initial_margin')
    slippage_pct = st.number_input('Slippage Percentage', 0.0, 0.05, key='slippage')
    
    
    selected_tradesheets = {}
    selected_weights = {}
    # Only show tradesheet selection if strategies are selected
    if strats:
        st.subheader("Input Weightages for Selected Strategies")
        for strat in strats:
            weight = st.number_input(
                f"Weight for {strat}",
                min_value=0.0,
                max_value=100.0,
                value=1.0,
                step=0.5,
                key=f"weight_{strat}"
            )
            selected_weights[strat] = weight

        st.subheader("Select Tradesheets")
        
        for strat in strats:
            folder_path = folder_paths.get(strat)
            
            # Get list of tradesheets in the folder
            if os.path.exists(folder_path):
                tradesheets = [f for f in os.listdir(folder_path) 
                             if os.path.isfile(os.path.join(folder_path, f))]
                
                # Filter for common file types (optional)
                tradesheets = [f for f in tradesheets 
                             if f.endswith(('.csv', '.xlsx', '.xls', '.parquet'))]
                
                if tradesheets:
                    # Create multiselect for each strategy
                    selected = st.multiselect(
                        f'{strat} Tradesheets',
                        tradesheets,
                        key=f'tradesheets_{strat}'
                    )
                    
                    if selected:
                        selected_tradesheets[strat] = selected
                        
                else:
                    st.warning(f"No tradesheets found in {folder_path}")
            else:
                st.error(f"Folder not found: {folder_path}")
        
        if selected_tradesheets:
            # st.write(f'Selected Strats: {selected_tradesheets}')
            all_dfs = []
            # Fixed: Use .items() to properly unpack dictionary
            for strat, uids in selected_tradesheets.items():
                concated_df = concat_all_uids(folder_paths.get(strat), uids, selected_weights.get(strat))
                all_dfs.append(concated_df)

            portfolio_df = pd.concat(all_dfs, ignore_index=True)
            portfolio_df['timestamp'] = pd.to_datetime(portfolio_df['timestamp'])
            portfolio_df = portfolio_df.sort_values(by='timestamp')
            df_metrics, metrics = calc.calculate_metrics(portfolio_df, initial_margin, slippage_pct)
            st.write("Portfolio Metrics")
            st.dataframe([metrics])
            st.divider()

            if not df_metrics.empty:
                fig = px.line(df_metrics, x='timestamp', y='Equity Curve', title='Portfolio Equity Curve')
                fig.update_traces(line_color='white')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No equity curve data available for the selected portfolio.")
            
            st.divider()
            st.write('Portfolio Monthly Returns')
            monthly_df = calculate_monthly_returns(portfolio_df, initial_margin)
            st.dataframe(monthly_df)
        

def strategy_driver():
    strategies = ['PCCO_SPOT', 'PCCO_OPT', 'PRICEMA', 'PRICEMA_ATR', 'PRICEMA_TRAIL', 'IVIX']  # both options in the same radio
    selected_strat = st.sidebar.radio('Select A Strategy', strategies, key='pcco_strategy')
    folder_paths = {
        'PCCO_SPOT': './tradesheets/pcco/',
        'PCCO_OPT': './tradesheets/pcco_opt/',
        'PRICEMA': './tradesheets/pricema/',
        'PRICEMA_ATR': './tradesheets/pricema_atr/',
        'PRICEMA_TRAIL': './tradesheets/pricema_atr_exit/',
        'IVIX': './tradesheets/ivix/'
    }
    initial_margin = st.number_input('Initial Margin', 1, 100000000, key='initial_margin')
    slippage_pct = st.number_input('Slippage Percentage', 0.0, 0.05, key = 'slippage')

    if selected_strat == 'PCCO_SPOT':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)

    elif selected_strat == 'PCCO_OPT':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)
    
    elif selected_strat == 'PRICEMA':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)
    
    elif selected_strat == 'PRICEMA_ATR':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)
    
    elif selected_strat == 'PRICEMA_TRAIL':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)
    
    elif selected_strat == 'IVIX':
        folder_path = folder_paths.get(selected_strat)
        plot_all_eq_curves(folder_path, initial_margin, slippage_pct)
        calculate_metrics(folder_path, initial_margin, slippage_pct)
        display_multi_select_strats(folder_path, initial_margin, slippage_pct)
        display_correlation_matrix(folder_path)
        calculate_avergae_optimizations(folder_path, initial_margin, slippage_pct)
        calculate_pl_distribution(folder_path, initial_margin)




# C:/Users/admin/VSCode/tradesheets/pcco/

def main():
    
    if not st.session_state.authenticated:
        login_page()
        return  # ⛔ stop execution here
    
    homepage()
    section = st.sidebar.radio("Select section:", ["Strats", "Downloads", "Portfolios"])
    if section == "Downloads":
        downloads_section()
    elif section == "Strats":
        strategy_driver()
    elif section == 'Portfolios':
        portfolios_driver()
    


if __name__ == '__main__':
    main()
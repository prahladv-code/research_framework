import pandas as pd
import numpy as np


class CalculateMetrics:
    
    """
    Utility module for trade-level and portfolio-level performance analytics.

    This module contains the `CalculateMetrics` class, which provides a collection
    of helper functions for calculating realized profit/loss, equity curves,
    risk-adjusted performance metrics, portfolio aggregation statistics,
    correlation matrices, and return distributions for systematic trading
    strategies.

    The module supports:
    - Intraday tradesheets
    - Options tradesheets
    - Positional tradesheets
    - Portfolio-level aggregation across multiple strategies

    Core functionality includes:
    - Trade pairing logic for BUY/SELL and SHORT/COVER transactions
    - Realized P/L computation
    - Slippage-adjusted equity curve generation
    - CAGR, Calmar ratio, Sortino ratio, payoff ratio, and profit factor
    - Drawdown and drawdown recovery duration analysis
    - Recovery factor computation
    - Monthly return calendar generation
    - Strategy correlation matrix generation
    - Portfolio-level daily equity aggregation

    The calculations are designed primarily for backtesting and strategy research
    workflows and assume that trade execution data is already available in a
    structured pandas DataFrame format.

    Expected DataFrame columns vary by function but commonly include:
    - timestamp
    - trade
    - price
    - qty
    - symbol
    - cv
    - P/L

    Dependencies:
    - pandas
    - numpy

    Notes:
    - Most functions operate in-place on copies of DataFrames.
    - Drawdown duration is measured using equity curve peak-to-peak recovery time.
    - Slippage can be modeled either as percentage-based or fixed-point based.
    - Existing logic and calculations are intentionally preserved as-is for
    research consistency, even where certain financial assumptions may not
    be theoretically ideal.
    """

    def __init__(self):
        pass

    def calculate_pl_in_tradesheet(self, df):
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d%H:%M:%S')

        # Initialize P/L column
        df['P/L'] = np.nan

        # Group by date
        df_groups = df.groupby(df['timestamp'].dt.date)

        for group, df1 in df_groups:
            if len(df1) == 2:
                if df1['trade'].iloc[0] == 'BUY':
                    entry_price = df1.loc[df1['trade'] == 'BUY', 'price'].iloc[0]
                    exit_price = df1.loc[df1['trade'] == 'SELL', 'price'].iloc[0]
                    df.loc[df1.index[-1], 'P/L'] = exit_price - entry_price

                elif df1['trade'].iloc[0] == 'SHORT':
                    entry_price = df1.loc[df1['trade'] == 'SHORT', 'price'].iloc[0]
                    exit_price = df1.loc[df1['trade'] == 'COVER', 'price'].iloc[0]
                    df.loc[df1.index[-1], 'P/L'] = entry_price - exit_price
            else:
                # If incomplete trade, set P/L at last row = 0
                df.loc[df1.index[-1], 'P/L'] = 0

        return df
    
    def calculate_pl_in_opt_tradesheet(self, df):

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        df = df.sort_values(['timestamp', 'symbol']).reset_index(drop=True)

        df['P/L'] = np.nan

        # store open trades per symbol
        open_positions = {}

        for idx, row in df.iterrows():

            symbol = row['symbol']
            trade = row['trade']
            price = row['price']
            qty = row['qty']

            if symbol not in open_positions:
                open_positions[symbol] = []

            # ENTRY TRADES
            if trade in ['BUY', 'SHORT']:
                open_positions[symbol].append({
                    'price': price,
                    'qty': qty,
                    'type': trade,
                    'index': idx
                })

            # EXIT TRADES
            elif trade in ['SELL', 'COVER']:

                if open_positions[symbol]:

                    entry = open_positions[symbol].pop(0)
                    entry_price = entry['price']
                    entry_type = entry['type']

                    if entry_type == 'BUY' and trade == 'SELL':
                        pl = (price - entry_price) * qty

                    elif entry_type == 'SHORT' and trade == 'COVER':
                        pl = (entry_price - price) * qty

                    else:
                        pl = 0  # mismatched trade types

                    df.loc[idx, 'P/L'] = pl

                else:
                    df.loc[idx, 'P/L'] = 0

        # remaining open positions → mark last row per symbol as 0
        for symbol, entries in open_positions.items():
            for entry in entries:
                df.loc[entry['index'], 'P/L'] = 0

        return df


    def calculate_pl_in_positional_tradesheet(self, df):

        # Add columns
        df['P/L'] = np.nan

        # Initialize trackers
        entry_cv = None
        entry_trade = None

        mtm_entry_cv = None
        mtm_entry_trade = None

        for i, row in df.iterrows():
            trade = row['trade']
            cv = row['cv']

            # ---- REAL TRADES ----
            if "MTM" not in trade:
                if trade in ['BUY', 'SHORT']:
                    entry_cv = cv
                    entry_trade = trade

                elif trade in ['SELL', 'COVER'] and entry_trade is not None:
                    if entry_trade == 'BUY' and trade == 'SELL':
                        df.at[i, 'P/L'] = cv - entry_cv
                    elif entry_trade == 'SHORT' and trade == 'COVER':
                        df.at[i, 'P/L'] = entry_cv - cv  # short gains when cv drops
                    entry_cv = None
                    entry_trade = None

        return df

    
    def calculate_metrics(self, df, initial_margin, slippage_pct, slippage_points: float = None):
        df = df[df['P/L'].notna()].copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        slippage = 0
        if slippage_points > 0:
            slippage = slippage_points
        else:
            slippage = df['cv'] * slippage_pct

        df['P/L'] = df['P/L'] - slippage
        df['cumsum'] = df['P/L'].cumsum()
        df['Equity Curve'] = df['cumsum'] + initial_margin
        
        days = ((df['timestamp'].iloc[-1].date()) - (df['timestamp'].iloc[0].date())).days
        end_value = df['Equity Curve'].iloc[-1]
        start_value = df['Equity Curve'].iloc[0]
        
        absolute_return = end_value - start_value
        absolute_percentage = absolute_return/initial_margin * 100
        cagr = ((end_value/start_value) ** (365/days) - 1)*100
        
        df['cummax'] = df['Equity Curve'].cummax()
        df['drawdown'] = ((df['Equity Curve'] - df['cummax'])/df['cummax']) * 100
        max_drawdown = df['drawdown'].min()
        calmar = cagr/abs(max_drawdown)
        
        total_trades = len(df[(df['P/L'].notna()) & (df['P/L'] != 0)])
        winners = len(df[df['P/L'] > 0])
        losers = len(df[df['P/L'] < 0])
        win_percentage = winners/total_trades * 100
        loss_percentage = losers/total_trades * 100
        
        profit_factor = df.loc[df['P/L'] > 0, 'P/L'].sum() / abs(df.loc[df['P/L'] < 0, 'P/L'].sum())
        payoff_ratio = df.loc[df['P/L'] > 0, 'P/L'].mean() / abs(df.loc[df['P/L'] < 0, 'P/L'].mean())
        
        avg_win = df.loc[df['P/L'] > 0, 'P/L'].mean()
        avg_loss = abs(df.loc[df['P/L'] < 0, 'P/L'].mean())
        avg_win_percentage = avg_win/initial_margin*100
        avg_loss_percentage = avg_loss/initial_margin*100
        trading_edge = (avg_win_percentage*win_percentage) - (avg_loss_percentage*loss_percentage)
        
        downside_deviation = abs(df.loc[df['P/L'] < 0, 'P/L'].std())
        downside_deviation = downside_deviation/initial_margin * 100
        basic_sortino = cagr/downside_deviation

        # --- New: drawdown recovery time in days ---
        # Assuming df already has 'Equity Curve' and 'timestamp' columns
        df['cummax_change'] = df['cummax'] != df['cummax'].shift()

        # Record the date when a new peak occurs
        df['drawdown_date'] = np.where(df['cummax_change'], pd.to_datetime(df['timestamp']).dt.date, pd.NaT)

        # Keep only the new peak dates
        drawdown_dates = df.loc[df['cummax_change'], 'drawdown_date'].dropna().reset_index(drop=True)

        # Compute the differences between consecutive peak dates
        if len(drawdown_dates) > 1:
            differences = drawdown_dates.diff().dropna()  # results in Timedelta objects
            max_difference = differences.max().days   # max drawdown duration in days
        else:
            max_difference = 0  # only one peak, no drawdown

        # Recovery factor = absolute return / abs(max drawdown)
        recovery_factor = absolute_percentage / abs(max_drawdown)

        metrics_dict = { 
            'absolute_return': absolute_return,
            'absolute_percentage': absolute_percentage,
            'cagr': cagr,
            'mdd': max_drawdown,
            'calmar': calmar, 
            'win_percentage': win_percentage, 
            'loss_percentage': loss_percentage, 
            'average_win_percentage': avg_win_percentage,
            'average_loss_percentage': avg_loss_percentage,
            'profit_factor': profit_factor, 
            'payoff_ratio': payoff_ratio,
            'trading_edge': trading_edge,
            'basic_sortino': basic_sortino,
            'drawdown_duration_days': max_difference,
            'recovery_factor': recovery_factor
        }
        
        return df, metrics_dict

    
    def calculate_portfolio_metrics(self, portfolio_list, folder_path, initial_margin, slippage_pct, slippage_points: float = None):
        combined_df = pd.DataFrame()
        for file in portfolio_list:
            portfolio_df = pd.read_parquet(folder_path + file + '.parquet')
            portfolio_df['date'] = pd.to_datetime(portfolio_df['timestamp']).dt.date
            slippage = 0
            if slippage_points > 0:
                slippage = slippage_points
            else:
                slippage = portfolio_df['cv'] * slippage_pct
            
            portfolio_df['P/L'] = portfolio_df['P/L'] - slippage
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

            total_trades = len(portfolio[portfolio['Daily P/L'] != 0])
            winners = len(portfolio[portfolio['Daily P/L'] > 0])
            losers = len(portfolio[portfolio['Daily P/L'] < 0])
            win_percentage = winners / total_trades * 100
            loss_percentage = losers / total_trades * 100

            profit_factor = portfolio.loc[portfolio['Daily P/L'] > 0, 'Daily P/L'].sum() / abs(
                portfolio.loc[portfolio['Daily P/L'] < 0, 'Daily P/L'].sum()
            )
            payoff_ratio = portfolio.loc[portfolio['Daily P/L'] > 0, 'Daily P/L'].mean() / abs(
                portfolio.loc[portfolio['Daily P/L'] < 0, 'Daily P/L'].mean()
            )

            avg_win = portfolio.loc[portfolio['Daily P/L'] > 0, 'Daily P/L'].mean()
            avg_loss = abs(portfolio.loc[portfolio['Daily P/L'] < 0, 'Daily P/L'].mean())
            avg_win_percentage = avg_win / initial_margin * 100
            avg_loss_percentage = avg_loss / initial_margin * 100
            trading_edge = (avg_win_percentage * win_percentage) - (avg_loss_percentage * loss_percentage)

            # Equity curve
            portfolio['Daily EQ'] = portfolio['Daily P/L'].cumsum()
            portfolio['eq curve'] = portfolio['Daily EQ'] + (initial_margin * len(portfolio_list))

            # CAGR
            days = (portfolio['date'].iloc[-1] - portfolio['date'].iloc[0]).days
            cagr = ((portfolio['eq curve'].iloc[-1] / portfolio['eq curve'].iloc[0]) ** (365 / days)) - 1

            downside_deviation = abs(portfolio.loc[portfolio['Daily P/L'] < 0, 'Daily P/L'].std())
            downside_deviation = downside_deviation/initial_margin * 100
            basic_sortino = cagr / downside_deviation if downside_deviation != 0 else np.nan

            running_max = portfolio['eq curve'].cummax()
            drawdown = (portfolio['eq curve'] - running_max) / running_max
            portfolio['drawdown'] = drawdown
            portfolio['cummax'] = running_max
            mdd = drawdown.min()
            calmar = cagr / abs(mdd)

            # --- New: Drawdown recovery time and recovery factor ---
            portfolio['cummax_change'] = portfolio['cummax'] != portfolio['cummax'].shift()

            # Record the date when a new peak occurs
            portfolio['drawdown_date'] = np.where(portfolio['cummax_change'], pd.to_datetime(portfolio['timestamp']).dt.date, pd.NaT)

            # Keep only the new peak dates
            drawdown_dates = portfolio.loc[portfolio['cummax_change'], 'drawdown_date'].dropna().reset_index(drop=True)

            # Compute the differences between consecutive peak dates
            if len(drawdown_dates) > 1:
                differences = drawdown_dates.diff().dropna()  # results in Timedelta objects
                max_difference = differences.max().days   # max drawdown duration in days
            else:
                max_difference = 0  # only one peak, no drawdown


            metrics_dict = {
                'Portfolio CAGR': round(cagr * 100, 2),
                'Portfolio MDD': round(mdd * 100, 2),
                'Portfolio Calmar': round(calmar, 2),
                'Portfolio Profit Factor': profit_factor,
                'Portfolio Payoff Ratio': payoff_ratio,
                'Portfolio Trading Edge': trading_edge,
                'Portfolio Basic Sortino': basic_sortino,
                'Portfolio Drawdown Duration (days)': max_difference,
            }

            metrics_df = pd.DataFrame([metrics_dict])
            return metrics_df, portfolio
        else:
            return pd.DataFrame(), pd.DataFrame()

        
    def calculate_correlation_matrix(self, portfolio_list, folder_path):
        combined_df = pd.DataFrame()
        for file in portfolio_list:
            df = pd.read_parquet(folder_path + file + '.parquet')
            pl_series = df['P/L'].reset_index(drop=True)
            combined_df[file] = pl_series
        
        combined_df = combined_df.dropna()

        corr_matrix = combined_df.corr()
        return corr_matrix
    
    def calculate_pl_distribution(self, df, initial_margin):
        df['P/L'] = pd.to_numeric(df['P/L'], errors='coerce')
        df['percentage_pl'] = (df['P/L']/initial_margin)*100
        return df

    def calculate_monthly_returns(slef, df, initial_margin):
        
        # Ensure datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Clean P/L
        df['P/L'] = df['P/L'].fillna(0)

        # Use realized P/L only (as per your code)
        df['total_pl'] = df['P/L']

        # Extract year and month
        df['Year'] = df['timestamp'].dt.year
        df['Month'] = df['timestamp'].dt.month

        # Aggregate monthly P/L
        monthly = (
            df.groupby(['Year', 'Month'])['total_pl']
            .sum()
            .reset_index()
        )

        # Convert to percentage returns
        monthly['Return %'] = (monthly['total_pl'] / initial_margin) * 100

        # Pivot into calendar format
        calendar_returns = monthly.pivot(
            index='Year',
            columns='Month',
            values='Return %'
        )

        # Rename month numbers to names
        calendar_returns.columns = [
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
        ]

        # Optional: sort years
        calendar_returns = calendar_returns.sort_index()

        return calendar_returns

    def calculate_block_bootstrapped_simulation(self, combined_df: pd.DataFrame, initial_margin: float, block_size: int):
        INITIAL_CAPITAL = initial_margin
        N_BOOTSTRAPS = 10000
        TRADING_DAYS_PER_YEAR = 252
        BLOCK_SIZE = block_size

        pl = combined_df["Net P/L"].dropna().to_numpy()
        n_trades = len(pl)

        # ============================================================
        # Original duration (years)
        # ============================================================

        if "date" in combined_df.columns:

            combined_df["date"] = pd.to_datetime(
                combined_df["date"]
            )

            years = (
                (
                    combined_df["date"].max()
                    - combined_df["date"].min()
                ).days
                / 365.25
            )

        else:

            years = (
                n_trades
                / TRADING_DAYS_PER_YEAR
            )

        # ============================================================
        # Helper functions
        # ============================================================

        def max_drawdown(eq_curve):

            running_max = np.maximum.accumulate(
                eq_curve
            )

            drawdown = (
                eq_curve - running_max
            ) / running_max

            return drawdown.min()


        def calmar(cagr, mdd):

            return cagr / abs(mdd)


        def CAGR(eq_curve, years):

            # IMPORTANT:
            # CAGR starts from the actual initial capital,
            # not the equity after the first trade.

            start = INITIAL_CAPITAL

            end = eq_curve[-1]

            return (
                (end / start) ** (1 / years)
            ) - 1

        # ============================================================
        # Create overlapping blocks
        #
        # Example with BLOCK_SIZE = 50:
        #
        # Block 1 = trades 0:50
        # Block 2 = trades 1:51
        # Block 3 = trades 2:52
        # ...
        #
        # Overlapping blocks preserve local trade behaviour.
        # ============================================================

        blocks = []

        for start in range(
            0,
            n_trades - BLOCK_SIZE + 1
        ):

            block = pl[
                start:start + BLOCK_SIZE
            ]

            blocks.append(block)


        n_blocks = len(blocks)

        print(f"Number of available blocks: {n_blocks:,}")


        # ============================================================
        # Bootstrap arrays
        # ============================================================

        boot_cagr = np.zeros(
            N_BOOTSTRAPS
        )

        boot_mdd = np.zeros(
            N_BOOTSTRAPS
        )

        boot_calmar = np.zeros(
            N_BOOTSTRAPS
        )


        # ============================================================
        # Block Simulation
        # ============================================================

        for i in range(N_BOOTSTRAPS):

            simulated_pl = []

            # --------------------------------------------------------
            # Keep selecting random blocks until we have enough trades
            # --------------------------------------------------------

            while len(simulated_pl) < n_trades:

                block_index = np.random.randint(
                    0,
                    n_blocks
                )

                simulated_pl.extend(
                    blocks[block_index]
                )

            # --------------------------------------------------------
            # Trim to exactly the original number of trades
            # --------------------------------------------------------

            simulated_pl = np.asarray(
                simulated_pl[:n_trades]
            )

            # --------------------------------------------------------
            # Non-compounded equity curve
            # --------------------------------------------------------

            eq = (
                INITIAL_CAPITAL
                + simulated_pl.cumsum()
            )

            # --------------------------------------------------------
            # CAGR
            # --------------------------------------------------------

            boot_cagr[i] = CAGR(
                eq,
                years
            )

            # --------------------------------------------------------
            # Maximum Drawdown
            # --------------------------------------------------------

            boot_mdd[i] = max_drawdown(
                eq
            )

            # --------------------------------------------------------
            # Calmar
            # --------------------------------------------------------

            boot_calmar[i] = calmar(
                boot_cagr[i],
                boot_mdd[i]
            )


        # ============================================================
        # Confidence Intervals
        # ============================================================

        def confidence_interval(x):

            return {
                "Mean": np.mean(x),
                "Median": np.median(x),
                "2.5%": np.percentile(x, 2.5),
                "5%": np.percentile(x, 5),
                "95%": np.percentile(x, 95),
                "97.5%": np.percentile(x, 97.5),
            }

        cagr_metrics = confidence_interval(boot_cagr)
        mdd_metrics = confidence_interval(boot_mdd)
        calmar_metrics = confidence_interval(boot_calmar)
        all_confidence_intervals = {'CAGR': pd.DataFrame([cagr_metrics]), 'MDD': pd.DataFrame([mdd_metrics]), 'Calmar': pd.DataFrame([calmar_metrics])}
        return {"CAGR": boot_cagr, "MDD": boot_mdd, "Calmar": boot_calmar}, all_confidence_intervals



# def calculate_metrics(self, df, initial_margin, slippage_pct): df = df[df['P/L'].notna()].copy() slippage = df['cv'] * slippage_pct df['P/L'] = df['P/L'] - slippage df['cumsum'] = df['P/L'].cumsum() df['Equity Curve'] = df['cumsum'] + initial_margin days = ((df['timestamp'].iloc[-1].date()) - (df['timestamp'].iloc[0].date())).days end_value = df['Equity Curve'].iloc[-1] start_value = df['Equity Curve'].iloc[0] absolute_return = end_value - start_value absolute_percentage = absolute_return/initial_margin * 100 cagr = ((end_value/start_value) ** (365/days) - 1)*100 df['cummax'] = df['Equity Curve'].cummax() df['drawdown'] = ((df['Equity Curve'] - df['cummax'])/df['cummax']) * 100 max_drawdown = df['drawdown'].min() calmar = cagr/abs(max_drawdown) total_trades = len(df[df['P/L'].notna()]) winners = len(df[df['P/L'] > 0]) losers = len(df[df['P/L'] < 0]) win_percentage = winners/total_trades * 100 loss_percentage = losers/total_trades * 100 profit_factor = df.loc[df['P/L'] > 0, 'P/L'].sum() / abs(df.loc[df['P/L'] < 0, 'P/L'].sum()) payoff_ratio = df.loc[df['P/L'] > 0, 'P/L'].mean() / abs(df.loc[df['P/L'] < 0, 'P/L'].mean()) avg_win = df.loc[df['P/L'] > 0, 'P/L'].mean() avg_loss = abs(df.loc[df['P/L'] < 0, 'P/L'].mean()) avg_win_percentage = avg_win/initial_margin*100 avg_loss_percentage = avg_loss/initial_margin*100 trading_edge = (avg_win_percentage*win_percentage) - (avg_loss_percentage*loss_percentage) downside_deviation = abs(df.loc[df['P/L'] < 0, 'P/L'].std()) basic_sortino = cagr/downside_deviation metrics_dict = { 'absolute_return': absolute_return, 'absolute_percentage': absolute_percentage, 'cagr': cagr, 'mdd': max_drawdown, 'calmar': calmar, 'win_percentage': win_percentage, 'loss_percentage': loss_percentage, 'average_win_percentage': avg_win_percentage, 'average_loss_percentage': avg_loss_percentage, 'profit_factor': profit_factor, 'payoff_ratio': payoff_ratio, 'trading_edge': trading_edge, 'basic_sortino': basic_sortino} return df, metrics_dict In this function, keep everything the same (even if wrong). Just add a metric to measure time taken from drawdown rercovery (from peak to trough to peak) and the recovery factor.
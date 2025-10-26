import pandas as pd
import numpy as np

class CalculateMetrics:
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
                    exit_rows = df1.loc[df1['trade'] == 'SELL', 'price']
                    qty = df1['qty'].iloc[0]
                    if not exit_rows.empty:
                        exit_price = exit_rows.iloc[0]
                        df.loc[df1.index[-1], 'P/L'] = (exit_price - entry_price) * qty
                    else:
                        # Handle incomplete trade
                        df.loc[df1.index[-1], 'P/L'] = 0

                elif df1['trade'].iloc[0] == 'SHORT':
                    entry_price = df1.loc[df1['trade'] == 'SHORT', 'price'].iloc[0]
                    exit_rows = df1.loc[df1['trade'] == 'COVER', 'price']
                    qty = df1['qty'].iloc[0]
                    if not exit_rows.empty:
                        exit_price = exit_rows.iloc[0]
                        df.loc[df1.index[-1], 'P/L'] = (entry_price - exit_price) * qty
                    else:
                        # Handle incomplete trade
                        df.loc[df1.index[-1], 'P/L'] = 0
            else:
                # If incomplete trade, set P/L at last row = 0
                df.loc[df1.index[-1], 'P/L'] = 0

        return df

    def calculate_pl_in_positional_tradesheet(self, df):

        # Add columns
        df['P/L'] = 0.0
        df['MTM P/L'] = 0.0

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

            # ---- MTM TRADES ----
            else:
                if trade in ['MTM_BUY', 'MTM_SHORT']:
                    mtm_entry_cv = cv
                    mtm_entry_trade = trade

                elif trade in ['MTM_SELL', 'MTM_COVER'] and mtm_entry_trade is not None:
                    if mtm_entry_trade == 'MTM_BUY' and trade == 'MTM_SELL':
                        df.at[i, 'MTM P/L'] = cv - mtm_entry_cv
                    elif mtm_entry_trade == 'MTM_SHORT' and trade == 'MTM_COVER':
                        df.at[i, 'MTM P/L'] = mtm_entry_cv - cv
                    mtm_entry_cv = None
                    mtm_entry_trade = None

        return df

    
    def calculate_metrics(self, df, initial_margin, slippage_pct):
        df = df[df['P/L'].notna()].copy()
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
        
        total_trades = len(df[df['P/L'].notna()])
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
        basic_sortino = cagr/downside_deviation

        # --- New: drawdown recovery time in days ---
        drawdown_start = df['drawdown'].idxmin()  # trough index
        peak_before_trough = df['Equity Curve'][:drawdown_start].idxmax()  # peak before trough
        # first index after trough where Equity Curve >= previous peak
        # Slice equity curve from trough to the end
        eq_slice = df['Equity Curve'].loc[drawdown_start:]

        # Boolean mask for recovery points
        recovery_points = eq_slice.index[eq_slice >= df['Equity Curve'].loc[peak_before_trough]]

        # Compute duration
        if len(recovery_points) > 0:
            recovery_index = recovery_points[0]
            drawdown_duration_days = (df['timestamp'].iloc[recovery_index] - df['timestamp'].iloc[peak_before_trough]).days
        else:
            drawdown_duration_days = None  # not recovered yet


        # Recovery factor = absolute return / abs(max drawdown)
        recovery_factor = absolute_return / abs(max_drawdown)

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
            'drawdown_duration_days': drawdown_duration_days,
            'recovery_factor': recovery_factor
        }
        
        return df, metrics_dict

    
    def calculate_portfolio_metrics(self, portfolio_list, folder_path, initial_margin, slippage_pct):
        combined_df = pd.DataFrame()
        for file in portfolio_list:
            portfolio_df = pd.read_parquet(folder_path + file + '.parquet')
            portfolio_df['date'] = pd.to_datetime(portfolio_df['timestamp']).dt.date
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

            total_trades = len(portfolio['Daily P/L'])
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
            basic_sortino = cagr / downside_deviation if downside_deviation != 0 else np.nan

            running_max = portfolio['eq curve'].cummax()
            drawdown = (portfolio['eq curve'] - running_max) / running_max
            mdd = drawdown.min()
            calmar = cagr / abs(mdd)

            # --- New: Drawdown recovery time and recovery factor ---
            drawdown_start = df['drawdown'].idxmin()  # trough index
            peak_before_trough = df['Equity Curve'][:drawdown_start].idxmax()  # peak before trough
            # first index after trough where Equity Curve >= previous peak
            # Slice equity curve from trough to the end
            eq_slice = df['Equity Curve'].loc[drawdown_start:]

            # Boolean mask for recovery points
            recovery_points = eq_slice.index[eq_slice >= df['Equity Curve'].loc[peak_before_trough]]

            # Compute duration
            if len(recovery_points) > 0:
                recovery_index = recovery_points[0]
                drawdown_duration_days = (df['timestamp'].iloc[recovery_index] - df['timestamp'].iloc[peak_before_trough]).days
            else:
                drawdown_duration_days = None  # not recovered yet

            recovery_factor = (portfolio['eq curve'].iloc[-1] - portfolio['eq curve'].iloc[0]) / abs(mdd)

            metrics_dict = {
                'Portfolio CAGR': round(cagr * 100, 2),
                'Portfolio MDD': round(mdd * 100, 2),
                'Portfolio Calmar': round(calmar, 2),
                'Portfolio Profit Factor': profit_factor,
                'Portfolio Payoff Ratio': payoff_ratio,
                'Portfolio Trading Edge': trading_edge,
                'Portfolio Basic Sortino': basic_sortino,
                'Portfolio Drawdown Duration (days)': drawdown_duration_days,
                'Portfolio Recovery Factor': recovery_factor
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
    










# def calculate_metrics(self, df, initial_margin, slippage_pct): df = df[df['P/L'].notna()].copy() slippage = df['cv'] * slippage_pct df['P/L'] = df['P/L'] - slippage df['cumsum'] = df['P/L'].cumsum() df['Equity Curve'] = df['cumsum'] + initial_margin days = ((df['timestamp'].iloc[-1].date()) - (df['timestamp'].iloc[0].date())).days end_value = df['Equity Curve'].iloc[-1] start_value = df['Equity Curve'].iloc[0] absolute_return = end_value - start_value absolute_percentage = absolute_return/initial_margin * 100 cagr = ((end_value/start_value) ** (365/days) - 1)*100 df['cummax'] = df['Equity Curve'].cummax() df['drawdown'] = ((df['Equity Curve'] - df['cummax'])/df['cummax']) * 100 max_drawdown = df['drawdown'].min() calmar = cagr/abs(max_drawdown) total_trades = len(df[df['P/L'].notna()]) winners = len(df[df['P/L'] > 0]) losers = len(df[df['P/L'] < 0]) win_percentage = winners/total_trades * 100 loss_percentage = losers/total_trades * 100 profit_factor = df.loc[df['P/L'] > 0, 'P/L'].sum() / abs(df.loc[df['P/L'] < 0, 'P/L'].sum()) payoff_ratio = df.loc[df['P/L'] > 0, 'P/L'].mean() / abs(df.loc[df['P/L'] < 0, 'P/L'].mean()) avg_win = df.loc[df['P/L'] > 0, 'P/L'].mean() avg_loss = abs(df.loc[df['P/L'] < 0, 'P/L'].mean()) avg_win_percentage = avg_win/initial_margin*100 avg_loss_percentage = avg_loss/initial_margin*100 trading_edge = (avg_win_percentage*win_percentage) - (avg_loss_percentage*loss_percentage) downside_deviation = abs(df.loc[df['P/L'] < 0, 'P/L'].std()) basic_sortino = cagr/downside_deviation metrics_dict = { 'absolute_return': absolute_return, 'absolute_percentage': absolute_percentage, 'cagr': cagr, 'mdd': max_drawdown, 'calmar': calmar, 'win_percentage': win_percentage, 'loss_percentage': loss_percentage, 'average_win_percentage': avg_win_percentage, 'average_loss_percentage': avg_loss_percentage, 'profit_factor': profit_factor, 'payoff_ratio': payoff_ratio, 'trading_edge': trading_edge, 'basic_sortino': basic_sortino} return df, metrics_dict In this function, keep everything the same (even if wrong). Just add a metric to measure time taken from drawdown rercovery (from peak to trough to peak) and the recovery factor.
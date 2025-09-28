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
    
    def calculate_metrics(self, df, initial_margin):
        df = df[df['P/L'].notna()].copy()
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
        metrics_dict = { 'absolute_return': absolute_return,
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
                         'basic_sortino': basic_sortino}
        return df, metrics_dict
        









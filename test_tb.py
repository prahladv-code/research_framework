from chakraview.pcco_opt import PCCO

# This function will run in each process
def run_backtest(uid):
    # Instantiate PCCO inside the process
    p = PCCO()
    # Set up and run backtest
    p.create_backtest(uid)
    print(f"Backtest finished for {uid}")

run_backtest('PCCOOPT_nifty_1_0.05_False_0')
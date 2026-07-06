
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from threading import Event
from chakraview import STRATEGY_REGISTRY
import signal
import sys

uids = [
    'DONCHAINDIRECTIONAL_NIFTY_5_45_1_0_0'
]

# Global stop event
STOP_EVENT = mp.Event()


def signal_handler(sig, frame):
    """
    Handles Ctrl+C cleanly
    """
    print("\nKeyboard Interrupt Detected. Stopping all processes gracefully...")

    STOP_EVENT.set()


def run_uid(strategy_class, uid, stop_event):
    """
    Runs single UID inside a thread
    """

    if stop_event.is_set():
        return

    try:

        strategy = strategy_class()

        print(f"Running {uid}")

        strategy.run_backtest(uid)

        if stop_event.is_set():
            print(f"Stopping {uid}")

    except Exception as e:
        print(f"Error in {uid}: {e}")


def strategy_process(strategy_name, uid_list, stop_event):
    """
    One process per strategy
    
    """

    print(f"Started Process: {strategy_name}")

    strategy_class = STRATEGY_REGISTRY[strategy_name]

    try:

        with ThreadPoolExecutor(max_workers=len(uid_list)) as executor:

            futures = []

            for uid in uid_list:

                if stop_event.is_set():
                    break

                futures.append(
                    executor.submit(
                        run_uid,
                        strategy_class,
                        uid,
                        stop_event
                    )
                )

            for future in futures:

                if stop_event.is_set():
                    break

                future.result()

    except KeyboardInterrupt:
        pass

    finally:
        print(f"Finished Process: {strategy_name}")


def group_uids_by_strategy(uids):

    grouped = defaultdict(list)

    for uid in uids:

        strategy_name = uid.split("_")[0]

        grouped[strategy_name].append(uid)

    return grouped


def main():

    # Register Ctrl+C handler
    signal.signal(signal.SIGINT, signal_handler)

    grouped = group_uids_by_strategy(uids)

    processes = []

    try:

        for strategy_name, uid_list in grouped.items():

            if STOP_EVENT.is_set():
                break

            p = mp.Process(
                target=strategy_process,
                args=(strategy_name, uid_list, STOP_EVENT)
            )

            p.start()

            processes.append(p)

        while any(p.is_alive() for p in processes):

            if STOP_EVENT.is_set():

                print("Terminating child processes...")

                for p in processes:

                    if p.is_alive():
                        p.terminate()

                break

            for p in processes:
                p.join(timeout=0.5)

    except KeyboardInterrupt:

        print("\nForce stopping all processes...")

        STOP_EVENT.set()

        for p in processes:

            if p.is_alive():
                p.terminate()

    finally:

        for p in processes:
            p.join()

        print("All processes stopped cleanly.")


if __name__ == "__main__":

    mp.freeze_support()  # IMPORTANT FOR WINDOWS

    main()



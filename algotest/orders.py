import requests
from algotest.auth import AlgoTestClient
from dotenv import load_dotenv
import os

class TradeSignals:
    def __init__(self, main_url: str, order_url: str, token: dict):
        load_dotenv()
        self.main_url = main_url
        self.order_url = order_url
        self.access_token = os.getenv('ACCESS_TOKEN')
        self.broker_id = os.getenv('BROKER_ID')

        self.headers = {
            "Content-Type": "application/json",
            "X-CSRF-TOKEN-ACCESS": token.get('X-CSRF-TOKEN-ACCESS'),
            "Cookie": f"access_token_cookie={token.get('Authorization')}"
        }

    def create_trade_signals(self, payload: dict):
        """Create a new trade signal"""
        response = requests.post(
            f"{self.main_url}/trade-signal/create", 
            json=payload, 
            headers=self.headers
        )

        if response.status_code == 200:
            content = response.json()
            print(f"Signal created successfully: {content}")
            return content.get("id")
        else:
            raise Exception(f"Failed to create signal: {response.status_code}, {response.text}")

    def send_trade_signals(self, tag: str, payload: str, execution_type: str = "paper"):
        """Send a trade signal"""
        if execution_type == "paper":
            url = f"{self.order_url}/webhook/tv/tk-trade?token={self.access_token}&tag={tag}"
        elif execution_type == "live":
            url = f"{self.order_url}/webhook/tv/tk-trade?token={self.access_token}&tag={tag}&brokers={self.broker_id}"

        response = requests.post(url, json=payload, headers=self.headers)

        if response.status_code == 200:
            print(f"Signal sent successfully: {response.json()}")
            return True
        else:
            raise Exception(f"Failed to send signal: {response.status_code}, {response.text}")


if __name__ == '__main__':

    order_url="https://orders.algotest.in"
    main_url="https://api.algotest.in"

    client = AlgoTestClient(
        phone_number="+918369280017",
        main_url="https://api.algotest.in"
    )
    token = client.get_tokens()
    trade = TradeSignals(main_url, order_url, token)

    # Create a paper trading signal
    signal_payload = {
                        "name": "BTCUSD_TEST",
                        "signal_type": "paper",
                        "brokers": []
                    }
    signal_tag = trade.create_trade_signals(signal_payload)

    # Buy order
    buy_order = {"message": "BTCUSD.P buy 10"}
    trade.send_trade_signals(
        tag=signal_tag,
        payload=buy_order,
        execution_type='paper'
    )
    

    # URL FROM EXAMPLES
    # main_url="https://api.algotest.in",
    # order_url="https://orders.algotest.in/webhook/tv/",

    # URL FROM ORDERS PAGE
    # production_orders_api = "https://algotest.in/api/orders"
    # Place_Order_EndPoint = "https://algotest.in/api/orders/place"
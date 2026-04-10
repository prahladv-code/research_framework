import requests
from algotest.auth import AlgoTestClient
import json

class ContractFetcher:
    def __init__(self, token, underlying: str, prices_url: str):
        self.underlying = underlying
        self.contracts_url = f"{prices_url}/contracts?underlying={underlying}"
        self.contracts = None
        self.contract_count = 0

        self.headers = {"Content-Type": "application/json"}
        self.headers.update(token)


    def fetch_contracts(self):
        response = requests.get(self.contracts_url, headers=self.headers)

        if response.status_code == 200:
            self.contracts = response.json()
            self.contract_count = len(self.contracts)
            print(f"Fetched {self.contract_count} contracts for {self.underlying}")
            return self.contracts
        else:
            raise Exception(f"Failed to fetch contracts: {response.status_code}")
           
        


if __name__ == '__main__':
    # Initialize the client
    client = AlgoTestClient(
        phone_number="+918369280017",
        main_url="https://api.algotest.in"
    )


    # Fetch contracts for BTCUSD
    contracts = ContractFetcher(
        token=client.get_tokens(),
        underlying="NIFTY",
        prices_url="https://prices.algotest.in"
    )

    print(f"Available contracts: {contracts.contract_count}")
    METADATA = contracts.fetch_contracts()
    with open('./algotest_contracts_metadata.json', 'w') as f:
        json.dump(METADATA, f, indent=2)


    

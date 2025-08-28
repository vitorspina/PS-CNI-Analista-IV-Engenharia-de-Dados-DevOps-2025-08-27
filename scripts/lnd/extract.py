#importando as libs necessárias para fazer a requisição
import json
import requests
import logging
from datetime import datetime

#definindo o nome do arquivo que log que será salvo na sua respectiva pasta
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"storage/logs/extract_{timestamp}.log"

#configurando o logger
logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s [%(levelname)s] %(message)s",  
    handlers=[
        logging.FileHandler(log_file),   
        logging.StreamHandler()         
    ]
)



#primeira função é de configurar quais são os parametros que serão utilizados para a requisição, a ideia de se criar uma função é tornar futuros desenvolvimentos mais fáceis
def set_request_params(headers = {}, params = {}) -> str:
    return "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
def save_response_as_json(response: requests.Response, save_path :str)  -> bool:
    logging.info(f"saving file {save_path}")
    if response.status_code == 200:
        payload  = response.json()
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)
        return True
    return False


def perform_requests_with_retry(url:str , max_number_of_retrys:int):
    url = str(url)
    logging.info(f"starting request for the url {url}")
    for i in range(max_number_of_retrys):
        
        response = requests.get(url)
        logging.info(f"requests number {i}, for the url: {url} got status code: {response.status_code}")
        if response.status_code == 200:
            save_response_as_json(response,f"storage/lnd/sidra_ibge_{timestamp}.json")
            return True 
    logging.info(f"finished the resquets for the url: {url}")
    return False


def main():
    url = set_request_params()
    perform_requests_with_retry(url,5)
if __name__ == "__main__":
    main()
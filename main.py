import os
from dotenv import load_dotenv
from binance.client import Client
from binance import BinanceSocketManager
import asyncio
import  csv
from csv import writer
from python_settings import settings

process_list = []

async def main():
    """
    Main
    """
    # Loading environment variables
    load_dotenv('.')
    api_key = os.environ.get("api_key")
    api_secret = os.environ.get("api_secret")

    # Loading settings
    os.environ["SETTINGS_MODULE"] = 'settings' 

    # Creating the client
    client = Client(api_key, api_secret)

    # Defining sockets based on settings
    symbols = settings.SYMBOLS

    # Multiprocessing the api requests
    tasks = []
    for s in symbols:
        task = asyncio.create_task(scrape_process(s, client))
        tasks.append(task)

    # Waiting for all the processes to end
    for task in tasks:
        await task

    

async def scrape_process(s, client):
    """
    Gathering data into csv
    """
    bsm = BinanceSocketManager(client)
    socket = bsm.trade_socket(s)
    writeRow(await getData(socket))

async def getData(socket):
    """
    Returns the data received by a socket
    """
    await socket.__aenter__()
    res = await socket.recv()
    print("#########   " + str(res["s"]) + "   #############")
    print(res)
    return(res)

def writeRow(row):
    """
    Appends a dict's values as a row to csv file
    """
    s = []
    for _ , v in row.items():
        s.append(str(v))
    with open("data/" + row["s"] + ".csv", 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(s)

# Running main() through asyncio for async/await
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
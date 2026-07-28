import asyncio
import json
import os 
from datetime import datetime,timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv
from websockets.asyncio.client import connect

ENV_PATH =Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

WS_RPC_URL = os.getenv("WS_RPC_URL")
CHAIN_ID =int(os.getenv("CHAIN_ID","56"))

KAFKA_ADRESS="localhost:9092"
KAFKA_TOPIC = "evm.raw"

async def main():
    if not WS_RPC_URL:
        raise RuntimeError("ws rpc url .env dosyası bulunamadı")

    producer=AIOKafkaProducer(
        bootstrap_servers=KAFKA_ADRESS,
    )
    await producer.start()

    try:
        async with connect(WS_RPC_URL)as websocket:
            subscription_request ={

                "jsonrpc":"2.0",
                "id":1,
                "method":"eth_subscribe",
                "params":["newHeads"],
            }
            await websocket.send(json.dumps(subscription_request))

            subscription_response = json.loads(
                await websocket.recv()
            )

            print ("abonelik oluştu",subscription_response)

            while True:
                websocket_message= json.loads(
                    await websocket.recv()
                )

                block = websocket_message["params"]["result"]


                raw_event ={
                    "schema_version":1,
                    "chain_id":CHAIN_ID,
                    "data_type":"block",
                    "block_number":int(block["number"],16),
                    "block_hash":block["hash"],
                    "parent_hash":block["parentHash"],
                    "received_at":datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "payload":block,



                }



                await producer.send_and_wait(
                    topic=KAFKA_TOPIC,
                    key=str(CHAIN_ID).encode("utf-8"),
                    value=json.dumps(raw_event).encode("utf-8")

                )

                print(
                    f"blok kafkaya gönderildi :"
                    f"{raw_event['block_number']}"
                )
    finally:
        await producer.stop()



if __name__=="__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n durduruldu")
                
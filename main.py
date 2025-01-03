import os
import asyncio
import json
import logging
from datetime import datetime

import aiomysql
import websockets
from dotenv import load_dotenv
import websockets.exceptions

load_dotenv()


WS_SERVER = os.getenv("WS_SERVER")

async def send_data_to_server(data: dict) -> str:
    try:
        async with websockets.connect(WS_SERVER) as ws:
            await ws.send(json.dumps(data))
            response = await ws.recv()
            return response
    except websockets.exceptions.ConnectionClosedOK:
        pass
    except Exception as e:
        logging.error(f"WebSocket error: {e}")


async def process_data(pool) -> None:
    async with pool.acquire() as conn, conn.cursor(aiomysql.DictCursor) as cur:
        try:
            await cur.execute(
                f"""
                SELECT id, carwash_id, post_id, payment_type, value, last_update
                FROM transactions
                WHERE payment_type IN ('cash','paypass','liqpay')
                AND value > 0
                AND id NOT IN (SELECT id FROM fiscalized)
                AND service_id = 14
                """
            )
            result = await cur.fetchall()
            logging.info(f"Fetched rows: {result}")

            if not result:
                logging.info("No new transactions found")
                return

            for row in result:
                now = datetime.now()
                factory_number = "ADW" + str(row["carwash_id"]) + str(row["post_id"])
                response = await send_data_to_server({
                    "factory_number": factory_number,
                    "sales": {
                        "code": row["id"],
                        "payment_type": row["payment_type"],
                        "cash": row["value"],
                        "created_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                })
                if response:
                    logging.info(f"Response for transaction_id={row['id']}: {response}")
                    await cur.execute(
                        """
                        INSERT INTO fiscalized (id, serial, type, value, date, is_handled)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (row["id"], factory_number, row["payment_type"], row["value"], now, True)
                    )
                    await conn.commit()
                else:
                    logging.warning(f"Failed to process transaction_id={row['id']}, will retry later.")
        except Exception as e:
            logging.error(f"Error processing data: {e}")


async def main_loop():
    logging.basicConfig(level=logging.INFO)
    pool = await aiomysql.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        autocommit=True,
    )
    logging.info("Script has started")
    while True:
        try:
            await process_data(pool)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            
        await asyncio.sleep(5)


asyncio.run(main_loop())

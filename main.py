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

INTERVAL_HOURS = os.getenv("INTERVAL_HOURS")
WS_SERVER = os.getenv("WS_SERVER")

async def send_data_to_server(data: dict) -> str:
    try:
        async with websockets.connect(WS_SERVER) as ws:
            await ws.send(json.dumps(data))
            response = await ws.recv()
            return response
    except websockets.exceptions.ConnectionClosedOK:
        pass


async def process_data(conn, cur) -> int:
    await cur.execute(
        f"""
        SELECT id, carwash_id, post_id, payment_type, value
        FROM transactions
        WHERE payment_type IN ('cash', 'paypass', 'liqpay')
          AND value > 0
          AND id NOT IN (SELECT id FROM fiscalized)
          AND service_id = 14
          AND last_update > NOW() - INTERVAL '{INTERVAL_HOURS} HOURS'
        """
    )
    result = await cur.fetchall()

    for row in result:
        now = datetime.now()
        factory_number = "ADW" + str(row["carwash_id"]) + str(row["post_id"])
        
        response = await send_data_to_server({
            "factory_number": factory_number,
            "sales": {
                "code": row["id"],
                row["payment_type"]: row["value"],
                "created_at": now.strftime("%Y-%m-%dT%H:%M:%S")
            }
        })
        logging.info(f"Response for transaction_id={row['id']}: {response}")

        await cur.execute(
            """
            INSERT INTO fiscalized (id, serial, type, value, date, is_handled)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row["id"], factory_number, row["payment_type"], row["value"], now, True)
        )
        await conn.commit()

        code += 1
        await asyncio.sleep(2)

    return code


async def main_loop():
    logging.basicConfig(level=logging.INFO)
    code = 1
    async with await aiomysql.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
    ) as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            while True:
                await process_data(conn, cur)

asyncio.run(main_loop())

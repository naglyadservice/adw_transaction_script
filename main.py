import os
import asyncio
import json
import logging
from datetime import datetime

import aiomysql
import websockets
from dotenv import load_dotenv

load_dotenv()


async def send_data_to_server(data: dict) -> str:
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps(data))
        response = await ws.recv()
        return response


async def process_data(code: int, conn, cur) -> int:
    await cur.execute(
        """
        SELECT id, carwash_id, post_id, payment_type, value
        FROM transactions
        WHERE payment_type IN ('cash', 'paypass')
          AND value > 0
          AND id NOT IN (SELECT id FROM fiscalized)
        """
    )
    result = await cur.fetchall()

    for row in result:
        now = datetime.now()
        response = await send_data_to_server({
            # "factory_number": "ADW" + str(row["carwash_id"]) + str(row["post_id"]),
            "factory_number": "NPF010020",
            "sales": {
                "code": code,
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
            (row["id"], "NPF010020", row["payment_type"], row["value"], now, True)
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
                code = await process_data(code, conn, cur)


asyncio.run(main_loop())

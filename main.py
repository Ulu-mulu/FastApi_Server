from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import json

app = FastAPI()


async def get_connection():
    return await asyncpg.connect(
        user='postgres',
        password='admin',
        database='postgres',
        host='127.0.0.1'
    )


class SearchPartParams(BaseModel):
    mark_name: str = None
    part_name: str = None
    params: dict = None
    price_gte: float = None
    price_lte: float = None
    page: int = 1


@app.post("/search/part/")
async def search_parts(params: SearchPartParams):
    conn = await get_connection()

    query = "SELECT p.*, m.name AS model_name, m.mark_id, mk.name AS mark_name, mk.producer_country_name " \
            "FROM parts_part p " \
            "JOIN parts_model m ON p.model_id = m.id " \
            "JOIN parts_mark mk ON p.mark_id = mk.id " \
            "WHERE p.is_visible = TRUE "

    conditions = []
    if params.mark_name:
        conditions.append(f"mk.name ILIKE '%{params.mark_name}%'")
    if params.part_name:
        conditions.append(f"p.name ILIKE '%{params.part_name}%'")
    if params.price_gte is not None:
        conditions.append(f"p.price >= {params.price_gte}")
    if params.price_lte is not None:
        conditions.append(f"p.price <= {params.price_lte}")

    if params.params:
        for key, value in params.params.items():
            conditions.append(f"p.json_data ->> '{key}' = '{value}'")

    if conditions:
        query += " AND " + " AND ".join(conditions)

    # Пагинация
    offset = (params.page - 1) * 10
    query += f" LIMIT 10 OFFSET {offset};"

    try:
        records = await conn.fetch(query)
        await conn.close()

        # Формирование ответа
        response = {
            "response": [],
            "count": len(records),
            "summ": sum(record['price'] for record in records)
        }

        for record in records:
            response["response"].append({
                "mark": {
                    "id": record['mark_id'],
                    "name": record['mark_name'],
                    "producer_country_name": record['producer_country_name']
                },
                "model": {
                    "id": record['model_id'],
                    "name": record['model_name']
                },
                "name": record['name'],  # Имя запчасти
                "json_data": record['json_data'],  # JSON данные
                "price": record['price']  # Цена
            })

        return response
    except Exception as e:
        await conn.close()
        raise HTTPException(status_code=500, detail=str(e))
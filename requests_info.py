import asyncio
import aiohttp
from more_itertools import chunked

from models import init_orm, Character, Session

MAX_REQUESTS = 10


async def get_people(person_id, http_session):
    response = await http_session.get(f'https://swapi.dev/api/people/{person_id}/')
    json_data = await response.json()
    for key in ['films', "species", "starships", "vehicles", "homeworld"]:
        if not json_data.get(key):
            json_data[key] = ""
            continue
        elif type(json_data.get(key)) is list:
            extra_data = []
            for value in json_data.get(key):
                extra_response = await http_session.get(value)

                coro = await extra_response.json()
                extra_data.append(coro.get("name" if coro.get("name") else "title"))
            json_data[key] = ", ".join(extra_data).strip(", ")
        elif type(json_data.get(key)) is str:
            extra_response = await http_session.get(json_data.get(key))
            coro = await extra_response.json()
            json_data[key] = coro.get("name" if coro.get("name") else "title")

    return json_data


async def insert(response):
    async with Session() as db_session:
        orm_objects = [Character(id=response_object.get("id"),
                                   birth_year=response_object.get("birth_year"),
                                   eye_color=response_object.get("eye_color"),
                                   gender=response_object.get("gender"),
                                   hair_color=response_object.get("hair_color"),
                                   height=response_object.get("height"),
                                   mass=response_object.get("mass"),
                                   name=response_object.get("name"),
                                   skin_color=response_object.get("skin_color"),

                                   homeworld=response_object.get("homeworld"),  # extra requests
                                   films=response_object.get("films"),
                                   species=response_object.get("species"),
                                   starships=response_object.get("starships"),
                                   vehicles=response_object.get("vehicles")
                                   )
                       for response_object in response if response_object.get("name")]
        db_session.add_all(orm_objects)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for people_in_chunk in chunked(range(1, 101), MAX_REQUESTS):
            coros = [get_people(i, http_session) for i in people_in_chunk]

            response = await asyncio.gather(*coros)
            task = asyncio.create_task(insert(response))
    tasks_set = asyncio.all_tasks()
    tasks_set.remove(asyncio.current_task())
    await asyncio.gather(*tasks_set)

if __name__ == "__main__":
    asyncio.run(main())
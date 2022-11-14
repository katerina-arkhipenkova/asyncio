import datetime
import asyncio
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON, String

PG_DSN = 'postgresql+asyncpg://app:secret@127.0.0.1:5432/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Heroes_of_sw(Base):
    __tablename__ = 'heroesofsw'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK_SIZE = 10


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin{people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        json_data['id'] = people_id
    print(f'end{people_id}')
    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 84), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def get_item_list(links_list: list, title: str):
    item_list = []
    if type(links_list) != list:
        links_list = []
    for link in links_list:
        async with ClientSession() as session:
            async with session.get(link) as response:
                json_data = await response.json()
                if json_data.get(title):
                    item_list.append(json_data.get(title))
    item_string = ', '.join(item_list)
    return item_string


async def insert_people(people_chunk):
    async with Session() as session:
         for item in people_chunk:
            id = item.get('id')
            name = item.get('name')
            birth_year = item.get('birth_name')
            eye_color = item.get('eye_color')
            gender = item.get('gender')
            hair_color = item.get('hair_color')
            height = item.get('height')
            homeworld = item.get('homeworld')
            mass = item.get('mass')
            skin_color = item.get('skin_color')
            films = await get_item_list(item.get('films'), 'title')
            species = await get_item_list(item.get('species'), 'name')
            vehicles = await get_item_list(item.get('vehicles'), 'name')
            starships = await get_item_list(item.get('starships'), 'name')
            session.add(Heroes_of_sw(id=id, name=name, birth_year=birth_year, eye_color=eye_color,
                               films=films, gender=gender, hair_color=hair_color,
                               height=height, homeworld=homeworld, mass=mass,
                               skin_color=skin_color, species=species,
                               vehicles=vehicles, starships=starships))
            await session.commit()
            print(f'add hero {id}')


async def main():
    tasks = []
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        print(chunk)
        task = asyncio.create_task(insert_people(chunk))
        tasks.append(task)

    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)

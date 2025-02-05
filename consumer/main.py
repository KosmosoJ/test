from notifications.notifications import consume_data
import asyncio
from aiokafka.errors import KafkaConnectionError

async def main():
    try:
        asyncio.create_task(await consume_data())
    except KafkaConnectionError as ex:
        print(ex)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Выключился')
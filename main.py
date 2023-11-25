from telethon.sync import TelegramClient, events
import time, json
import asyncio, aiofiles
import aiosqlite

last_event = time.time()

async def getData():
    while True:
        try:
            async with aiosqlite.connect('ChatsAndAccounts.db') as conn:
                cursor = await conn.cursor()
                await cursor.execute("SELECT * FROM ChatsAndAccounts")
                rows = await cursor.fetchall()
                return rows
        except:
            await asyncio.sleep(0.5)


async def updateData(id, new_data):
    while True:
        try:
            async with aiosqlite.connect('ChatsAndAccounts.db') as conn:
                cursor = await conn.cursor()
                await cursor.execute("UPDATE ChatsAndAccounts SET data = ? WHERE account_id = ?",
                                     (new_data, id))
                await conn.commit()
                break
        except Exception as error:
            print(error)
            await asyncio.sleep(0.5)
    print(id, 1)


async def runClient(client, client_hash):
    async with client:
        timeLocal = dict()
        rows = await getData()
        for row in rows:
            for _ in row[1].split(","):
                second = _.split(":")
                chat_id = int(second[0])
                timeEvent = float(second[1])
                timeLocal[chat_id] = timeEvent

        async with aiofiles.open('config.json', 'r') as f:
            data = await f.read()
        data = json.loads(data)
        message = data['message']
        answer = data['answer']

        chat_ids = []
        for _ in data['accounts']:
            for i in data['accounts'][_].split(','):
                cur = i.split(':')
                chat_ids.append(cur[0])

        @client.on(events.NewMessage(incoming=True, pattern='(?i).*Hello'))
        async def handler(event):
            if str(event.chat_id) not in chat_ids:
                await event.reply(answer)

        @client.on(events.Raw)
        async def handler(update):
            global last_event
            if time.time() < last_event + 5.:
                return
            else:
                last_event = time.time() + 5.
            print("Event", time.strftime("%H:%M:%S", time.localtime()), last_event, ":", time.time())
            exist = False
            for _ in timeLocal:
                if timeLocal[_] < time.time():
                    exist = True
                    break
            if exist == False:
                return
            rows = await getData()

            for row in rows:
                account = row[0].split(":")
                hash = account[1]
                if hash != client_hash:
                    continue
                new_data = ""
                old_data = row[1]

                for _ in row[1].split(","):
                    print(hash, client_hash)
                    print("---------")
                    print(row)
                    print("----")
                    print(_)
                    print("---------")
                    second = _.split(":")
                    chat_id = int(second[0])
                    timeEvent = float(second[1])
                    period = int(second[2])
                    curTime = time.time()
                    if curTime > timeEvent + 5 and curTime > timeLocal[chat_id] + 5:
                        timeLocal[chat_id] = curTime + period
                        print(curTime, timeLocal[chat_id], timeEvent)
                        await client.send_message(chat_id, message)
                        print("Sent", time.strftime("%H:%M:%S", time.localtime()), chat_id, period)
                        new_data += f"{chat_id}:{time.time() + period}:{period},"
                    else:
                        new_data += f"{_},"
                new_data = new_data[:-1]
                if new_data != old_data:
                    await updateData(row[0], new_data)

        await client.run_until_disconnected()


async def initiate():
    async with aiofiles.open('config.json', 'r') as f:
        data = await f.read()
    data = json.loads(data)

    async with aiosqlite.connect('ChatsAndAccounts.db') as conn:
        cursor = await conn.cursor()

        await cursor.execute('''
                        CREATE TABLE IF NOT EXISTS ChatsAndAccounts (
                            account_id TEXT PRIMARY KEY,
                            data TEXT
                        )
                    ''')
        await conn.commit()

        rows = await getData()

        await cursor.execute("DELETE FROM ChatsAndAccounts")
        await conn.commit()
        ti = dict()
        for row in rows:
            for _ in row[1].split(','):
                obj = _.split(':')
                ti[obj[0]] = obj[1]

        for _ in data['accounts']:
            data_time = ""
            for i in data['accounts'][_].split(','):
                cur = i.split(':')
                t = time.time()
                print(cur[0], t)
                if cur[0] in ti:
                    if float(ti[cur[0]]) > t:
                        t = ti[cur[0]]
                print(t)
                print("//////////")
                data_time += f"{cur[0]}:{float(t)}:{cur[1]},"
            data_time = data_time[:-1]
            await cursor.execute("INSERT INTO ChatsAndAccounts (account_id, data) VALUES (?, ?)",
                                         (_, data_time))

            await conn.commit()


async def main():
    await initiate()
    rows = await getData()
    coroutine = []
    i = 0
    for row in rows:
        account = row[0]
        account = account.split(":")
        id = int(account[0])
        hash = account[1]
        coroutine.append(runClient(TelegramClient(f'user{i}', id, hash), hash))
        i += 1
    await asyncio.gather(*coroutine)


if __name__ == "__main__":
    asyncio.run(main())

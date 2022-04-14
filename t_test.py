import asyncio
import aiohttp
import csv
import json
from queue import PriorityQueue
import time


URL = 'https://klaytn04.fandom.finance/'

headers = {
    'Content-Type': 'application/json',
    # 'x-chain-id': '8217',
}

file = open('t.cvs', 'a+')

field_names = ['blockNumber', 'blockHash', 'timestamp', 'from', 'to', 'value',
               'gas', 'gasPrice', 'txHash', 'txType']

writer = csv.DictWriter(file, fieldnames=field_names)
# writer.writerow({
#     'blockNumber': 'blockNumber',
#     'blockHash': 'blockHash',
#     'timestamp': 'timestamp',
#     'from': 'from',
#     'to': 'to',
#     'value': 'value',
#     'gas': 'gas',
#     'gasPrice': 'gasPrice',
#     'txHash': 'txHash',
#     'txType': 'txType'
# })

q = PriorityQueue()


async def get_each_block(block_num: int, sem):
    async with sem:
        request_param = {"jsonrpc": "2.0",
                        "method": "klay_getBlockByNumber",
                        "params": [hex(block_num), True],
                        "id": 2}

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            while True:
                try:
                    print(block_num)
                    async with session.post(url=URL, data=json.dumps(request_param), headers=headers, timeout=60) as resp:
                        raw = await resp.read()
                        res = json.loads(raw)['result']
                        q.put((int(res['number'], 16), res))
                        break
                except Exception as e:
                    with open('log', 'a+') as log:
                        log.writelines('block num: ' + str(block_num) + '\n' + str(e) + '\n\n')
                        continue


async def parse_data(block_num_start, block_num_stop, sem):
    async with sem:
        last_block_num = block_num_start - 1
        await asyncio.sleep(5)
        while True:
            print('enter')
            res = q.get()
            if res[0] != last_block_num + 1:
                q.put((res[0], res[1]))
                continue

            last_block_num = res[0]
            res = res[1]

            print('writing: ' + str(last_block_num))

            if res['transactions']:
                for tx in res['transactions']:
                    writer.writerow({
                        'blockNumber': int(res['number'], 16),
                        'blockHash': res['hash'],
                        'timestamp': res['timestamp'],
                        'from': tx['from'],
                        'to': tx['to'] if 'to' in tx else 'null',
                        'value': tx['value'] if 'value' in tx else 'null',
                        'gas': tx['gas'],
                        'gasPrice': tx['gasPrice'],
                        'txHash': tx['hash'],
                        'txType': tx['type']
                    })
            else:
                writer.writerow({
                    'blockNumber': int(res['number'], 16),
                    'blockHash': res['hash'],
                    'timestamp': res['timestamp'],
                    'from': 'null',
                    'to': 'null',
                    'value': 'null',
                    'gas': 'null',
                    'gasPrice': 'null',
                    'txHash': 'null',
                    'txType': 'null'
                })

            if last_block_num == block_num_stop-1:
                break



async def get_data(block_num_start):

    sem = asyncio.Semaphore(1005)

    tasks = [asyncio.create_task(parse_data(block_num_start - 1000, block_num_start, sem))]
    for b_n in range(block_num_start, block_num_start + 1000):
        task = asyncio.create_task(get_each_block(b_n, sem))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def init_get_data(block_num_start):

    sem = asyncio.Semaphore(1005)

    tasks = []
    for b_n in range(block_num_start, block_num_start + 1000):
        task = asyncio.create_task(get_each_block(b_n, sem))
        tasks.append(task)
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(init_get_data(70152815))
    for block_num_start in range(70152815 + 1000, 80000000, 1000):
        asyncio.run(get_data(block_num_start))
    print(time.perf_counter() - start)



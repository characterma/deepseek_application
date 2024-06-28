import math
import pandas as pd
import aiohttp
import json
import time
import asyncio
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
from loguru import logger


class AsyncioRequestGPT:
    def __init__(self, TEMPLATE_ID: int, tags: str,
                 API_URL: str = 'http://aiapi.wisers.com/openai-result-service-api/invoke',
                 semaphore_number: int = 20):
        self.TEMPLATE_ID = TEMPLATE_ID
        self.API_URL = API_URL
        self.tags = tags
        self.complete_result = []
        # self.semaphore = asyncio.Semaphore(semaphore_number)

    @retry(wait=wait_random_exponential(min=1, max=10), stop=stop_after_attempt(3))
    async def request_post(self, index: str, data: dict, semaphore, *args, **kwargs):
        template_data = {
            "template_id": self.TEMPLATE_ID,
            "tags": [self.tags],
            "data": data
        }
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.API_URL, json=template_data) as resp:
                    data = await resp.json()
                    if not data["response_json"] is None:
                        self.complete_result.append({"doc_id": index, 'llm_response': data["response_json"]})
                    elif not data["response_text"] is None:
                        self.complete_result.append({"doc_id": index, 'llm_response': data["response_text"]})
                    else:
                        logger.error(f"response result is except， {resp}")
                        
                        
async def main(tools: AsyncioRequestGPT, data: pd.DataFrame):
    semaphore = asyncio.Semaphore(50)
    tasks = [
        asyncio.create_task(tools.request_post(
            index=value.get("doc_id", "")
            , data={
                "doc_id": "",
                "headline": "",
                "content": value.get("content", "")
            }, semaphore=semaphore)) for index, value in data.iterrows()
    ]
    responses = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]


if __name__ == '__main__':
    template_code = 337
    api_url = 'http://aiapi.wisers.com/openai-result-service-api/invoke'
    semaphore_num = 20
    tags = 'tesla_entity'

    tempdict = [
        {"004": {"headline": "【亚某爆料时间】莱西奥-末日机甲皮肤爆料视频来啦！", "content": "老村长:忠诚，美妙的谎言"}},
        {"005": {"headline": "#哈利波特魔法觉醒[超话]#有没有uu跟我一样从taptap下载更新资源结果安装不了的[悲伤]",
                 "content": "这下不得不买了"}},
        {"006": {"headline": "李元芳KPL新皮肤预览，匿光侦查者，9月8日上线",
                 "content": "李元芳这个意外的特效还阔以 我是玩腻了海滩和战令"}}
    ]
    complete_tempdict = []

    for i in range(1000):
        complete_tempdict.extend(tempdict)
    dictResult = {}
    tools = AsyncioRequestGPT(TEMPLATE_ID=template_code, tags=tags, API_URL=api_url, semaphore_number=semaphore_num)
    start_time = time.time()
    # 使用方法1
    done = asyncio.run(main1(tempdict))

    # # 使用方法2
    # for key, value in tempdict.items():
    #     done = asyncio.run(main(key, value))

    end_time = time.time()
    print("执行完成", done)
    print(tools.complete_result)
    print(f'time:{end_time - start_time}')
   


import json
import random
import string
import time
import uuid
import requests

from typing import Dict, List
from joblib import Parallel, delayed


def get_random_uuid_str() -> str:
    return str(uuid.uuid4())


def get_random_payload(probe_id: str) -> Dict:
    def get_random_int() -> int:
        return random.randint(10 ** 2, 10 ** 4)

    def get_random_float() -> float:
        return random.uniform(0, 1) * random.randint(0, 10 ** 3)

    def get_random_str() -> str:
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(random.randint(0, 300)))

    return {
        "probeId": probe_id,
        "eventId": get_random_uuid_str(),
        "messageType": get_random_str(),
        "eventReceivedTime": get_random_int(),
        "eventTransmissionTime": get_random_int(),
        "messageData": [{
            "measureName": get_random_str(),
            "measureCode": get_random_str(),
            "measureUnit": get_random_str(),
            "measureValue": get_random_str(),
            "measureValueDescription": get_random_str(),
            "measureType": get_random_str(),
            "componentReading": get_random_float()
        } for _ in range(6)]
    }


def get_probe_ids(count: int = 10 ** 4) -> List[str]:
    return [get_random_uuid_str() for _ in range(count)]


def persist(probe_id: str):
    payload = json.dumps(get_random_payload(probe_id), indent=4)
    r = requests.put(f"http://localhost:8080/api/mydb/probe/{probe_id}/event/{probe_id}", data=payload,
                      headers={'content-type': 'application/json'})
    print(r.status_code, r.reason)
    if r.status_code in [200, '200']:
        write_count[0] += 1
    return r.status_code


def durability(probe_id: str):
    r = requests.get(f"http://localhost:8080/api/mydb/probe/{probe_id}/latest",
                     headers={'content-type': 'application/json'})
    print(r.status_code, r.reason)
    if r.status_code in [200, '200']:
        read_count[0] += 1
    return r.status_code


if __name__ == '__main__':
    write_count, read_count = [0], [0]
    probe_ids = get_probe_ids(1000)

    # for probe_id in probe_ids:
    #     try:
    #         code = persist(probe_id)
    #         write_count = write_count + 1 if code in [200, '200'] else write_count
    #     except Exception as e:
    #         print(e)

    # for probe_id in probe_ids:
    #     try:
    #         code = durability(probe_id)
    #         read_count = read_count + 1 if code in [200, '200'] else read_count
    #     except Exception as e:
    #         print(e)

    # print(f'write_count: {write_count} & read_count: {read_count}')

    start_w = time.time()
    try:
        Parallel(n_jobs=24, require='sharedmem')(delayed(persist)(probe_id) for probe_id in probe_ids)
    except Exception as e:
        print(e)
    end_w = time.time()

    start_r = time.time()
    try:
        Parallel(n_jobs=24, require='sharedmem')(delayed(durability)(probe_id) for probe_id in probe_ids)
    except Exception as e:
        print(e)
    end_r = time.time()

    print(f'Writing took : {end_w - start_w} secs')
    print(f'Reading took : {end_r - start_r} secs')
    print(f'write_count: {write_count[0]} & read_count: {read_count[0]}')

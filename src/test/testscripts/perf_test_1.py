import random
import string
import uuid
import json
import requests

from typing import Dict


def get_random_payload() -> Dict:
    def get_random_int() -> int:
        return random.randint(10 ** 2, 10 ** 4)

    def get_random_float() -> float:
        return random.uniform(0, 1) * random.randint(0, 10 ** 3)

    def get_random_str() -> str:
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(random.randint(0, 300)))

    def get_random_uuid_str() -> str:
        return str(uuid.uuid4())

    return {
        "probeId": get_random_uuid_str(),
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


if __name__ == '__main__':
    for _ in range(50):
        payload = json.dumps(get_random_payload(), indent=4)
        r = requests.post("http://localhost:8080/api/mydb/persist", data=json.dumps(payload),
                          headers={'content-type': 'application/json'})
        print(r.status_code, r.reason)

import csv
import uuid


def getProbeId() -> str:
    return str(uuid.uuid4())


if __name__ == '__main__':
    probeIds = [getProbeId() for _ in range(1000)]
    with open('/Users/niranjani/code/big-o/mydb-testing/gatling_mydb/src/test/resources/probeIds.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['probeId'])
        for val in probeIds:
            writer.writerow([val])

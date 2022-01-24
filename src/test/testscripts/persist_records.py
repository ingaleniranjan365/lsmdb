payload = {
  "eventId": "7707d6a0-61b5-11ec-9f10-0800200c9a66",
  "messageType": "spaceCartography",
  "eventReceivedTime": 432452543535,
  "eventTransmissionTime": 1640018265951,
  "messageData": [
    {
      "measureName": "Spherical coordinate system - euclidean distance",
      "measureCode": "SCSED",
      "measureUnit": "parsecs",
      "measureValue": 539900000,
      "measureValueDescription": "Euclidean distance from earth",
      "measureType": "Positioning",
      "componentReading": 4.3e+24
    },
    {
      "measureName": "Spherical coordinate system - azimuth angle",
      "measureCode": "SCSEAA",
      "measureUnit": "degrees",
      "measureValue": 170.42,
      "measureValueDescription": "Azimuth angle from earth",
      "measureType": "Positioning",
      "componentReading": 4600
    },
    {
      "measureName": "Spherical coordinate system - polar angle",
      "measureCode": "SCSEPA",
      "measureUnit": "degrees",
      "measureValue": 30.23,
      "measureValueDescription": "Polar/Inclination angle from earth",
      "measureType": "Positioning",
      "componentReading": 5.6e+43
    },
    {
      "measureName": "Localized electromagnetic frequency reading",
      "measureCode": "LER",
      "measureUnit": "hz",
      "measureValue": 300000,
      "measureValueDescription": "Electromagnetic frequency reading",
      "measureType": "Composition",
      "componentReading": 3000000000000000
    },
    {
      "measureName": "Probe lifespan estimate",
      "measureCode": "PLSE",
      "measureUnit": "Years",
      "measureValue": 239000,
      "measureValueDescription": "Number of years left in probe lifespan",
      "measureType": "Probe",
      "componentReading": 6524000
    },
    {
      "measureName": "Probe diagnostic logs",
      "measureCode": "PDL",
      "measureUnit": "Text",
      "measureValue": "some log data from probe",
      "measureValueDescription": "the diagnostic information from the probe",
      "measureType": "Probe",
      "componentReading": 0
    }
  ]
}

probe_id_prefix = 'PRB342224224213'

seg_1_probe_suffixes = ['01', '02', '05', '10', '12', '13', '19', '20']
seg_2_probe_suffixes = ['03', '04', '06', '10', '12', '14', '20', '21']
seg_3_probe_suffixes = ['01', '02', '10', '13', '14', '15', '16', '20']

probe_id_suffixes = []
probe_id_suffixes.extend(seg_1_probe_suffixes)
# probe_id_suffixes.extend(seg_2_probe_suffixes)
# probe_id_suffixes.extend(seg_3_probe_suffixes)

if __name__ == '__main__':
    for suffix in probe_id_suffixes:
        payload["probeId"] = probe_id_prefix + suffix
        import requests
        import json
        r = requests.post("http://localhost:8080/api/mydb/persist", data=json.dumps(payload), headers = {'content-type' : 'application/json'})
        print(r.status_code, r.reason)

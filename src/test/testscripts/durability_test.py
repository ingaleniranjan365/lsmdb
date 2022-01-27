probe_id_prefix = 'PRB342224224213'

seg_1_probe_suffixes = ['01', '02', '05', '10', '12', '13', '19', '20']
seg_2_probe_suffixes = ['03', '04', '06', '10', '12', '14', '20', '21']
seg_3_probe_suffixes = ['01', '02', '10', '13', '14', '15', '16', '20']

probe_id_suffixes = [str(i) for i in range(13000)]
# probe_id_suffixes.extend(seg_1_probe_suffixes)
# probe_id_suffixes.extend(seg_2_probe_suffixes)
# probe_id_suffixes.extend(seg_3_probe_suffixes)


def get_data(suffix: str):
    import requests
    probe_id = probe_id_prefix + suffix
    r = requests.get(f"http://localhost:8080/api/mydb/probe/{probe_id}/latest", headers={'content-type': 'application/json'})
    print(r.status_code, r.reason)
    assert r.status_code == (200 or '200')


if __name__ == '__main__':
    list(map(lambda id: get_data(id), probe_id_suffixes))
    # for suffix in probe_id_suffixes:
    #     probe_id = probe_id_prefix + suffix
    #     import requests
    #     r = requests.get(f"http://localhost:8080/api/mydb/probe/{probe_id}/latest", headers = {'content-type' : 'application/json'})
    #     print(r.status_code, r.reason)

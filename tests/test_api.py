import httpx
import time


client = httpx.Client(base_url="http://localhost:8000")


def test_dataset_lifecycle(cleanup=True):
    response = client.delete("/datasets/asdf")

    dbsname = "/ZH_HToBB_ZToQQ_M125_13TeV_powheg_pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/NANOAODSIM"
    response = client.post("/datasets", params={"name": "asdf", "dbsname": dbsname})
    print(response.text)
    assert response.status_code == 200
    nfiles = response.json()["nfiles"]

    response = client.get("/datasets/asdf")
    assert response.status_code == 200

    response = client.get("/datasets/asdf/files")
    assert response.status_code == 200
    
    retry = 0
    while (len(response.json()) < nfiles) and (retry < 5):
        time.sleep(5)
        response = client.get("/datasets/asdf/files")
        assert response.status_code == 200
        retry += 1

    print(response.json())

    if cleanup:
        response = client.delete("/datasets/asdf")
        assert response.status_code == 200


if __name__ == '__main__':
    # test_dataset_lifecycle(False)
    response = client.get("/datasets/asdf/partitions", params={"columnset_name": "Events"})
    from pprint import pprint
    pprint(response.json())

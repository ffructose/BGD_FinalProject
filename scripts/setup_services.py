import json
import time
from urllib.request import Request, urlopen

TOPICS = ("shop.public.customers", "shop.public.products", "shop.public.orders")
REGISTRY = "http://schema-registry:8081"
CONNECT = "http://connect:8083"


def send_json(url, data, method="POST"):
    request = Request(
        url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def main():
    # waiting untill schema registry will accept registration
    for i in range(30):
        try:
            with urlopen(f"{REGISTRY}/subjects", timeout=5):
                break
        except Exception:
            time.sleep(2)
    else:
        raise RuntimeError("Schema Registry is not ready")

    # registering the same json schema (from task 3)
    with open("schemas/debezium_event.json", encoding="utf-8") as f:
        schema = f.read()

    for topic in TOPICS:
        result = send_json(
            f"{REGISTRY}/subjects/{topic}-value/versions",
            {"schemaType": "JSON", "schema": schema},
        )
        print(f"{topic}-value: schema ID {result['id']}", flush=True)

    # registering or updating existing debezium connector (from task 2)
    with open("debezium/postgres-connector.json", encoding="utf-8") as f:
        connector = json.load(f)

    name = connector["name"]
    send_json(f"{CONNECT}/connectors/{name}/config", connector["config"], "PUT")

    for i in range(30):
        with urlopen(f"{CONNECT}/connectors/{name}/status", timeout=10) as response:
            status = json.load(response)

        tasks = status.get("tasks", [])
        if status["connector"]["state"] == "RUNNING" and tasks and all(
            task["state"] == "RUNNING" for task in tasks
        ):
            print(f"{name}: RUNNING", flush=True)
            return
        if any(task["state"] == "FAILED" for task in tasks):
            raise RuntimeError(str(tasks))
        time.sleep(2)

    raise RuntimeError("Debezium connector did not start")


if __name__ == "__main__":
    main()

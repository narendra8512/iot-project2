"""
Smart Dustbin IoT - Simple version
Sensors: fill level (%), weight (kg), temperature (C)
pip install paho-mqtt matplotlib
"""
import json
import random
import time
import paho.mqtt.client as mqtt
import matplotlib.pyplot as plt

BROKER = "broker.hivemq.com"
TOPIC = "smartbin/myname123/data"      # apna unique naam daal dena

# ---------- Task 2: thresholds (isse bahar = abnormal) ----------
LIMITS = {"fill": 80, "weight": 40, "temp": 50}


# ---------- Task 1: sensor data simulate (25 readings) ----------
def generate_data(n=25):
    data = []
    fill = 5
    for i in range(1, n + 1):
        fill = min(100, fill + random.uniform(2.5, 4))
        weight = fill * 0.4 + random.uniform(-1, 1)
        temp = random.gauss(30, 1.5)
        if i == 15:
            temp = 62            # fire jaisa abnormal reading
        data.append({"seq": i, "fill": round(fill, 1),
                     "weight": round(weight, 1), "temp": round(temp, 1)})
    return data


# ---------- Task 2: average, max, min, abnormal count ----------
def analyse(data):
    print("\nSensor   Avg    Max    Min    Abnormal")
    for s in LIMITS:
        v = [d[s] for d in data]
        bad = [x for x in v if x > LIMITS[s]]
        print(f"{s:<8}{sum(v)/len(v):<7.1f}{max(v):<7}{min(v):<7}{len(bad)}")


# ---------- Task 5: decision, alert, automation, prediction ----------
def decide(d):
    if d["temp"] > LIMITS["temp"]:
        return "FIRE ALERT", "Buzzer ON + Lid LOCK + Fire dept notify"
    if d["fill"] > LIMITS["fill"]:
        return "BIN FULL", "Red LED ON + Send collection truck"
    if d["weight"] > LIMITS["weight"]:
        return "OVERLOAD", "Notify maintenance"
    return "NORMAL", "No action"


def predict(data):
    # average fill rate se batao bin kitni der me 80% hoga
    rate = (data[-1]["fill"] - data[0]["fill"]) / (len(data) - 1)   # % per reading
    if data[-1]["fill"] >= 80:
        return 0
    return (80 - data[-1]["fill"]) / rate * 5      # 5 min har reading


# ---------- Task 3: MQTT publish / subscribe ----------
received = []     # (reading, send_time, receive_time)


def on_connect(client, userdata, flags, rc, props=None):
    client.subscribe(TOPIC, qos=1)


def on_message(client, userdata, msg):
    m = json.loads(msg.payload.decode())
    received.append((m["reading"], m["t"], time.time()))
    status, action = decide(m["reading"])
    if status != "NORMAL":
        print(f"  ALERT seq {m['reading']['seq']}: {status} -> {action}")


def run_mqtt(data):
    try:
        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)   # paho 2.x
    except AttributeError:
        sub = mqtt.Client()                                   # paho 1.x
    sub.on_connect = on_connect
    sub.on_message = on_message
    sub.connect(BROKER, 1883)
    sub.loop_start()
    time.sleep(1)

    try:
        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        pub = mqtt.Client()
    pub.connect(BROKER, 1883)
    pub.loop_start()

    for d in data:
        pub.publish(TOPIC, json.dumps({"reading": d, "t": time.time()}), qos=1)
        time.sleep(0.2)
    time.sleep(2)             # last messages aane do
    pub.loop_stop()
    sub.loop_stop()


# ---------- Performance evaluation ----------
def performance(sent_count):
    lat = [(r - s) * 1000 for _, s, r in received]
    print("\n--- MQTT Performance ---")
    print("Sent:", sent_count, " Received:", len(received))
    print("Packet delivery ratio: %.1f %%" % (100 * len(received) / sent_count))
    if lat:
        print("Latency avg/min/max (ms): %.1f / %.1f / %.1f" % (sum(lat)/len(lat), min(lat), max(lat)))


# ---------- Task 4: graph, abnormal red ----------
def plot(data):
    fig, ax = plt.subplots(3, 1, figsize=(8, 9))
    for a, s in zip(ax, LIMITS):
        x = [d["seq"] for d in data]
        y = [d[s] for d in data]
        a.plot(x, y, "b-o", label=s)
        a.axhline(LIMITS[s], color="r", ls="--", label="limit")
        bx = [d["seq"] for d in data if d[s] > LIMITS[s]]
        by = [d[s] for d in data if d[s] > LIMITS[s]]
        a.scatter(bx, by, color="red", s=100, zorder=5, label="abnormal")
        a.set_ylabel(s)
        a.legend()
    ax[-1].set_xlabel("reading no.")
    plt.tight_layout()
    plt.savefig("graph.png")
    plt.show()


# ---------- main ----------
data = generate_data(25)
print("Readings generated:", len(data))
analyse(data)                       # Task 2
print("\nMQTT chal raha hai...")
run_mqtt(data)                      # Task 3 + 5 (alerts)
performance(len(data))
print("Prediction: bin 80%% hone me ~%.0f minute" % predict(data))
plot(data)                          # Task 4

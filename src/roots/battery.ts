import Mqtt from "async-mqtt";

const main = async () => {
  const mqttPrefix = "battery";
  const haPrefix = "homeassistant/discovery";

  const mqttConn = await Mqtt.connectAsync("tcp://service-mqtt.lan:1883", {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${mqttPrefix}/status`,
    },
  });
  await mqttConn.publish(`${mqttPrefix}/status`, "online", {
    qos: 0,
    retain: true,
  });
  const published: Record<string, Buffer> = {};
  const mqttPublish = async (topic: string, payload: Buffer | string) => {
    const buffer = typeof payload === "string" ? Buffer.from(payload) : payload;

    const publishedPayload = published[topic];
    if (
      publishedPayload !== undefined &&
      Buffer.compare(publishedPayload, buffer) === 0
    ) {
      // log.info(`skipping pub ${topic}`);
      return;
    }

    await mqttConn.publish(topic, buffer, {
      qos: 0,
      retain: true,
    });
    published[topic] = buffer;
    // log.info(`published ${topic}`);
  };

  const mqttState: Record<string, Buffer> = {};
  const onIntervalFn = async () => {
    const amperage = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/amperage"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(2)
    );
    await mqttPublish(`${mqttPrefix}/amperage`, `${amperage}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_amperage/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "current",
        icon: "mdi:current-dc",
        name: `Battery Amperage`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/amperage`,
        unique_id: `battery_amperage`,
        unit_of_measurement: "A",
      })
    );

    const capacity = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/capacity"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(3)
    );
    await mqttPublish(`${mqttPrefix}/capacity`, `${capacity}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_capacity/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "energy",
        icon: "mdi:battery",
        name: `Battery Capacity`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/capacity`,
        unique_id: `battery_capacity`,
        unit_of_measurement: "Wh",
      })
    );

    const charge = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/charge"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(3)
    );
    await mqttPublish(`${mqttPrefix}/charge`, `${charge}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_charge/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "energy",
        icon: "mdi:battery-50",
        name: `Battery Charge`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/charge`,
        unique_id: `battery_charge`,
        unit_of_measurement: "Wh",
      })
    );

    const socs = Object.entries(mqttState).filter(([key, _buffer]) =>
      key.endsWith("/soc")
    );
    const socSum = socs.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const soc = Number((socSum / socs.length).toFixed(3));
    await mqttPublish(`${mqttPrefix}/soc`, `${soc}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_soc/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "battery",
        icon: "mdi:percent",
        name: `Battery SOC`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/soc`,
        unique_id: `battery_soc`,
        unit_of_measurement: "%",
      })
    );

    const timeToEmptys = Object.entries(mqttState).filter(([key, _buffer]) =>
      key.endsWith("/time_to_empty")
    );
    const timeToEmptySum = timeToEmptys.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const timeToEmpty = Number(
      (timeToEmptySum / timeToEmptys.length).toFixed(2)
    );
    await mqttPublish(`${mqttPrefix}/time_to_empty`, `${timeToEmpty}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_time_to_empty/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "duration",
        icon: "mdi:battery-clock-outline",
        name: `Battery Time To Empty`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/time_to_empty`,
        unique_id: `battery_time_to_empty`,
        unit_of_measurement: "h",
      })
    );

    const timeToFulls = Object.entries(mqttState).filter(([key, _buffer]) =>
      key.endsWith("/time_to_full")
    );
    const timeToFullSum = timeToFulls.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const timeToFull = Number((timeToFullSum / timeToFulls.length).toFixed(2));
    await mqttPublish(`${mqttPrefix}/time_to_full`, `${timeToFull}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_time_to_full/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "duration",
        icon: "mdi:battery-clock",
        name: `Battery Time To Full`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/time_to_full`,
        unique_id: `battery_time_to_full`,
        unit_of_measurement: "h",
      })
    );

    const voltages = Object.entries(mqttState).filter(
      ([key, _buffer]) => key.endsWith("/voltage") && !key.includes("/cells/")
    );
    const voltageSum = voltages.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const voltage = Number((voltageSum / voltages.length).toFixed(1));
    await mqttPublish(`${mqttPrefix}/voltage`, `${voltage}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_voltage/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "voltage",
        icon: "mdi:flash-triangle-outline",
        name: `Battery Voltage`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/voltage`,
        unique_id: `battery_voltage`,
        unit_of_measurement: "V",
      })
    );

    const wattage = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/wattage"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(1)
    );
    await mqttPublish(`${mqttPrefix}/wattage`, `${wattage}`);
    await mqttPublish(
      `${haPrefix}/sensor/battery_wattage/config`,
      JSON.stringify({
        device: {
          identifiers: ["battery"],
          name: `Battery`,
        },
        device_class: "current",
        icon: "mdi:current-dc",
        name: `Battery Wattage`,
        state_class: "measurement",
        state_topic: `${mqttPrefix}/wattage`,
        unique_id: `battery_wattage`,
        unit_of_measurement: "W",
      })
    );

    setTimeout(onIntervalFn, 0);
  };
  void onIntervalFn();

  mqttConn.on("message", (topic, payload) => {
    // log.info(`received message on ${topic}`);
    mqttState[topic] = payload;
  });

  mqttConn.subscribe(["renogy/batteries/#"]);
};

main().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

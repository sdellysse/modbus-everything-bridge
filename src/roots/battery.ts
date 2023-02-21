import Mqtt from "async-mqtt";
import winston from "winston";
import { parseEnv } from "znv";
import { z } from "zod";

(async () => {
  const env = parseEnv(process.env, {
    HOME_ASSISTANT_DISCOVERY_PREFIX: z
      .string()
      .default("homeassistant/discovery"),
    LOGLEVEL: z.string().default("info"),
    MQTT_PREFIX: z.string().default("battery"),
    MQTT_URI: z.string().default("tcp://service-mqtt.lan:1883"),
    SOURCES: z
      .string()
      .transform((string) => string.split(":"))
      .default("renogy_batteries/#"),
  });

  const logger = winston.createLogger({
    level: env.LOGLEVEL,
    format: winston.format.simple(),
    transports: [new winston.transports.Console()],
  });
  logger.debug("env", env);

  logger.debug(`connecting to "${env.MQTT_URI}"`);
  const mqttConn = await Mqtt.connectAsync(env.MQTT_URI, {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${env.MQTT_PREFIX}/status`,
    },
  });
  logger.info(`Connected to broker via "${env.MQTT_URI}"`);

  logger.debug(`publishing status`);
  await mqttConn.publish(`${env.MQTT_PREFIX}/status`, "online", {
    qos: 0,
    retain: true,
  });
  logger.info("published online status");

  const published: Record<string, string> = {};
  const mqttPublish = async (topic: string, payload: string) => {
    logger.debug(`publishing "${topic}" with "${payload}"`);
    if (published[topic] === payload) {
      logger.debug(`skipping, dupe`);
      return;
    }

    await mqttConn.publish(topic, payload, {
      qos: 0,
      retain: true,
    });
    logger.info(`Published "${topic}": "${published[topic]}" -> "${payload}"`);
    published[topic] = payload;
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
    await mqttPublish(`${env.MQTT_PREFIX}/amperage`, `${amperage}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_amperage/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "current",
          icon: "mdi:current-dc",
          name: `Battery Amperage`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/amperage`,
          unique_id: `battery_amperage`,
          unit_of_measurement: "A",
        })
      );
    }

    const capacity = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/capacity"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(3)
    );
    await mqttPublish(`${env.MQTT_PREFIX}/capacity`, `${capacity}`);

    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_capacity/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "energy",
          icon: "mdi:battery",
          name: `Battery Capacity`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/capacity`,
          unique_id: `battery_capacity`,
          unit_of_measurement: "Wh",
        })
      );
    }

    const charge = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/charge"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(3)
    );
    await mqttPublish(`${env.MQTT_PREFIX}/charge`, `${charge}`);

    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_charge/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "energy",
          icon: "mdi:battery-50",
          name: `Battery Charge`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/charge`,
          unique_id: `battery_charge`,
          unit_of_measurement: "Wh",
        })
      );
    }

    const socs = Object.entries(mqttState).filter(([key, _buffer]) =>
      key.endsWith("/soc")
    );
    const socSum = socs.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const soc = Number((socSum / socs.length).toFixed(3));
    await mqttPublish(`${env.MQTT_PREFIX}/soc`, `${soc}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_soc/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "battery",
          icon: "mdi:percent",
          name: `Battery SOC`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/soc`,
          unique_id: `battery_soc`,
          unit_of_measurement: "%",
        })
      );
    }

    const voltages = Object.entries(mqttState).filter(
      ([key, _buffer]) => key.endsWith("/voltage") && !key.includes("/cells/")
    );
    const voltageSum = voltages.reduce(
      (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
      0
    );
    const voltage = Number((voltageSum / voltages.length).toFixed(1));
    await mqttPublish(`${env.MQTT_PREFIX}/voltage`, `${voltage}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_voltage/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "voltage",
          icon: "mdi:flash-triangle-outline",
          name: `Battery Voltage`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/voltage`,
          unique_id: `battery_voltage`,
          unit_of_measurement: "V",
        })
      );
    }

    const wattage = Number(
      Object.entries(mqttState)
        .filter(([key, _buffer]) => key.endsWith("/wattage"))
        .reduce(
          (acc, [_key, buffer]) => acc + parseFloat(buffer.toString("utf-8")),
          0
        )
        .toFixed(1)
    );
    await mqttPublish(`${env.MQTT_PREFIX}/wattage`, `${wattage}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_wattage/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "current",
          icon: "mdi:current-dc",
          name: `Battery Wattage`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/wattage`,
          unique_id: `battery_wattage`,
          unit_of_measurement: "W",
        })
      );
    }

    const timeToEmpty = Number(
      (wattage < 0 ? Math.abs(charge / wattage) : 0).toFixed(2)
    );
    await mqttPublish(`${env.MQTT_PREFIX}/time_to_empty`, `${timeToEmpty}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_time_to_empty/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "duration",
          icon: "mdi:battery-clock-outline",
          name: `Battery Time To Empty`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/time_to_empty`,
          unique_id: `battery_time_to_empty`,
          unit_of_measurement: "h",
        })
      );
    }

    const timeToFull = Number(
      (wattage > 0 ? (capacity - charge) / wattage : 0).toFixed(2)
    );
    await mqttPublish(`${env.MQTT_PREFIX}/time_to_full`, `${timeToFull}`);
    if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
      await mqttPublish(
        `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/battery_time_to_full/config`,
        JSON.stringify({
          device: {
            identifiers: ["battery"],
            name: `Battery`,
          },
          device_class: "duration",
          icon: "mdi:battery-clock",
          name: `Battery Time To Full`,
          state_class: "measurement",
          state_topic: `${env.MQTT_PREFIX}/time_to_full`,
          unique_id: `battery_time_to_full`,
          unit_of_measurement: "h",
        })
      );
    }

    setTimeout(onIntervalFn, 0);
  };
  void onIntervalFn();

  mqttConn.on("message", (topic, payload) => {
    mqttState[topic] = payload;
  });

  mqttConn.subscribe(<Array<string>>env.SOURCES);
})().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

import Mqtt from "async-mqtt";
import { asciiFromBuffer, bufferParsersOf } from "../utils";
import { z } from "zod";
import { parseEnv } from "znv";
import winston from "winston";

(async () => {
  const env = parseEnv(process.env, {
    LOGLEVEL: z.string().default("info"),
    HOME_ASSISTANT_DISCOVERY_PREFIX: z.string().optional(),
    MQTT_PREFIX: z.string(),
    MQTT_URI: z.string(),
    SOURCES: z.string().transform((string) => string.split(":")),
  });

  const logger = winston.createLogger({
    level: env.LOGLEVEL,
    format: winston.format.simple(),
    transports: [new winston.transports.Console()],
  });
  logger.debug("env", env);

  logger.debug(`connecting to broker via ${env.MQTT_URI}`);
  const mqttConn = await Mqtt.connectAsync(env.MQTT_URI, {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${env.MQTT_PREFIX}/status`,
    },
  });
  logger.info(`Connected to ${env.MQTT_URI}`);

  logger.debug("publishing status");
  await mqttConn.publish(`${env.MQTT_PREFIX}/status`, "online", {
    qos: 0,
    retain: true,
  });
  logger.debug("status published");

  const published: Record<string, string> = {};
  const publish = async (topic: string, payload: string) => {
    logger.debug("publish", {
      topic,
      payload,
    });

    if (payload === published[topic]) {
      logger.debug(`skipping publish`);
      return;
    }

    logger.info(`publishing "${topic}": "${published[topic]}" -> "${payload}"`);
    await mqttConn.publish(topic, payload, {
      qos: 0,
      retain: true,
    });
    published[topic] = payload;
    logger.debug(`publish complete`);
  };

  const mqttState: Record<string, Buffer> = {};
  const onIntervalFn = async () => {
    logger.debug("onInterval");

    type Server = {
      attributes: {
        // For some reason, a few of my batteries have started reporting as
        // only having 3 sensors but it actually has 4 of them. I dunno why
        // but my solution is to allow `cell_voltage_count` and
        // `cell_temperature_count` to be overridden. I'll still publish the
        // value received by the query to the broker.
        cell_count?: number;

        is_renogy_battery: true;
        serial?: string;
      };
      prefix: string;
      serial: string;
    };
    const servers: Array<Server> = [];

    for (const [topic, payload] of Object.entries(mqttState)) {
      if (/\/servers\/[0-9]+\/attributes\.json$/.test(topic)) {
        const json = JSON.parse(payload.toString("utf-8"));
        if (json["is_renogy_battery"] === true) {
          const attributes = <Server["attributes"]>json;

          const prefix = topic.replace(/\/attributes\.json$/, "");
          let serial: Server["serial"] | undefined;

          if (attributes["serial"] !== undefined) {
            serial = attributes["serial"];
          } else if (
            mqttState[`${prefix}/queries/properties/data`] !== undefined
          ) {
            serial = asciiFromBuffer(
              mqttState[`${prefix}/queries/properties/data`]!,
              {
                startRegister: 5110,
                register: 5110,
                length: 8,
              }
            );
          }

          if (serial !== undefined) {
            servers.push({
              attributes,
              prefix,
              serial,
            });
          }
        }
      }
    }
    logger.debug("servers", servers);

    for (const server of servers) {
      logger.debug("server", server);

      const cellTemperaturesAndVoltagesData =
        mqttState[
          `${server.prefix}/queries/cell_temperatures_and_voltages/data`
        ];
      if (cellTemperaturesAndVoltagesData !== undefined) {
        const { numberAt } = bufferParsersOf(
          cellTemperaturesAndVoltagesData,
          5000
        );

        const cellVoltageCount = numberAt(5000, 1, "unsigned");
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_count`,
          `${cellVoltageCount}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_voltage_count/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              entity_category: "diagnostic",
              name: `Renogy Battery ${server.serial} Cell Voltage Count`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_count`,
              unique_id: `renogy_battery_${server.serial}_cell_voltage_count`,
            })
          );
        }

        const cellVoltages: Array<number> = [];
        for (
          let i = 0;
          i < (server.attributes["cell_count"] ?? cellVoltageCount);
          i++
        ) {
          const register = 5001 + i;

          cellVoltages.push(
            Number((0.1 * numberAt(register, 1, "unsigned")).toFixed(1))
          );
        }
        logger.debug({ cellVoltages });

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_maximum`,
          `${Math.max(...cellVoltages)}`
        );

        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_voltage_maximum/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${server.serial} Cell Voltage Maximum`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_maximum`,
              unique_id: `renogy_battery_${server.serial}_cell_voltage_maximum`,
              unit_of_measurement: "V",
            })
          );
        }

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_minimum`,
          `${Math.min(...cellVoltages)}`
        );

        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_voltage_minimum/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${server.serial} Cell Voltage Minimum`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_minimum`,
              unique_id: `renogy_battery_${server.serial}_cell_voltage_minimum`,
              unit_of_measurement: "V",
            })
          );
        }

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_variance`,
          (Math.max(...cellVoltages) - Math.min(...cellVoltages)).toFixed(1)
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_voltage_variance/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${server.serial} Cell Voltage Variance`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_voltage_variance`,
              unique_id: `renogy_battery_${server.serial}_cell_voltage_variance`,
              unit_of_measurement: "V",
            })
          );
        }

        for (let i = 0; i < cellVoltages.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await publish(
            `${env.MQTT_PREFIX}/${server.serial}/cells/${cellNumber}/voltage`,
            `${cellVoltages[i]!}`
          );
          if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
            await publish(
              `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_${cellNumber}_voltage/config`,
              JSON.stringify({
                device: {
                  identifiers: [server.serial],
                  manufacturer: "Renogy",
                  model: "Battery",
                  name: `Renogy Battery ${server.serial}`,
                },
                device_class: "voltage",
                entity_category: "diagnostic",
                icon: "mdi:flash-triangle-outline",
                name: `Renogy Battery ${server.serial} Cell ${cellNumber} Voltage`,
                state_class: "measurement",
                state_topic: `${env.MQTT_PREFIX}/${server.serial}/cells/${cellNumber}/voltage`,
                unique_id: `renogy_battery_${server.serial}_cell_${cellNumber}_voltage`,
                unit_of_measurement: "V",
              })
            );
          }
        }

        const cellTemperatureCount = numberAt(5000, 1, "unsigned");
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_count`,
          `${cellTemperatureCount}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_temperature_count/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              entity_category: "diagnostic",
              name: `Renogy Battery ${server.serial} Cell Temperature Count`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_count`,
              unique_id: `renogy_battery_${server.serial}_cell_temperature_count`,
            })
          );
        }

        const cellTemperatures: Array<number> = [];
        for (
          let i = 0;
          i < (server.attributes["cell_count"] ?? cellVoltageCount);
          i++
        ) {
          const register = 5018 + i;

          cellTemperatures.push(
            Number((0.1 * numberAt(register, 1, "unsigned")).toFixed(1))
          );
        }
        logger.debug({ cellTemperatures });

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_maximum`,
          `${Math.max(...cellTemperatures)}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_temperature_maximum/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-chevron-up",
              name: `Renogy Battery ${server.serial} Cell Temperature Maximum`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_maximum`,
              unique_id: `renogy_battery_${server.serial}_cell_temperature_maximum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_minimum`,
          `${Math.min(...cellTemperatures)}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_temperature_minimum/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-chevron-down",
              name: `Renogy Battery ${server.serial} Cell Temperature Minimum`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_minimum`,
              unique_id: `renogy_battery_${server.serial}_cell_temperature_minimum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_variance`,
          (
            Math.max(...cellTemperatures) - Math.min(...cellTemperatures)
          ).toFixed(1)
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_temperature_Variance/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-minus",
              name: `Renogy Battery ${server.serial} Cell Temperature Minimum`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/cell_temperature_Variance`,
              unique_id: `renogy_battery_${server.serial}_cell_temperature_Variance`,
              unit_of_measurement: "°C",
            })
          );
        }

        for (let i = 0; i < cellTemperatures.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await publish(
            `${env.MQTT_PREFIX}/${server.serial}/cells/${cellNumber}/temperature`,
            `${cellTemperatures[i]!}`
          );
          if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
            await publish(
              `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_cell_${cellNumber}_temperature/config`,
              JSON.stringify({
                device: {
                  identifiers: [server.serial],
                  manufacturer: "Renogy",
                  model: "Battery",
                  name: `Renogy Battery ${server.serial}`,
                },
                device_class: "voltage",
                entity_category: "diagnostic",
                icon: "mdi:thermometer",
                name: `Renogy Battery ${server.serial} Cell ${cellNumber} Temperature`,
                state_class: "measurement",
                state_topic: `${env.MQTT_PREFIX}/${server.serial}/cells/${cellNumber}/temperature`,
                unique_id: `renogy_battery_${server.serial}_cell_${cellNumber}_temperature`,
                unit_of_measurement: "°C",
              })
            );
          }
        }
      }

      const countsData = mqttState[`${server.prefix}/queries/counts/data`];
      if (countsData !== undefined) {
        const { numberAt } = bufferParsersOf(countsData, 5035);

        const amperage = Number(
          (0.01 * numberAt(5042, 1, "signed")).toFixed(2)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/amperage`,
          `${amperage}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_amperage/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "current",
              icon: "mdi:current-dc",
              name: `Renogy Battery ${server.serial} Amperage`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/amperage`,
              unique_id: `renogy_battery_${server.serial}_amperage`,
              unit_of_measurement: "A",
            })
          );
        }

        const voltage = Number(
          (0.1 * numberAt(5043, 1, "unsigned")).toFixed(1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/voltage`,
          `${voltage}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_voltage/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "voltage",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${server.serial} Voltage`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/voltage`,
              unique_id: `renogy_battery_${server.serial}_voltage`,
              unit_of_measurement: "V",
            })
          );
        }

        const wattage = Number((amperage * voltage).toFixed(1));
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/wattage`,
          `${wattage}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_wattage/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "current",
              icon: "mdi:current-dc",
              name: `Renogy Battery ${server.serial} Wattage`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/wattage`,
              unique_id: `renogy_battery_${server.serial}_wattage`,
              unit_of_measurement: "W",
            })
          );
        }

        const chargeAh = Number(
          (0.001 * numberAt(5044, 2, "unsigned")).toFixed(3)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/charge`,
          (chargeAh * voltage).toFixed(1)
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_charge/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "energy",
              icon: "mdi:battery-50",
              name: `Renogy Battery ${server.serial} Charge`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/charge`,
              unique_id: `renogy_battery_${server.serial}_charge`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const capacityAh = Number(
          (0.001 * numberAt(5046, 2, "unsigned")).toFixed(3)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/capacity`,
          (capacityAh * voltage).toFixed(1)
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_capacity/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "energy",
              icon: "mdi:battery",
              name: `Renogy Battery ${server.serial} Capacity`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/capacity`,
              unique_id: `renogy_battery_${server.serial}_capacity`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const soc = Number((100.0 * (chargeAh / capacityAh)).toFixed(3));
        await publish(`${env.MQTT_PREFIX}/${server.serial}/soc`, `${soc}`);
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_soc/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "battery",
              icon: "mdi:percent",
              name: `Renogy Battery ${server.serial} SOC`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/soc`,
              unique_id: `renogy_battery_${server.serial}_soc`,
              unit_of_measurement: "%",
            })
          );
        }

        const timeToFull = Number(
          (amperage > 0 ? (capacityAh - chargeAh) / amperage : 0).toFixed(2)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/time_to_full`,
          `${timeToFull}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_time_to_full/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "duration",
              icon: "mdi:battery-clock",
              name: `Renogy Battery ${server.serial} Time To Full`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/time_to_full`,
              unique_id: `renogy_battery_${server.serial}_time_to_full`,
              unit_of_measurement: "h",
            })
          );
        }

        const timeToEmpty = Number(
          (amperage < 0 ? Math.abs(chargeAh / amperage) : 0).toFixed(2)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/time_to_empty`,
          `${timeToEmpty}`
        );
        if (env.HOME_ASSISTANT_DISCOVERY_PREFIX !== undefined) {
          await publish(
            `${env.HOME_ASSISTANT_DISCOVERY_PREFIX}/sensor/renogy_battery_${server.serial}_time_to_empty/config`,
            JSON.stringify({
              device: {
                identifiers: [server.serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${server.serial}`,
              },
              device_class: "duration",
              icon: "mdi:battery-clock-outline",
              name: `Renogy Battery ${server.serial} Time To Empty`,
              state_class: "measurement",
              state_topic: `${env.MQTT_PREFIX}/${server.serial}/time_to_empty`,
              unique_id: `renogy_battery_${server.serial}_time_to_empty`,
              unit_of_measurement: "h",
            })
          );
        }

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/cycle`,
          `${numberAt(5048, 1, "unsigned")}`
        );
      }

      const limitsData = mqttState[`${server.prefix}/queries/limits/data`];
      if (limitsData !== undefined) {
        const { numberAt } = bufferParsersOf(limitsData, 5049);

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/charge_voltage_limit`,
          (0.1 * numberAt(5049, 1, "unsigned")).toFixed(1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/discharge_voltage_limit`,
          (0.1 * numberAt(5050, 1, "unsigned")).toFixed(1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/charge_amerpage_limit`,
          (0.01 * numberAt(5051, 1, "signed")).toFixed(1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/discharge_amperage_limit`,
          (0.01 * numberAt(5052, 1, "signed")).toFixed(1)
        );
      }

      const statusData = mqttState[`${server.prefix}/queries/status/data`];
      if (statusData !== undefined) {
        const { numberAt } = bufferParsersOf(statusData, 5100);

        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/alarminfo_cell_voltage`,
          `${numberAt(5100, 2, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/alarminfo_cell_temperature`,
          `${numberAt(5102, 2, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/alarminfo_other`,
          `${numberAt(5104, 2, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/status1`,
          `${numberAt(5106, 1, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/status2`,
          `${numberAt(5107, 1, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/status3`,
          `${numberAt(5108, 1, "unsigned")}`
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/status_charge_discharge`,
          `${numberAt(5109, 1, "unsigned")}`
        );
      }

      const propertiesData =
        mqttState[`${server.prefix}/queries/propertis/data`];
      if (propertiesData !== undefined) {
        const { asciiAt } = bufferParsersOf(propertiesData, 5110);
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/serial`,
          asciiAt(5110, 8)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/manufacturer_version`,
          asciiAt(5118, 1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/mainline_version`,
          asciiAt(5119, 2)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/communication_protocol_version`,
          asciiAt(5121, 1)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/model`,
          asciiAt(5122, 8)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/software_version`,
          asciiAt(5130, 2)
        );
        await publish(
          `${env.MQTT_PREFIX}/${server.serial}/manufacturer_name`,
          asciiAt(5132, 10)
        );
      }
    }

    setTimeout(onIntervalFn, 0);
  };
  void onIntervalFn();

  mqttConn.on("message", (topic, payload) => {
    // log.info(`received message on ${topic}`);
    mqttState[topic] = payload;
  });

  mqttConn.subscribe(<Array<string>>env.SOURCES);
})().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

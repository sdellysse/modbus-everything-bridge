import Mqtt from "async-mqtt";
import { bufferParsersOf, log } from "../utils";
import { z } from "zod";
import util from "node:util";
import fs from "node:fs/promises";

const main = async () => {
  const argsSchema = z.object({
    values: z.object({
      config: z.string().default(`${__dirname}/../etc/renogy.json`),
    }),
    positionals: z.array(z.never()).length(0),
  });

  const args = argsSchema.parse(
    util.parseArgs({
      options: {
        config: {
          short: "c",
          type: "string",
        },
      },
    })
  );

  const configSchema = z.object({
    mqtt: z.object({
      prefix: z.string(),
      server: z.string(),
      source: z.object({
        prefixes: z.array(z.string()),
      }),
      homeAssistant: z.object({
        discoveryPrefix: z.string(),
      }),
    }),
  });

  const config = configSchema.parse(
    JSON.parse(await fs.readFile(args.values.config, { encoding: "utf-8" }))
  );

  const mqttConn = await Mqtt.connectAsync(config.mqtt.server, {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${config.mqtt.prefix}/status`,
    },
  });
  await mqttConn.publish(`${config.mqtt.prefix}/status`, "online", {
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

  const serialCache: Record<string, string> = {};

  const mqttState: Record<string, Buffer> = {};
  const onIntervalFn = async () => {
    const serverPrefixes: Array<string> = [];
    for (const [topic, payload] of Object.entries(mqttState)) {
      if (/\/servers\/[0-9]+\/attributes\.json$/.test(topic)) {
        const serverPrefix = topic.replace(/\/attributes\.json$/, "");

        const json = JSON.parse(payload.toString("utf-8"));
        if (json["renogy_battery"] === true) {
          serverPrefixes.push(serverPrefix);
        }
      }
    }

    for (const serverPrefix of serverPrefixes) {
      if (serialCache[serverPrefix] === undefined) {
        const serialAttribute = JSON.parse(
          mqttState[`${serverPrefix}/attributes.json`]?.toString("utf-8") ?? ""
        )?.["serial"];
        const queryData =
          mqttState[`${serverPrefix}/queries/register_5100_length_42/data`];

        if (false) {
        } else if (typeof serialAttribute === "string") {
          serialCache[serverPrefix] = serialAttribute;
        } else if (queryData !== undefined) {
          serialCache[serverPrefix] = bufferParsersOf(queryData, 5100).asciiAt(
            5110,
            8
          );
        }

        if (serialCache[serverPrefix] !== undefined) {
          console.log(
            `associating prefix "${serverPrefix}" with serial "${serialCache[serverPrefix]}"`
          );
        }
      }

      const serial = serialCache[serverPrefix];
      if (serial === undefined) {
        continue;
      }

      const block5000To5034 =
        mqttState[`${serverPrefix}/queries/register_5000_length_34/data`];
      if (block5000To5034 !== undefined) {
        const { numberAt } = bufferParsersOf(block5000To5034, 5000);

        const cellVoltages = [
          Number((0.1 * numberAt(5001, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5002, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5003, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5004, 1, "unsigned")).toFixed(1)),
        ];

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_maximum`,
          `${Math.max(...cellVoltages)}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_maximum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_maximum/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${serial} Cell Voltage Maximum`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_maximum`,
              unique_id: `renogy_battery_${serial}_cell_voltage_maximum`,
              unit_of_measurement: "V",
            })
          );
        }

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_minimum`,
          `${Math.min(...cellVoltages)}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_minimum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_minimum/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${serial} Cell Voltage Minimum`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_minimum`,
              unique_id: `renogy_battery_${serial}_cell_voltage_minimum`,
              unit_of_measurement: "V",
            })
          );
        }

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_variance`,
          (Math.max(...cellVoltages) - Math.min(...cellVoltages)).toFixed(1)
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_variance/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_voltage_variance/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "voltage",
              entity_category: "diagnostic",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${serial} Cell Voltage Variance`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_variance`,
              unique_id: `renogy_battery_${serial}_cell_voltage_variance`,
              unit_of_measurement: "V",
            })
          );
        }

        for (let i = 0; i < cellVoltages.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/voltage`,
            `${cellVoltages[i]!}`
          );
          if (
            published[
              `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_voltage/config`
            ] === undefined
          ) {
            await mqttPublish(
              `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_voltage/config`,
              JSON.stringify({
                device: {
                  identifiers: [serial],
                  manufacturer: "Renogy",
                  model: "Battery",
                  name: `Renogy Battery ${serial}`,
                },
                device_class: "voltage",
                entity_category: "diagnostic",
                icon: "mdi:flash-triangle-outline",
                name: `Renogy Battery ${serial} Cell ${cellNumber} Voltage`,
                state_class: "measurement",
                state_topic: `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/voltage`,
                unique_id: `renogy_battery_${serial}_cell_${cellNumber}_voltage`,
                unit_of_measurement: "V",
              })
            );
          }
        }

        const cellTemperatures = [
          Number((0.1 * numberAt(5018, 1, "signed")).toFixed(1)),
          Number((0.1 * numberAt(5019, 1, "signed")).toFixed(1)),
          Number((0.1 * numberAt(5020, 1, "signed")).toFixed(1)),
          Number((0.1 * numberAt(5021, 1, "signed")).toFixed(1)),
        ];

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_maximum`,
          `${Math.max(...cellTemperatures)}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_maximum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_maximum/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-chevron-up",
              name: `Renogy Battery ${serial} Cell Temperature Maximum`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_maximum`,
              unique_id: `renogy_battery_${serial}_cell_temperature_maximum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_minimum`,
          `${Math.min(...cellTemperatures)}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_minimum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_minimum/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-chevron-down",
              name: `Renogy Battery ${serial} Cell Temperature Minimum`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_minimum`,
              unique_id: `renogy_battery_${serial}_cell_temperature_minimum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_variance`,
          (
            Math.max(...cellTemperatures) - Math.min(...cellTemperatures)
          ).toFixed(1)
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_Variance/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_temperature_Variance/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "temperature",
              entity_category: "diagnostic",
              icon: "mdi:thermometer-minus",
              name: `Renogy Battery ${serial} Cell Temperature Minimum`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_Variance`,
              unique_id: `renogy_battery_${serial}_cell_temperature_Variance`,
              unit_of_measurement: "°C",
            })
          );
        }

        for (let i = 0; i < cellTemperatures.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/temperature`,
            `${cellTemperatures[i]!}`
          );
          if (
            published[
              `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_temperature/config`
            ] === undefined
          ) {
            await mqttPublish(
              `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_temperature/config`,
              JSON.stringify({
                device: {
                  identifiers: [serial],
                  manufacturer: "Renogy",
                  model: "Battery",
                  name: `Renogy Battery ${serial}`,
                },
                device_class: "voltage",
                entity_category: "diagnostic",
                icon: "mdi:thermometer",
                name: `Renogy Battery ${serial} Cell ${cellNumber} Temperature`,
                state_class: "measurement",
                state_topic: `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/temperature`,
                unique_id: `renogy_battery_${serial}_cell_${cellNumber}_temperature`,
                unit_of_measurement: "°C",
              })
            );
          }
        }
      }

      const block5035To5053 =
        mqttState[`${serverPrefix}/queries/register_5035_length_18/data`];
      if (block5035To5053 !== undefined) {
        const { numberAt } = bufferParsersOf(block5035To5053, 5035);

        const amperage = Number(
          (0.01 * numberAt(5042, 1, "signed")).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/amperage`,
          `${amperage}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_amperage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_amperage/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "current",
              icon: "mdi:current-dc",
              name: `Renogy Battery ${serial} Amperage`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/amperage`,
              unique_id: `renogy_battery_${serial}_amperage`,
              unit_of_measurement: "A",
            })
          );
        }

        const voltage = Number(
          (0.1 * numberAt(5043, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/voltage`,
          `${voltage}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_voltage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_voltage/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "voltage",
              icon: "mdi:flash-triangle-outline",
              name: `Renogy Battery ${serial} Voltage`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/voltage`,
              unique_id: `renogy_battery_${serial}_voltage`,
              unit_of_measurement: "V",
            })
          );
        }

        const wattage = Number((amperage * voltage).toFixed(1));
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/wattage`,
          `${wattage}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_wattage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_wattage/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "current",
              icon: "mdi:current-dc",
              name: `Renogy Battery ${serial} Wattage`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/wattage`,
              unique_id: `renogy_battery_${serial}_wattage`,
              unit_of_measurement: "W",
            })
          );
        }

        const chargeAh = Number(
          (0.001 * numberAt(5044, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/charge`,
          (chargeAh * voltage).toFixed(1)
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_charge/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_charge/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "energy",
              icon: "mdi:battery-50",
              name: `Renogy Battery ${serial} Charge`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/charge`,
              unique_id: `renogy_battery_${serial}_charge`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const capacityAh = Number(
          (0.001 * numberAt(5046, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/capacity`,
          (capacityAh * voltage).toFixed(1)
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_capacity/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_capacity/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "energy",
              icon: "mdi:battery",
              name: `Renogy Battery ${serial} Capacity`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/capacity`,
              unique_id: `renogy_battery_${serial}_capacity`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const soc = Number((100.0 * (chargeAh / capacityAh)).toFixed(3));
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/soc`,
          `${soc}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_soc/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_soc/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "battery",
              icon: "mdi:percent",
              name: `Renogy Battery ${serial} SOC`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/soc`,
              unique_id: `renogy_battery_${serial}_soc`,
              unit_of_measurement: "%",
            })
          );
        }

        const timeToFull = Number(
          (amperage > 0 ? (capacityAh - chargeAh) / amperage : 0).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/time_to_full`,
          `${timeToFull}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_time_to_full/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_time_to_full/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "duration",
              icon: "mdi:battery-clock",
              name: `Renogy Battery ${serial} Time To Full`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/time_to_full`,
              unique_id: `renogy_battery_${serial}_time_to_full`,
              unit_of_measurement: "h",
            })
          );
        }

        const timeToEmpty = Number(
          (amperage < 0 ? Math.abs(chargeAh / amperage) : 0).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/time_to_empty`,
          `${timeToEmpty}`
        );
        if (
          published[
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_time_to_empty/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${config.mqtt.homeAssistant.discoveryPrefix}/sensor/renogy_battery_${serial}_time_to_empty/config`,
            JSON.stringify({
              device: {
                identifiers: [serial],
                manufacturer: "Renogy",
                model: "Battery",
                name: `Renogy Battery ${serial}`,
              },
              device_class: "duration",
              icon: "mdi:battery-clock-outline",
              name: `Renogy Battery ${serial} Time To Empty`,
              state_class: "measurement",
              state_topic: `${config.mqtt.prefix}/batteries/${serial}/time_to_empty`,
              unique_id: `renogy_battery_${serial}_time_to_empty`,
              unit_of_measurement: "h",
            })
          );
        }

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cycle`,
          `${numberAt(5048, 1, "unsigned")}`
        );

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/charge_voltage_limit`,
          (0.1 * numberAt(5049, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/discharge_voltage_limit`,
          (0.1 * numberAt(5050, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/charge_amerpage_limit`,
          (0.01 * numberAt(5051, 1, "signed")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/discharge_amperage_limit`,
          (0.01 * numberAt(5052, 1, "signed")).toFixed(1)
        );
      }

      const block5100To5142 =
        mqttState[`${serverPrefix}/queries/register_5100_length_42/data`];
      if (block5100To5142 !== undefined) {
        const { asciiAt, numberAt } = bufferParsersOf(block5100To5142, 5100);

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/alarminfo_cell_voltage`,
          `${numberAt(5100, 2, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/alarminfo_cell_temperature`,
          `${numberAt(5102, 2, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/alarminfo_other`,
          `${numberAt(5104, 2, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/status1`,
          `${numberAt(5106, 1, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/status2`,
          `${numberAt(5107, 1, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/status3`,
          `${numberAt(5108, 1, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/status_charge_discharge`,
          `${numberAt(5109, 1, "unsigned")}`
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/serial`,
          serial
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/manufacturer_version`,
          asciiAt(5118, 1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/mainline_version`,
          asciiAt(5119, 2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/communication_protocol_version`,
          asciiAt(5121, 1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/model`,
          asciiAt(5122, 8)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/software_version`,
          asciiAt(5130, 2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/manufacturer_name`,
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

  const subscribeTopics = config.mqtt.source.prefixes.map(
    (prefix) => `${prefix}/#`
  );
  console.log(JSON.stringify({ subscribeTopics }, undefined, 4));
  mqttConn.subscribe(subscribeTopics);
};

main().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

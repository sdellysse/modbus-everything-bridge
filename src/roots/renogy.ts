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
    const batteryPrefixes: Array<string> = [];
    const sccPrefixes: Array<string> = [];
    for (const [topic, payload] of Object.entries(mqttState)) {
      if (/\/servers\/[0-9]+\/attributes\.json$/.test(topic)) {
        const serverPrefix = topic.replace(/\/attributes\.json$/, "");

        const json = JSON.parse(payload.toString("utf-8"));
        if (json["renogy_battery"] === true) {
          batteryPrefixes.push(serverPrefix);
        }
        if (json["renogy_scc"] === true) {
          sccPrefixes.push(serverPrefix);
        }
      }
    }

    for (const sccPrefix of sccPrefixes) {
      if (serialCache[sccPrefix] === undefined) {
        const queryData =
          mqttState[`${sccPrefix}/queries/register_10_length_22/data`];

        if (false) {
        } else if (queryData !== undefined) {
          serialCache[sccPrefix] = `${bufferParsersOf(queryData, 10).numberAt(
            24,
            2,
            "unsigned"
          )}`;
        }

        if (serialCache[sccPrefix] !== undefined) {
          console.log(
            `associating prefix "${sccPrefix}" with serial "${serialCache[sccPrefix]}"`
          );
        }
      }

      const serial = serialCache[sccPrefix];
      if (serial === undefined) {
        continue;
      }

      const block00010UpTo00032 =
        mqttState[`${sccPrefix}/queries/register_10_length_22/data`];
      if (block00010UpTo00032 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt, asciiAt } = bufferParsersOf(
          block00010UpTo00032,
          10
        );

        const systemVoltage = upperByteAt(10, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/system_voltage`,
          `${systemVoltage}`
        );

        const ratedChargeAmperage = lowerByteAt(10, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/rated_charge_amperage`,
          `${ratedChargeAmperage}`
        );

        const ratedDischargeAmperage = upperByteAt(11, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/rated_discharge_amperage`,
          `${ratedDischargeAmperage}`
        );

        const productTypeLookup: Record<number, string> = {
          0: "controller",
        };
        const productTypeCode = lowerByteAt(11, "unsigned");
        const productType =
          productTypeLookup[productTypeCode] ?? `code:${productTypeCode}`;
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/product_type`,
          `${productType}`
        );

        const model = asciiAt(12, 8);
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/model`,
          `${model}`
        );

        const softwareVersion = numberAt(20, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/software_version`,
          `${softwareVersion}`
        );

        const hardwareVersion = numberAt(22, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/hardwareVersion`,
          `${hardwareVersion}`
        );

        const serialNumber = numberAt(24, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/serial`,
          `${serialNumber}`
        );

        const address = numberAt(26, 1, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/address`,
          `${address}`
        );

        const protocolVersion = numberAt(27, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/protocol_version`,
          `${protocolVersion}`
        );

        const uniqueIdCode = numberAt(29, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/unique_id_code`,
          `${uniqueIdCode}`
        );
      }

      const block00256UpTo00266 =
        mqttState[`${sccPrefix}/queries/register_256_length_10/data`];
      if (block00256UpTo00266 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt } = bufferParsersOf(
          block00256UpTo00266,
          256
        );

        const batterySoc = numberAt(256, 1, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/battery_soc`,
          `${batterySoc}`
        );

        const batteryVoltage = Number(
          (0.1 * numberAt(257, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/battery_voltage`,
          `${batteryVoltage}`
        );

        const chargingAmperage = Number(
          (0.01 * numberAt(258, 1, "unsigned")).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/charging_amperage`,
          `${chargingAmperage}`
        );

        const controllerTemperature = upperByteAt(259, "signed");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/controller_temperature`,
          `${controllerTemperature}`
        );

        const batteryTemperature = lowerByteAt(259, "signed");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/battery_temperature`,
          `${batteryTemperature}`
        );

        const loadVoltage = Number(
          (0.1 * numberAt(260, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/load_voltage`,
          `${loadVoltage}`
        );

        const loadAmperage = Number(
          (0.01 * numberAt(261, 1, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/load_amperage`,
          `${loadAmperage}`
        );

        const loadWattage = numberAt(262, 1, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/load_wattage`,
          `${loadWattage}`
        );

        const solarVoltage = Number(
          (0.1 * numberAt(263, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/solar_voltage`,
          `${solarVoltage}`
        );

        const solarAmperage = Number(
          (0.01 * numberAt(264, 1, "unsigned")).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/solar_amperage`,
          `${solarAmperage}`
        );

        const solarWattage = numberAt(265, 1, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/solar_wattage`,
          `${solarWattage}`
        );
      }

      const block00266upTo00288 =
        mqttState[`${sccPrefix}/queries/register_266_length_22/data`];
      if (block00266upTo00288 !== undefined) {
      }

      const block00288UpTo00292 =
        mqttState[`${sccPrefix}/queries/register_288_length_4/data`];
      if (block00288UpTo00292 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt } = bufferParsersOf(
          block00288UpTo00292,
          288
        );

        const streetLightBrightness = -1 * upperByteAt(288, "signed");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/street_light_brightness`,
          `${streetLightBrightness}`
        );

        const chargingStatusLookup: Record<number, string> = {
          0: "charging not on",
          1: "start charging",
          2: "mppt charging mode",
          3: "equalization",
          4: "boost charging",
          5: "float charge",
          6: "current limit flow (max power)",
        };
        const chargingStatusCode = lowerByteAt(288, "unsigned");
        const chargingStatus =
          chargingStatusLookup[chargingStatusCode] ??
          `code: ${chargingStatusCode}`;
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/charging_status`,
          `${chargingStatus}`
        );

        const alarm = numberAt(289, 2, "unsigned");
        await mqttPublish(
          `${config.mqtt.prefix}/solar_charge_controllers/${serial}/alarm`,
          `${alarm}`
        );
      }

      const block57345UpTo57360 =
        mqttState[`${sccPrefix}/queries/register_57345_length_15/data`];
      if (block57345UpTo57360 !== undefined) {
      }
    }

    for (const batteryPrefix of batteryPrefixes) {
      if (serialCache[batteryPrefix] === undefined) {
        const serialAttribute = JSON.parse(
          mqttState[`${batteryPrefix}/attributes.json`]?.toString("utf-8") ?? ""
        )?.["serial"];
        const queryData =
          mqttState[`${batteryPrefix}/queries/register_5100_length_42/data`];

        if (false) {
        } else if (typeof serialAttribute === "string") {
          serialCache[batteryPrefix] = serialAttribute;
        } else if (queryData !== undefined) {
          serialCache[batteryPrefix] = bufferParsersOf(queryData, 5100).asciiAt(
            5110,
            8
          );
        }

        if (serialCache[batteryPrefix] !== undefined) {
          console.log(
            `associating prefix "${batteryPrefix}" with serial "${serialCache[batteryPrefix]}"`
          );
        }
      }

      const serial = serialCache[batteryPrefix];
      if (serial === undefined) {
        continue;
      }

      const block5000To5034 =
        mqttState[`${batteryPrefix}/queries/register_5000_length_34/data`];
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

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_minimum`,
          `${Math.min(...cellVoltages)}`
        );
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

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_voltage_variance`,
          (Math.max(...cellVoltages) - Math.min(...cellVoltages)).toFixed(1)
        );
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

        for (let i = 0; i < cellVoltages.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/voltage`,
            `${cellVoltages[i]!}`
          );
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

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_minimum`,
          `${Math.min(...cellTemperatures)}`
        );
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

        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/cell_temperature_variance`,
          (
            Math.max(...cellTemperatures) - Math.min(...cellTemperatures)
          ).toFixed(1)
        );
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

        for (let i = 0; i < cellTemperatures.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${config.mqtt.prefix}/batteries/${serial}/cells/${cellNumber}/temperature`,
            `${cellTemperatures[i]!}`
          );
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

      const block5035To5053 =
        mqttState[`${batteryPrefix}/queries/register_5035_length_18/data`];
      if (block5035To5053 !== undefined) {
        const { numberAt } = bufferParsersOf(block5035To5053, 5035);

        const amperage = Number(
          (0.01 * numberAt(5042, 1, "signed")).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/amperage`,
          `${amperage}`
        );
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

        const voltage = Number(
          (0.1 * numberAt(5043, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/voltage`,
          `${voltage}`
        );
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

        const wattage = Number((amperage * voltage).toFixed(1));
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/wattage`,
          `${wattage}`
        );
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

        const chargeAh = Number(
          (0.001 * numberAt(5044, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/charge`,
          (chargeAh * voltage).toFixed(1)
        );
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

        const capacityAh = Number(
          (0.001 * numberAt(5046, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/capacity`,
          (capacityAh * voltage).toFixed(1)
        );
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

        const soc = Number((100.0 * (chargeAh / capacityAh)).toFixed(3));
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/soc`,
          `${soc}`
        );
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

        const timeToFull = Number(
          (amperage > 0 ? (capacityAh - chargeAh) / amperage : 0).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/time_to_full`,
          `${timeToFull}`
        );
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

        const timeToEmpty = Number(
          (amperage < 0 ? Math.abs(chargeAh / amperage) : 0).toFixed(2)
        );
        await mqttPublish(
          `${config.mqtt.prefix}/batteries/${serial}/time_to_empty`,
          `${timeToEmpty}`
        );
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
        mqttState[`${batteryPrefix}/queries/register_5100_length_42/data`];
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

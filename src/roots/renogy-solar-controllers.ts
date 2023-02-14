import Mqtt from "async-mqtt";
import { bufferParsersOf } from "../utils";
import { z } from "zod";
import { parseEnv } from "znv";

(async () => {
  const env = parseEnv(process.env, {
    HOME_ASSISTANT_DISCOVERY_PREFIX: z.string().optional(),
    MQTT_PREFIX: z.string(),
    MQTT_URI: z.string(),
    SOURCES: z.string().transform((string) => string.split(":")),
  });

  const mqttConn = await Mqtt.connectAsync(env.MQTT_URI, {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${env.MQTT_PREFIX}/status`,
    },
  });
  await mqttConn.publish(`${env.MQTT_PREFIX}/status`, "online", {
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
    const prefixes: Array<string> = [];
    for (const [topic, payload] of Object.entries(mqttState)) {
      if (/\/servers\/[0-9]+\/attributes\.json$/.test(topic)) {
        const serverPrefix = topic.replace(/\/attributes\.json$/, "");

        const json = JSON.parse(payload.toString("utf-8"));
        if (json["renogy_solar_controller"] === true) {
          prefixes.push(serverPrefix);
        }
      }
    }

    for (const prefix of prefixes) {
      if (serialCache[prefix] === undefined) {
        const queryData =
          mqttState[`${prefix}/queries/register_10_length_22/data`];

        if (false) {
        } else if (queryData !== undefined) {
          serialCache[prefix] = `${bufferParsersOf(queryData, 10).numberAt(
            24,
            2,
            "unsigned"
          )}`;
        }

        if (serialCache[prefix] !== undefined) {
          console.log(
            `associating prefix "${prefix}" with serial "${serialCache[prefix]}"`
          );
        }
      }

      const serial = serialCache[prefix];
      if (serial === undefined) {
        continue;
      }

      const block00010UpTo00032 =
        mqttState[`${prefix}/queries/register_10_length_22/data`];
      if (block00010UpTo00032 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt, asciiAt } = bufferParsersOf(
          block00010UpTo00032,
          10
        );

        const systemVoltage = upperByteAt(10, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/system_voltage`,
          `${systemVoltage}`
        );

        const ratedChargeAmperage = lowerByteAt(10, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/rated_charge_amperage`,
          `${ratedChargeAmperage}`
        );

        const ratedDischargeAmperage = upperByteAt(11, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/rated_discharge_amperage`,
          `${ratedDischargeAmperage}`
        );

        const productTypeLookup: Record<number, string> = {
          0: "controller",
        };
        const productTypeCode = lowerByteAt(11, "unsigned");
        const productType =
          productTypeLookup[productTypeCode] ?? `code:${productTypeCode}`;
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/product_type`,
          `${productType}`
        );

        const model = asciiAt(12, 8);
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/model`,
          `${model}`
        );

        const softwareVersion = numberAt(20, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/software_version`,
          `${softwareVersion}`
        );

        const hardwareVersion = numberAt(22, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/hardwareVersion`,
          `${hardwareVersion}`
        );

        const serialNumber = numberAt(24, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/serial`,
          `${serialNumber}`
        );

        const address = numberAt(26, 1, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/address`,
          `${address}`
        );

        const protocolVersion = numberAt(27, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/protocol_version`,
          `${protocolVersion}`
        );

        const uniqueIdCode = numberAt(29, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/unique_id_code`,
          `${uniqueIdCode}`
        );
      }

      const block00256UpTo00266 =
        mqttState[`${prefix}/queries/register_256_length_10/data`];
      if (block00256UpTo00266 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt } = bufferParsersOf(
          block00256UpTo00266,
          256
        );

        const batterySoc = numberAt(256, 1, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/battery_soc`,
          `${batterySoc}`
        );

        const batteryVoltage = Number(
          (0.1 * numberAt(257, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/battery_voltage`,
          `${batteryVoltage}`
        );

        const chargingAmperage = Number(
          (0.01 * numberAt(258, 1, "unsigned")).toFixed(2)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/charging_amperage`,
          `${chargingAmperage}`
        );

        const controllerTemperature = upperByteAt(259, "signed");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/controller_temperature`,
          `${controllerTemperature}`
        );

        const batteryTemperature = lowerByteAt(259, "signed");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/battery_temperature`,
          `${batteryTemperature}`
        );

        const loadVoltage = Number(
          (0.1 * numberAt(260, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/load_voltage`,
          `${loadVoltage}`
        );

        const loadAmperage = Number(
          (0.01 * numberAt(261, 1, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/load_amperage`,
          `${loadAmperage}`
        );

        const loadWattage = numberAt(262, 1, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/load_wattage`,
          `${loadWattage}`
        );

        const solarVoltage = Number(
          (0.1 * numberAt(263, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/solar_voltage`,
          `${solarVoltage}`
        );

        const solarAmperage = Number(
          (0.01 * numberAt(264, 1, "unsigned")).toFixed(2)
        );
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/solar_amperage`,
          `${solarAmperage}`
        );

        const solarWattage = numberAt(265, 1, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/solar_wattage`,
          `${solarWattage}`
        );
      }

      const block00266upTo00288 =
        mqttState[`${prefix}/queries/register_266_length_22/data`];
      if (block00266upTo00288 !== undefined) {
      }

      const block00288UpTo00292 =
        mqttState[`${prefix}/queries/register_288_length_4/data`];
      if (block00288UpTo00292 !== undefined) {
        const { numberAt, upperByteAt, lowerByteAt } = bufferParsersOf(
          block00288UpTo00292,
          288
        );

        const streetLightBrightness = -1 * upperByteAt(288, "signed");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/street_light_brightness`,
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
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/charging_status`,
          `${chargingStatus}`
        );

        const alarm = numberAt(289, 2, "unsigned");
        await mqttPublish(
          `${env.MQTT_PREFIX}/solar_controllers/${serial}/alarm`,
          `${alarm}`
        );
      }

      const block57345UpTo57360 =
        mqttState[`${prefix}/queries/register_57345_length_15/data`];
      if (block57345UpTo57360 !== undefined) {
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

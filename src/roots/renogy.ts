import Mqtt from "async-mqtt";
import { bufferParsersOf, log } from "../utils";

const main = async () => {
  const mqttPrefix = "renogy";
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

  const serverSerialMap: Record<number, string> = {};
  const mqttState: Record<string, Buffer> = {};
  const onIntervalFn = async () => {
    for (const server of [48, 49, 50, 51]) {
      if (serverSerialMap[server] === undefined) {
        const block = mqttState[`modbus/blocks/${server}::5100..5142`];

        if (block !== undefined) {
          serverSerialMap[server] = bufferParsersOf(block, 5100).asciiAt(
            5110,
            8
          );
        }
      }

      const serial = serverSerialMap[server];
      if (serial === undefined) {
        continue;
      }

      const block5000To5034 = mqttState[`modbus/blocks/${server}::5000..5034`];
      if (block5000To5034 !== undefined) {
        const { numberAt } = bufferParsersOf(block5000To5034, 5000);

        const cellVoltages = [
          Number((0.1 * numberAt(5001, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5002, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5003, 1, "unsigned")).toFixed(1)),
          Number((0.1 * numberAt(5004, 1, "unsigned")).toFixed(1)),
        ];

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cell_voltage_maximum`,
          `${Math.max(...cellVoltages)}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_maximum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_maximum/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_voltage_maximum`,
              unique_id: `renogy_battery_${serial}_cell_voltage_maximum`,
              unit_of_measurement: "V",
            })
          );
        }

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cell_voltage_minimum`,
          `${Math.min(...cellVoltages)}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_minimum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_minimum/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_voltage_minimum`,
              unique_id: `renogy_battery_${serial}_cell_voltage_minimum`,
              unit_of_measurement: "V",
            })
          );
        }

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cell_voltage_variance`,
          (Math.max(...cellVoltages) - Math.min(...cellVoltages)).toFixed(1)
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_variance/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_voltage_variance/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_voltage_variance`,
              unique_id: `renogy_battery_${serial}_cell_voltage_variance`,
              unit_of_measurement: "V",
            })
          );
        }

        for (let i = 0; i < cellVoltages.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${mqttPrefix}/batteries/${serial}/cells/${cellNumber}/voltage`,
            `${cellVoltages[i]!}`
          );
          if (
            published[
              `${haPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_voltage/config`
            ] === undefined
          ) {
            await mqttPublish(
              `${haPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_voltage/config`,
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
                state_topic: `${mqttPrefix}/batteries/${serial}/cells/${cellNumber}/voltage`,
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
          `${mqttPrefix}/batteries/${serial}/cell_temperature_maximum`,
          `${Math.max(...cellTemperatures)}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_maximum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_maximum/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_temperature_maximum`,
              unique_id: `renogy_battery_${serial}_cell_temperature_maximum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cell_temperature_minimum`,
          `${Math.min(...cellTemperatures)}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_minimum/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_minimum/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_temperature_minimum`,
              unique_id: `renogy_battery_${serial}_cell_temperature_minimum`,
              unit_of_measurement: "°C",
            })
          );
        }

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cell_temperature_variance`,
          (
            Math.max(...cellTemperatures) - Math.min(...cellTemperatures)
          ).toFixed(1)
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_Variance/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_cell_temperature_Variance/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/cell_temperature_Variance`,
              unique_id: `renogy_battery_${serial}_cell_temperature_Variance`,
              unit_of_measurement: "°C",
            })
          );
        }

        for (let i = 0; i < cellTemperatures.length; i++) {
          const cellNumber = `${i + 1}`.padStart(2, "0");

          await mqttPublish(
            `${mqttPrefix}/batteries/${serial}/cells/${cellNumber}/temperature`,
            `${cellTemperatures[i]!}`
          );
          if (
            published[
              `${haPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_temperature/config`
            ] === undefined
          ) {
            await mqttPublish(
              `${haPrefix}/sensor/renogy_battery_${serial}_cell_${cellNumber}_temperature/config`,
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
                state_topic: `${mqttPrefix}/batteries/${serial}/cells/${cellNumber}/temperature`,
                unique_id: `renogy_battery_${serial}_cell_${cellNumber}_temperature`,
                unit_of_measurement: "°C",
              })
            );
          }
        }
      }

      const block5035To5053 = mqttState[`modbus/blocks/${server}::5035..5053`];
      if (block5035To5053 !== undefined) {
        const { numberAt } = bufferParsersOf(block5035To5053, 5035);

        const amperage = Number(
          (0.01 * numberAt(5042, 1, "signed")).toFixed(2)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/amperage`,
          `${amperage}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_amperage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_amperage/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/amperage`,
              unique_id: `renogy_battery_${serial}_amperage`,
              unit_of_measurement: "A",
            })
          );
        }

        const voltage = Number(
          (0.1 * numberAt(5043, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/voltage`,
          `${voltage}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_voltage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_voltage/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/voltage`,
              unique_id: `renogy_battery_${serial}_voltage`,
              unit_of_measurement: "V",
            })
          );
        }

        const wattage = Number((amperage * voltage).toFixed(1));
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/wattage`,
          `${wattage}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_wattage/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_wattage/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/wattage`,
              unique_id: `renogy_battery_${serial}_wattage`,
              unit_of_measurement: "W",
            })
          );
        }

        const chargeAh = Number(
          (0.001 * numberAt(5044, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/charge`,
          (chargeAh * voltage).toFixed(1)
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_charge/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_charge/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/charge`,
              unique_id: `renogy_battery_${serial}_charge`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const capacityAh = Number(
          (0.001 * numberAt(5046, 2, "unsigned")).toFixed(3)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/capacity`,
          (capacityAh * voltage).toFixed(1)
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_capacity/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_capacity/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/capacity`,
              unique_id: `renogy_battery_${serial}_capacity`,
              unit_of_measurement: "Wh",
            })
          );
        }

        const soc = Number((100.0 * (chargeAh / capacityAh)).toFixed(3));
        await mqttPublish(`${mqttPrefix}/batteries/${serial}/soc`, `${soc}`);
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_soc/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_soc/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/soc`,
              unique_id: `renogy_battery_${serial}_soc`,
              unit_of_measurement: "%",
            })
          );
        }

        const timeToFull = Number(
          (amperage > 0 ? (capacityAh - chargeAh) / amperage : 0).toFixed(2)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/time_to_full`,
          `${timeToFull}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_time_to_full/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_time_to_full/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/time_to_full`,
              unique_id: `renogy_battery_${serial}_time_to_full`,
              unit_of_measurement: "h",
            })
          );
        }

        const timeToEmpty = Number(
          (amperage < 0 ? Math.abs(chargeAh / amperage) : 0).toFixed(2)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/time_to_empty`,
          `${timeToEmpty}`
        );
        if (
          published[
            `${haPrefix}/sensor/renogy_battery_${serial}_time_to_empty/config`
          ] === undefined
        ) {
          await mqttPublish(
            `${haPrefix}/sensor/renogy_battery_${serial}_time_to_empty/config`,
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
              state_topic: `${mqttPrefix}/batteries/${serial}/time_to_empty`,
              unique_id: `renogy_battery_${serial}_time_to_empty`,
              unit_of_measurement: "h",
            })
          );
        }

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/cycle`,
          `${numberAt(5048, 1, "unsigned")}`
        );

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/charge_voltage_limit`,
          (0.1 * numberAt(5049, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/discharge_voltage_limit`,
          (0.1 * numberAt(5050, 1, "unsigned")).toFixed(1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/charge_amerpage_limit`,
          (0.01 * numberAt(5051, 1, "signed")).toFixed(1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/discharge_amperage_limit`,
          (0.01 * numberAt(5052, 1, "signed")).toFixed(1)
        );
      }

      const block5100To5142 = mqttState[`modbus/blocks/${server}::5100..5142`];
      if (block5100To5142 !== undefined) {
        const { asciiAt, numberAt } = bufferParsersOf(block5100To5142, 5100);

        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/alarminfo_cell_voltage`,
          `${numberAt(5100, 2, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/alarminfo_cell_temperature`,
          `${numberAt(5102, 2, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/alarminfo_other`,
          `${numberAt(5104, 2, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/status1`,
          `${numberAt(5106, 1, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/status2`,
          `${numberAt(5107, 1, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/status3`,
          `${numberAt(5108, 1, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/status_charge_discharge`,
          `${numberAt(5109, 1, "unsigned")}`
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/serial`,
          asciiAt(5110, 8)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/manufacturer_version`,
          asciiAt(5118, 1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/mainline_version`,
          asciiAt(5119, 2)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/communication_protocol_version`,
          asciiAt(5121, 1)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/model`,
          asciiAt(5122, 8)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/software_version`,
          asciiAt(5130, 2)
        );
        await mqttPublish(
          `${mqttPrefix}/batteries/${serial}/manufacturer_name`,
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

  mqttConn.subscribe(["modbus/blocks/#"]);
};

main().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";

const pMapSeries = async <In, Out>(
  iterable: Array<In>,
  mapper: (item: In) => Promise<Out>
) => {
  const result: Array<Out> = [];

  for (const item of iterable) {
    result.push(await mapper(item));
  }

  return result;
};

const publishBatteryModuleConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  batteryModule: BatteryModule,
  dict: object
) =>
  await pMapSeries(Object.entries(dict), async ([property, props]) => {
    console.log(
      `[INFO] publishing home assistant config endpoint for ${batteryModule.serial}.${property}`
    );
    return await mqttConn.publish(
      `homeassistant/sensor/batterymodule_${batteryModule.serial}_${property}/config`,
      JSON.stringify({
        ...props,
        device: {
          identifiers: [batteryModule.serial],
          manufacturer: batteryModule.manufacturerName,
          model: batteryModule.model,
        },
        object_id: `batterymodule_${batteryModule.serial}_${property}`,
        state_topic: `homeassistant/sensor/batterymodule_${batteryModule.serial}_${property}/state`,
        unique_id: `batterymodule_${batteryModule.serial}_${property}`,
      }),
      {
        retain: true,
      }
    );
  });

const publishBatteryConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  dict: object
) =>
  await pMapSeries(Object.entries(dict), async ([property, props]) => {
    console.log(
      `[INFO] publishing home assistant config endpoint for battery.${property}`
    );
    return await mqttConn.publish(
      `homeassistant/sensor/battery_${property}/config`,
      JSON.stringify({
        ...props,
        device: {
          identifiers: "battery",
        },
        object_id: `battery_${property}`,
        state_topic: `homeassistant/sensor/battery_${property}/state`,
        unique_id: `battery_${property}`,
      }),
      { retain: true }
    );
  });

const publishBatteryModuleStates = async (
  mqttConn: Mqtt.AsyncMqttClient,
  batteryModule: BatteryModule,
  dict: object
) =>
  await pMapSeries(Object.entries(dict), async ([property, value]) => {
    console.log(
      `[INFO] publishing home assistant state endpoint for ${batteryModule.serial}.${property}: ${value}`
    );
    return await mqttConn.publish(
      `homeassistant/sensor/batterymodule_${batteryModule.serial}_${property}/state`,
      `${value}`
    );
  });

const publishBatteryStates = async (
  mqttConn: Mqtt.AsyncMqttClient,
  dict: object
) =>
  await pMapSeries(Object.entries(dict), async ([property, value]) => {
    console.log(
      `[INFO] publishing home assistant state endpoint for battery.${property}: ${value}`
    );
    return await mqttConn.publish(
      `homeassistant/sensor/battery_${property}/state`,
      `${value}`
    );
  });

const queryServer = async (modbusConn: Modbus, server: number) => {
  console.log(`[INFO] querying server ${server}`);

  modbusConn.setID(server);
  const blockAlpha = (await modbusConn.readHoldingRegisters(5000, 5034 - 5000))
    .buffer;
  const blockBeta = (await modbusConn.readHoldingRegisters(5035, 5053 - 5035))
    .buffer;
  const blockDelta = (await modbusConn.readHoldingRegisters(5100, 5142 - 5100))
    .buffer;

  return { blockAlpha, blockBeta, blockDelta };
};
type ServerData = Awaited<ReturnType<typeof queryServer>>;

const batteryModuleOf = ({ blockAlpha, blockBeta, blockDelta }: ServerData) => {
  const offsetOf = (blockStartsAt: number, registerStartsAt: number) =>
    (registerStartsAt - blockStartsAt) * 2;

  const indexesOf = (
    blockStartsAt: number,
    registerStartsAt: number,
    length: number
  ) =>
    <const>[
      offsetOf(blockStartsAt, registerStartsAt),
      offsetOf(blockStartsAt, registerStartsAt) + length * 2,
    ];

  const replaceArgs = <const>[/\x00+$/, ""];

  const data = <const>{
    cellVoltageCount: 1.0 * blockAlpha.readUInt16BE(offsetOf(5000, 5000)),
    cell01Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5001)),
    cell02Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5002)),
    cell03Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5003)),
    cell04Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5004)),
    cell05Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5005)),
    cell06Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5006)),
    cell07Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5007)),
    cell08Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5008)),
    cell09Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5009)),
    cell10Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5010)),
    cell11Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5011)),
    cell12Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5012)),
    cell13Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5013)),
    cell14Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5014)),
    cell15Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5015)),
    cell16Voltage: 0.1 * blockAlpha.readUInt16BE(offsetOf(5000, 5016)),
    cellTempCount: 1.0 * blockAlpha.readUInt16BE(offsetOf(5000, 5017)),
    cell01Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5018)),
    cell02Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5019)),
    cell03Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5020)),
    cell04Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5021)),
    cell05Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5022)),
    cell06Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5023)),
    cell07Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5024)),
    cell08Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5025)),
    cell09Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5026)),
    cell10Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5027)),
    cell11Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5028)),
    cell12Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5029)),
    cell13Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5030)),
    cell14Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5031)),
    cell15Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5032)),
    cell16Temp: 0.1 * blockAlpha.readInt16BE(offsetOf(5000, 5033)),
    bmsTemp: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5035)),
    envTempCount: 1.0 * blockBeta.readUInt16BE(offsetOf(5035, 5036)),
    env01Temp: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5037)),
    env02Temp: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5038)),
    heaterTempCount: 1.0 * blockBeta.readUInt16BE(offsetOf(5035, 5039)),
    heater01Temp: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5040)),
    heater02Temp: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5041)),
    amperage: 0.01 * blockBeta.readInt16BE(offsetOf(5035, 5042)),
    voltage: 0.1 * blockBeta.readUInt16BE(offsetOf(5035, 5043)),
    energyRemaining: 0.001 * blockBeta.readUInt32BE(offsetOf(5035, 5044)),
    energyCapacity: 0.001 * blockBeta.readUInt32BE(offsetOf(5035, 5046)),
    cycleNumber: 1.0 * blockBeta.readInt16BE(offsetOf(5035, 5048)),
    chargeVoltageLimit: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5049)),
    dischargeVoltageLimit: 0.1 * blockBeta.readInt16BE(offsetOf(5035, 5050)),
    chargeCurrentLimit: 0.01 * blockBeta.readInt16BE(offsetOf(5035, 5051)),
    dischargeCurrentLimit: 0.01 * blockBeta.readInt16BE(offsetOf(5035, 5052)),
    alarminfoCellVoltage: blockDelta.readUInt32BE(offsetOf(5100, 5100)),
    alarminfoCellTemperature: blockDelta.readUInt32BE(offsetOf(5100, 5102)),
    alarminfoOther: blockDelta.readUInt32BE(offsetOf(5100, 5104)),
    status1: blockDelta.readUInt16BE(offsetOf(5100, 5106)),
    status2: blockDelta.readUInt16BE(offsetOf(5100, 5107)),
    status3: blockDelta.readUInt16BE(offsetOf(5100, 5108)),
    statusChargeDischarge: blockDelta.readUInt16BE(offsetOf(5100, 5109)),
    serial: blockDelta
      .toString("ascii", ...indexesOf(5100, 5110, 8))
      .replace(...replaceArgs),
    manufacturerVersion: blockDelta
      .toString("ascii", ...indexesOf(5100, 5118, 1))
      .replace(...replaceArgs),
    mainlineVersion: blockDelta
      .toString("ascii", ...indexesOf(5100, 5119, 2))
      .replace(...replaceArgs),
    communicationProtocolVersion: blockDelta
      .toString("ascii", ...indexesOf(5100, 5121, 1))
      .replace(...replaceArgs),
    model: blockDelta
      .toString("ascii", ...indexesOf(5100, 5122, 8))
      .replace(...replaceArgs),
    softwareVersion: blockDelta
      .toString("ascii", ...indexesOf(5100, 5130, 2))
      .replace(...replaceArgs),
    manufacturerName: blockDelta
      .toString("ascii", ...indexesOf(5100, 5132, 10))
      .replace(...replaceArgs),
  };

  return {
    ...data,
    energy: 100.0 * (data.energyRemaining / data.energyCapacity),
    power: data.amperage * data.voltage,
    timeToFull:
      data.amperage > 0
        ? (data.energyCapacity - data.energyRemaining) / data.amperage
        : undefined,
    timeToEmpty:
      data.amperage < 0
        ? Math.abs(data.energyRemaining / data.amperage)
        : undefined,
  };
};
type BatteryModule = Awaited<ReturnType<typeof batteryModuleOf>>;

const batteryOf = (batteryModules: Array<BatteryModule>) => {
  const batteryBankSums = batteryModules.reduce(
    (bank, battery) => {
      return {
        amperage: bank.amperage + battery.amperage,
        voltage: bank.voltage + battery.voltage,
        energyCapacity: bank.energyCapacity + battery.energyCapacity,
        energyRemaining: bank.energyRemaining + battery.energyRemaining,
      };
    },
    {
      amperage: 0,
      voltage: 0,
      energyRemaining: 0,
      energyCapacity: 0,
    }
  );

  return {
    ...batteryBankSums,
    batteryModules,
    voltage: batteryBankSums.voltage / batteryModules.length,
    energy:
      100.0 *
      (batteryBankSums.energyRemaining / batteryBankSums.energyCapacity),
    power:
      batteryBankSums.amperage *
      (batteryBankSums.voltage / batteryModules.length),
    timeToFull:
      batteryBankSums.amperage > 0
        ? (batteryBankSums.energyCapacity - batteryBankSums.energyRemaining) /
          batteryBankSums.amperage
        : undefined,
    timeToEmpty:
      batteryBankSums.amperage < 0
        ? Math.abs(batteryBankSums.energyRemaining / batteryBankSums.amperage)
        : undefined,
  };
};

(async () => {
  const servers = [48, 49, 50, 51];

  const configs = {
    amperage: {
      device_class: "current",
      state_class: "measurement",
      unit_of_measurement: "A",
    },
    energy: {
      device_class: "battery",
      state_class: "measurement",
      unit_of_measurement: "%",
    },
    energy_capacity: {
      device_class: "energy",
      state_class: "measurement",
      unit_of_measurement: "Wh",
    },
    energy_remaining: {
      device_class: "energy",
      state_class: "measurement",
      unit_of_measurement: "Wh",
    },
    power: {
      device_class: "power",
      state_class: "measurement",
      unit_of_measurement: "W",
    },
    time_to_empty: {
      device_class: "duration",
      unit_of_measurement: "h",
    },
    time_to_full: {
      device_class: "duration",
      unit_of_measurement: "h",
    },
    voltage: {
      device_class: "voltage",
      state_class: "measurement",
      unit_of_measurement: "V",
    },
  };

  for (;;) {
    try {
      console.log(`[INFO] Connecting to MQTT`);
      const mqttConn = await Mqtt.connectAsync("tcp://macos-server.lan:1883");

      console.log(`[INFO] Connecting to MODBUS`);
      const modbusConn = new Modbus();
      await modbusConn.connectTCP("macos-server.lan", {
        port: 502,
      });

      console.log(`[INFO] begin config publish`);
      await publishBatteryConfigs(mqttConn, configs);
      for (const server of servers) {
        console.log(`[INFO] config'ing server ${server}`);
        const serverData = await queryServer(modbusConn, server);
        const batteryModule = batteryModuleOf(serverData);

        await publishBatteryModuleConfigs(mqttConn, batteryModule, {
          ...configs,
          cycleNumber: {
            entity_category: "diagnostic",
            state_class: "measurement",
          },
        });
      }

      console.log(`[INFO] begin main loop`);
      for (;;) {
        const batteryModules: Array<BatteryModule> = [];
        for (const server of servers) {
          const serverData = await queryServer(modbusConn, server);
          const batteryModule = batteryModuleOf(serverData);

          await publishBatteryModuleStates(mqttConn, batteryModule, {
            amperage: batteryModule.amperage.toFixed(2),
            cycleNumber: batteryModule.cycleNumber,
            energy: batteryModule.energy.toFixed(2),
            energy_capacity: (
              batteryModule.energyCapacity * batteryModule.voltage
            ).toFixed(2),
            energy_remaining: (
              batteryModule.energyRemaining * batteryModule.voltage
            ).toFixed(2),
            power: batteryModule.power.toFixed(3),
            time_to_empty:
              batteryModule.timeToEmpty?.toFixed(2) ?? "unavailable",
            time_to_full: batteryModule.timeToFull?.toFixed(2) ?? "unavailable",
            voltage: batteryModule.voltage.toFixed(1),
          });

          batteryModules.push(batteryModule);
        }

        const battery = batteryOf(batteryModules);
        await publishBatteryStates(mqttConn, {
          amperage: battery.amperage.toFixed(2),
          energy: battery.energy.toFixed(2),
          energy_capacity: (battery.energyCapacity * battery.voltage).toFixed(
            3
          ),
          energy_remaining: (battery.energyRemaining * battery.voltage).toFixed(
            3
          ),
          power: battery.power.toFixed(2),
          time_to_empty: battery.timeToEmpty?.toFixed(2) ?? "unavailable",
          time_to_full: battery.timeToFull?.toFixed(2) ?? "unavailable",
          voltage: battery.voltage.toFixed(1),
        });
      }
    } catch (error) {
      console.log(`[ERROR] Error: ${JSON.stringify(error)}`);
    }
  }
})();

import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import CRC32 from "crc-32";
import { log, queryModbus, runForever, uniqueStrings } from "./utils";

const servers = <const>[48, 49, 50, 51];
const homeAssistantConfigs = {
  amperage: {
    device_class: "current",
    name: "Amperage",
    state_class: "measurement",
    unit_of_measurement: "A",
  },
  cell_count: {
    entity_category: "diagnostic",
    name: "Cell Count",
    state_class: "measurement",
  },
  "cell_%%_temperature": {
    device_class: "temperature",
    entity_category: "diagnostic",
    name: "Cell %% Temperature",
    state_class: "measurement",
    unit_of_measurement: "°C",
  },
  "cell_%%_voltage": {
    device_class: "voltage",
    entity_category: "diagnostic",
    name: "Cell %% Voltage",
    state_class: "measurement",
    unit_of_measurement: "V",
  },
  cycle_number: {
    entity_category: "diagnostic",
    name: "Cycle Number",
    state_class: "measurement",
  },
  energy: {
    device_class: "battery",
    name: "Energy",
    state_class: "measurement",
    unit_of_measurement: "%",
  },
  energy_capacity: {
    device_class: "energy",
    name: "Energy Capacity",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  },
  energy_remaining: {
    device_class: "energy",
    name: "Energy Remaining",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  },
  power: {
    device_class: "power",
    name: "Power",
    state_class: "measurement",
    unit_of_measurement: "W",
  },
  time_to_empty: {
    device_class: "duration",
    name: "Time To Empty",
    unit_of_measurement: "h",
  },
  time_to_full: {
    device_class: "duration",
    name: "Time To Full",
    unit_of_measurement: "h",
  },
  voltage: {
    device_class: "voltage",
    name: "Voltage",
    state_class: "measurement",
    unit_of_measurement: "V",
  },
};
const configsForCellCount = (cellCount: number) => {
  let configs: Record<
    string,
    typeof homeAssistantConfigs[keyof typeof homeAssistantConfigs]
  > = {};

  for (const [property, props] of Object.entries(homeAssistantConfigs)) {
    if (property.includes("%%")) {
      for (let i = 0; i < cellCount; i++) {
        const cellNumberString = (i + 1).toString().padStart(2, "0");
        const numberedProperty = property.split("%%").join(cellNumberString);
        const numberedName = props.name.split("%%").join(cellNumberString);

        configs[numberedProperty] = {
          ...props,
          name: numberedName,
        };
      }
    } else {
      configs[property] = props;
    }
  }

  return configs;
};
const statesForDict = (dict: Record<string, unknown>) => {
  let states: Record<string, unknown> = {};

  for (const [property, value] of Object.entries(dict)) {
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        const numberedProperty = property
          .split("%%")
          .join((i + 1).toString().padStart(2, "0"));

        states[numberedProperty] = value[i];
      }
    } else {
      states[property] = value;
    }
  }

  return states;
};

const queryServer = async (modbusConn: Modbus, server: number) => {
  log.info(`querying server ${server}`);

  return <const>{
    ...(await queryModbus(
      modbusConn,
      server,
      5000,
      5034,
      ({ numberAt }) =>
        <const>{
          cellVoltageCount: 1.0 * numberAt(5000, 1, "unsigned"),
          cell01Voltage: 0.1 * numberAt(5001, 1, "unsigned"),
          cell02Voltage: 0.1 * numberAt(5002, 1, "unsigned"),
          cell03Voltage: 0.1 * numberAt(5003, 1, "unsigned"),
          cell04Voltage: 0.1 * numberAt(5004, 1, "unsigned"),
          cell05Voltage: 0.1 * numberAt(5005, 1, "unsigned"),
          cell06Voltage: 0.1 * numberAt(5006, 1, "unsigned"),
          cell07Voltage: 0.1 * numberAt(5007, 1, "unsigned"),
          cell08Voltage: 0.1 * numberAt(5008, 1, "unsigned"),
          cell09Voltage: 0.1 * numberAt(5009, 1, "unsigned"),
          cell10Voltage: 0.1 * numberAt(5010, 1, "unsigned"),
          cell11Voltage: 0.1 * numberAt(5011, 1, "unsigned"),
          cell12Voltage: 0.1 * numberAt(5012, 1, "unsigned"),
          cell13Voltage: 0.1 * numberAt(5013, 1, "unsigned"),
          cell14Voltage: 0.1 * numberAt(5014, 1, "unsigned"),
          cell15Voltage: 0.1 * numberAt(5015, 1, "unsigned"),
          cell16Voltage: 0.1 * numberAt(5016, 1, "unsigned"),
          cellTempCount: 1.0 * numberAt(5017, 1, "unsigned"),
          cell01Temperature: 0.1 * numberAt(5018, 1, "signed"),
          cell02Temperature: 0.1 * numberAt(5019, 1, "signed"),
          cell03Temperature: 0.1 * numberAt(5020, 1, "signed"),
          cell04Temperature: 0.1 * numberAt(5021, 1, "signed"),
          cell05Temperature: 0.1 * numberAt(5022, 1, "signed"),
          cell06Temperature: 0.1 * numberAt(5023, 1, "signed"),
          cell07Temperature: 0.1 * numberAt(5024, 1, "signed"),
          cell08Temperature: 0.1 * numberAt(5025, 1, "signed"),
          cell09Temperature: 0.1 * numberAt(5026, 1, "signed"),
          cell10Temperature: 0.1 * numberAt(5027, 1, "signed"),
          cell11Temperature: 0.1 * numberAt(5028, 1, "signed"),
          cell12Temperature: 0.1 * numberAt(5029, 1, "signed"),
          cell13Temperature: 0.1 * numberAt(5030, 1, "signed"),
          cell14Temperature: 0.1 * numberAt(5031, 1, "signed"),
          cell15Temperature: 0.1 * numberAt(5032, 1, "signed"),
          cell16Temperature: 0.1 * numberAt(5033, 1, "signed"),
        }
    )),

    ...(await queryModbus(
      modbusConn,
      server,
      5035,
      5053,
      ({ numberAt }) =>
        <const>{
          bmsTemp: 0.1 * numberAt(5035, 1, "signed"),
          envTempCount: 1.0 * numberAt(5036, 1, "unsigned"),
          env01Temp: 0.1 * numberAt(5037, 1, "signed"),
          env02Temp: 0.1 * numberAt(5038, 1, "signed"),
          heaterTempCount: 1.0 * numberAt(5039, 1, "unsigned"),
          heater01Temp: 0.1 * numberAt(5040, 1, "signed"),
          heater02Temp: 0.1 * numberAt(5041, 1, "signed"),
          amperage: 0.01 * numberAt(5042, 1, "signed"),
          voltage: 0.1 * numberAt(5043, 1, "unsigned"),
          energyRemaining: 0.001 * numberAt(5044, 2, "unsigned"),
          energyCapacity: 0.001 * numberAt(5046, 2, "unsigned"),
          cycleNumber: 1.0 * numberAt(5048, 1, "signed"),
          chargeVoltageLimit: 0.1 * numberAt(5049, 1, "signed"),
          dischargeVoltageLimit: 0.1 * numberAt(5050, 1, "signed"),
          chargeCurrentLimit: 0.01 * numberAt(5051, 1, "signed"),
          dischargeCurrentLimit: 0.01 * numberAt(5052, 1, "signed"),
        }
    )),

    ...(await queryModbus(
      modbusConn,
      server,
      5100,
      5142,
      ({ asciiAt, numberAt }) =>
        <const>{
          alarminfoCellVoltage: numberAt(5100, 2, "unsigned"),
          alarminfoCellTemperature: numberAt(5102, 2, "unsigned"),
          alarminfoOther: numberAt(5104, 2, "unsigned"),
          status1: numberAt(5106, 1, "unsigned"),
          status2: numberAt(5107, 1, "unsigned"),
          status3: numberAt(5108, 1, "unsigned"),
          statusChargeDischarge: numberAt(5109, 1, "unsigned"),
          serial: asciiAt(5110, 8),
          manufacturerVersion: asciiAt(5118, 1),
          mainlineVersion: asciiAt(5119, 2),
          communicationProtocolVersion: asciiAt(5121, 1),
          model: asciiAt(5122, 8),
          softwareVersion: asciiAt(5130, 2),
          manufacturerName: asciiAt(5132, 10),
        }
    )),
  };
};
type ServerData = Awaited<ReturnType<typeof queryServer>>;

const moduleOf = (data: ServerData) => {
  let cellTemperatures: Array<number> = [];
  let cellVoltages: Array<number> = [];
  for (let i = 0; i < data.cellVoltageCount; i++) {
    const cellTemperatureKey: keyof ServerData = <any>(
      `cell${(i + 1).toString().padStart(2, "0")}Temperature`
    );
    const cellVoltageKey: keyof ServerData = <any>(
      `cell${(i + 1).toString().padStart(2, "0")}Voltage`
    );

    cellTemperatures = [...cellTemperatures, <number>data[cellTemperatureKey]];
    cellVoltages = [...cellVoltages, <number>data[cellVoltageKey]];
  }

  return {
    ...data,
    cellCount: data.cellVoltageCount,
    cellTemperatures,
    cellVoltages,
    energy: 100.0 * (data.energyRemaining / data.energyCapacity),
    power: data.amperage * data.voltage,
    serial: data.serial.replace(/[^A-Za-z0-9]/g, ""),
    timeToFull:
      data.amperage > 0
        ? (data.energyCapacity - data.energyRemaining) / data.amperage
        : 0,
    timeToEmpty:
      data.amperage < 0 ? Math.abs(data.energyRemaining / data.amperage) : 0,
  };
};
type Module = Awaited<ReturnType<typeof moduleOf>>;

const batteryOf = (modules: Array<Module>) => {
  const sortedModules = [...modules].sort((l, r) =>
    l.serial.localeCompare(r.serial)
  );

  let sums = {
    amperage: 0,
    cellCount: 0,
    cellTemperatures: <Array<number>>[],
    cellVoltages: <Array<number>>[],
    cycleNumber: 0,
    energy: 0,
    energyCapacity: 0,
    energyRemaining: 0,
    manufacturerName: <Array<string>>[],
    model: <Array<string>>[],
    power: 0,
    serial: <Array<string>>[],
    timeToEmpty: 0,
    timeToFull: 0,
    voltage: 0,
  };
  for (const module of sortedModules) {
    sums = {
      amperage: sums.amperage + module.amperage,
      cellCount: sums.cellCount + module.cellCount,
      cellTemperatures: [...sums.cellTemperatures, ...module.cellTemperatures],
      cellVoltages: [...sums.cellVoltages, ...module.cellVoltages],
      cycleNumber: sums.cycleNumber + module.cycleNumber,
      energy: sums.energy + module.energy,
      energyCapacity: sums.energyCapacity + module.energyCapacity,
      energyRemaining: sums.energyRemaining + module.energyRemaining,
      manufacturerName: [...sums.manufacturerName, module.manufacturerName],
      model: [...sums.model, module.model],
      power: sums.power + module.power,
      serial: [...sums.serial, module.serial],
      timeToEmpty: sums.timeToEmpty + module.timeToEmpty,
      timeToFull: sums.timeToFull + module.timeToFull,
      voltage: sums.voltage + module.voltage,
    };
  }

  return <const>{
    battery: {
      ...sums,
      cycleNumber: sums.cycleNumber / modules.length,
      energy: sums.energy / modules.length,
      manufacturerName: uniqueStrings(sums.manufacturerName).join(","),
      model: uniqueStrings(sums.model).join(","),
      power: sums.power / modules.length,
      serial: `${modules.length.toString().padStart(2, "0")}x${Math.abs(
        CRC32.str(uniqueStrings(sums.serial).join("."))
      )}`,
      timeToEmpty: sums.timeToEmpty / modules.length,
      timeToFull: sums.timeToFull / modules.length,
      voltage: sums.voltage / modules.length,
    },
    modules,
  };
};
type Battery = Awaited<ReturnType<typeof batteryOf>>;

const publishBatteryConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  battery: Battery
) => {
  for (const [property, props] of Object.entries(
    configsForCellCount(battery.battery.cellCount)
  )) {
    const topic = `homeassistant/sensor/battery_${battery.battery.serial}_${property}/config`;
    const payload = JSON.stringify(
      {
        ...props,
        device: {
          identifiers: [battery.battery.serial],
          manufacturer: battery.battery.manufacturerName,
          model: battery.battery.model,
          name: `Battery ${battery.battery.serial}`,
        },
        name: `Battery ${battery.battery.serial} ${props.name}`,
        object_id: `battery_${battery.battery.serial}_${property}`,
        state_topic: `battery/battery/${battery.battery.serial}/${property}`,
        unique_id: `battery_${battery.battery.serial}_${property}`,
      },
      undefined,
      4
    );

    log.info(`publishing retained: ${topic}: '${payload}'`);
    await mqttConn.publish(topic, payload, { retain: true });
  }
};
const publishModuleConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  module: Module
) => {
  for (const [property, props] of Object.entries(
    configsForCellCount(module.cellCount)
  )) {
    const topic = `homeassistant/sensor/batterymodule_${module.serial}_${property}/config`;
    const payload = JSON.stringify(
      {
        ...props,
        device: {
          identifiers: [module.serial],
          manufacturer: module.manufacturerName,
          model: module.model,
          name: `Battery Module ${module.serial}`,
        },
        name: `Battery Module ${module.serial} ${props.name}`,
        object_id: `batterymodule_${module.serial}_${property}`,
        state_topic: `battery/modules/${module.serial}/${property}`,
        unique_id: `batterymodule_${module.serial}_${property}`,
      },
      undefined,
      4
    );

    log.info(`publishing retained: ${topic}: ${payload}`);
    await mqttConn.publish(topic, payload, { retain: true });
  }
};

const publishBatteryStates = async (
  mqttConn: Mqtt.AsyncMqttClient,
  battery: Battery
) => {
  const dict = {
    amperage: battery.battery.amperage.toFixed(2),
    cell_count: battery.battery.cellCount,
    "cell_%%_temperature": battery.battery.cellTemperatures.map(
      (cellTemperature) => cellTemperature.toFixed(1)
    ),
    "cell_%%_voltage": battery.battery.cellVoltages.map((cellVoltage) =>
      cellVoltage.toFixed(1)
    ),
    cycle_number: battery.battery.cycleNumber.toFixed(2),
    energy: battery.battery.energy.toFixed(2),
    energy_capacity: (
      battery.battery.energyCapacity * battery.battery.voltage
    ).toFixed(3),
    energy_remaining: (
      battery.battery.energyRemaining * battery.battery.voltage
    ).toFixed(3),
    power: battery.battery.power.toFixed(2),
    time_to_empty:
      battery.battery.timeToEmpty !== 0
        ? battery.battery.timeToEmpty.toFixed(2)
        : "unavailable",
    time_to_full:
      battery.battery.timeToFull !== 0
        ? battery.battery.timeToFull.toFixed(2)
        : "unavailable",
    voltage: battery.battery.voltage.toFixed(1),
  };

  for (const [property, value] of Object.entries(statesForDict(dict))) {
    const topic = `battery/battery/${battery.battery.serial}/${property}`;
    const payload = `${value}`;

    log.info(`publishing: ${topic}: ${payload}`);
    await mqttConn.publish(topic, payload);
  }
};
const publishModuleStates = async (
  mqttConn: Mqtt.AsyncMqttClient,
  module: Module
) => {
  const dict = {
    amperage: module.amperage.toFixed(2),
    cell_count: module.cellCount,
    "cell_%%_temperature": module.cellTemperatures.map((cellTemperature) =>
      cellTemperature.toFixed(1)
    ),
    "cell_%%_voltage": module.cellVoltages.map((cellVoltage) =>
      cellVoltage.toFixed(1)
    ),
    cycle_number: module.cycleNumber,
    energy: module.energy.toFixed(2),
    energy_capacity: (module.energyCapacity * module.voltage).toFixed(2),
    energy_remaining: (module.energyRemaining * module.voltage).toFixed(2),
    power: module.power.toFixed(3),
    time_to_empty: module.timeToEmpty?.toFixed(2) ?? "unavailable",
    time_to_full: module.timeToFull?.toFixed(2) ?? "unavailable",
    voltage: module.voltage.toFixed(1),
  };

  for (const [property, value] of Object.entries(statesForDict(dict))) {
    const topic = `battery/modules/${module.serial}/${property}`;
    const payload = `${value}`;

    log.info(`publishing: ${topic}: ${payload}`);
    await mqttConn.publish(topic, payload);
  }
};

runForever({
  setup: async () => {
    log.info(`Connecting to MQTT`);
    const mqttConn = await Mqtt.connectAsync("tcp://debian.lan:1883");
    log.info(`Connected to MQTT`);

    log.info(`Connecting to MODBUS`);
    const modbusConn = new Modbus();
    await modbusConn.connectTCP("debian.lan", {
      port: 502,
    });
    log.info(`Connected to MODBUS`);

    let modules: Array<Module> = [];
    for (const server of servers) {
      log.info(`configuring HA for server ${server}`);

      const serverData = await queryServer(modbusConn, server);

      const module = moduleOf(serverData);
      await publishModuleConfigs(mqttConn, module);

      modules = [...modules, module];
    }

    const battery = batteryOf(modules);
    await publishBatteryConfigs(mqttConn, battery);

    return <const>{ modbusConn, mqttConn };
  },

  loop: async ({ modbusConn, mqttConn }) => {
    let modules: Array<Module> = [];
    for (const server of servers) {
      const serverData = await queryServer(modbusConn, server);
      const module = moduleOf(serverData);

      await publishModuleStates(mqttConn, module);
      modules = [...modules, module];
    }

    const battery = batteryOf(modules);
    await publishBatteryStates(mqttConn, battery);
  },

  teardown: async ({ modbusConn, mqttConn }) => {},
});

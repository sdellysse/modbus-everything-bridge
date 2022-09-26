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
  "%MODULE%_cell_count": {
    entity_category: "diagnostic",
    name: "%MODULE% Cell Count",
    state_class: "measurement",
  },
  "%MODULE%_%CELL%_temperature": {
    device_class: "temperature",
    entity_category: "diagnostic",
    name: "%MODULE% %CELL% Temperature",
    state_class: "measurement",
    unit_of_measurement: "°C",
  },
  "%MODULE%_%CELL%_voltage": {
    device_class: "voltage",
    entity_category: "diagnostic",
    name: "%MODULE% %CELL% Voltage",
    state_class: "measurement",
    unit_of_measurement: "V",
  },
  "%MODULE%_cycle_number": {
    entity_category: "diagnostic",
    name: "%MODULE% Cycle Number",
    state_class: "measurement",
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
          cellVoltages: [
            0.1 * numberAt(5001, 1, "unsigned"),
            0.1 * numberAt(5002, 1, "unsigned"),
            0.1 * numberAt(5003, 1, "unsigned"),
            0.1 * numberAt(5004, 1, "unsigned"),
            0.1 * numberAt(5005, 1, "unsigned"),
            0.1 * numberAt(5006, 1, "unsigned"),
            0.1 * numberAt(5007, 1, "unsigned"),
            0.1 * numberAt(5008, 1, "unsigned"),
            0.1 * numberAt(5009, 1, "unsigned"),
            0.1 * numberAt(5010, 1, "unsigned"),
            0.1 * numberAt(5011, 1, "unsigned"),
            0.1 * numberAt(5012, 1, "unsigned"),
            0.1 * numberAt(5013, 1, "unsigned"),
            0.1 * numberAt(5014, 1, "unsigned"),
            0.1 * numberAt(5015, 1, "unsigned"),
            0.1 * numberAt(5016, 1, "unsigned"),
          ],
          cellTemperatureCount: 1.0 * numberAt(5017, 1, "unsigned"),
          cellTemperatures: [
            0.1 * numberAt(5018, 1, "signed"),
            0.1 * numberAt(5019, 1, "signed"),
            0.1 * numberAt(5020, 1, "signed"),
            0.1 * numberAt(5021, 1, "signed"),
            0.1 * numberAt(5022, 1, "signed"),
            0.1 * numberAt(5023, 1, "signed"),
            0.1 * numberAt(5024, 1, "signed"),
            0.1 * numberAt(5025, 1, "signed"),
            0.1 * numberAt(5026, 1, "signed"),
            0.1 * numberAt(5027, 1, "signed"),
            0.1 * numberAt(5028, 1, "signed"),
            0.1 * numberAt(5029, 1, "signed"),
            0.1 * numberAt(5030, 1, "signed"),
            0.1 * numberAt(5031, 1, "signed"),
            0.1 * numberAt(5032, 1, "signed"),
            0.1 * numberAt(5033, 1, "signed"),
          ],
        }
    )),

    ...(await queryModbus(
      modbusConn,
      server,
      5035,
      5053,
      ({ numberAt }) =>
        <const>{
          bmsTemperature: 0.1 * numberAt(5035, 1, "signed"),
          envTemperatureCount: 1.0 * numberAt(5036, 1, "unsigned"),
          envTemperatures: [
            0.1 * numberAt(5037, 1, "signed"),
            0.1 * numberAt(5038, 1, "signed"),
          ],
          heaterTemperatureCount: 1.0 * numberAt(5039, 1, "unsigned"),
          heaterTemperatures: [
            0.1 * numberAt(5040, 1, "signed"),
            0.1 * numberAt(5041, 1, "signed"),
          ],
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
  return {
    ...data,
    cellCount: data.cellVoltageCount,
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
    energy: 0,
    energyCapacity: 0,
    energyRemaining: 0,
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
      energy: sums.energy + module.energy,
      energyCapacity: sums.energyCapacity + module.energyCapacity,
      energyRemaining: sums.energyRemaining + module.energyRemaining,
      model: [...sums.model, module.model],
      power: sums.power + module.power,
      serial: [...sums.serial, module.serial],
      timeToEmpty: sums.timeToEmpty + module.timeToEmpty,
      timeToFull: sums.timeToFull + module.timeToFull,
      voltage: sums.voltage + module.voltage,
    };
  }

  return {
    ...sums,
    energy: sums.energy / modules.length,
    model: `${modules.length.toString().padStart(2, "0")}x${Math.abs(
      CRC32.str(uniqueStrings(sums.model).join("."))
    )}`,
    modules,
    power: sums.power / modules.length,
    serial: `${modules.length.toString().padStart(2, "0")}x${Math.abs(
      CRC32.str(uniqueStrings(sums.serial).join("."))
    )}`,
    timeToEmpty: sums.timeToEmpty / modules.length,
    timeToFull: sums.timeToFull / modules.length,
    voltage: sums.voltage / modules.length,
  };
};
type Battery = Awaited<ReturnType<typeof batteryOf>>;

const publishBatteryConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  battery: Battery
) => {
  const configs: Record<string, { name: string } & Record<string, unknown>> =
    {};
  configs[`amperage`] = {
    device_class: "current",
    name: "Amperage",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`energy`] = {
    device_class: "battery",
    name: "Energy",
    state_class: "measurement",
    unit_of_measurement: "%",
  };
  configs[`energy_capacity`] = {
    device_class: "energy",
    name: "Energy Capacity",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`energy_remaining`] = {
    device_class: "energy",
    name: "Energy Remaining",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`power`] = {
    device_class: "power",
    name: "Power",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`time_to_empty`] = {
    device_class: "duration",
    name: "Time To Empty",
    unit_of_measurement: "h",
  };
  configs[`time_to_full`] = {
    device_class: "duration",
    name: "Time To Full",
    unit_of_measurement: "h",
  };
  configs[`voltage`] = {
    device_class: "voltage",
    name: "Voltage",
    state_class: "measurement",
    unit_of_measurement: "V",
  };
  for (let mi = 0; mi < battery.modules.length; mi++) {
    const module = battery.modules[mi];
    const moduleNumberString = (mi + 1).toString().padStart(2, "0");

    configs[`module_${moduleNumberString}_cell_count`] = {
      entity_category: "diagnostic",
      name: `Module ${moduleNumberString} Cell Count`,
      state_class: "measurement",
    };
    configs[`module_${moduleNumberString}_cycle_number`] = {
      entity_category: "diagnostic",
      name: `Module ${moduleNumberString} Cycle Number`,
      state_class: "measurement",
    };

    for (let ci = 0; ci < module.cellCount; ci++) {
      const cellNumberString = (ci + 1).toString().padStart(2, "0");

      configs[
        `module_${moduleNumberString}_cell_${cellNumberString}_temperature`
      ] = {
        device_class: "temperature",
        entity_category: "diagnostic",
        name: `Module ${moduleNumberString} Cell ${cellNumberString} Temperature`,
        state_class: "measurement",
        unit_of_measurement: "°C",
      };
      configs[`module_${moduleNumberString}_cell_${cellNumberString}_voltage`] =
        {
          device_class: "voltage",
          entity_category: "diagnostic",
          name: `Module ${moduleNumberString} Cell ${cellNumberString} Voltage`,
          state_class: "measurement",
          unit_of_measurement: "V",
        };
    }
  }

  for (const [property, props] of Object.entries(configs)) {
    const topic = `homeassistant/sensor/battery_${battery.serial}_${property}/config`;
    const payload = JSON.stringify(
      {
        ...props,
        device: {
          identifiers: [battery.serial],
          model: battery.model,
          name: `Battery ${battery.serial}`,
        },
        name: `Battery ${battery.serial} ${props.name}`,
        object_id: `battery_${battery.serial}_${property}`,
        state_topic: `battery/battery/${battery.serial}/${property}`,
        unique_id: `battery_${battery.serial}_${property}`,
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
  const configs: Record<string, { name: string } & Record<string, unknown>> =
    {};
  configs[`amperage`] = {
    device_class: "current",
    name: "Amperage",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`cell_count`] = {
    entity_category: "diagnostic",
    name: `Cell Count`,
    state_class: "measurement",
  };
  configs[`cycle_number`] = {
    entity_category: "diagnostic",
    name: `Cycle Number`,
    state_class: "measurement",
  };
  configs[`energy`] = {
    device_class: "battery",
    name: "Energy",
    state_class: "measurement",
    unit_of_measurement: "%",
  };
  configs[`energy_capacity`] = {
    device_class: "energy",
    name: "Energy Capacity",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`energy_remaining`] = {
    device_class: "energy",
    name: "Energy Remaining",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`power`] = {
    device_class: "power",
    name: "Power",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`time_to_empty`] = {
    device_class: "duration",
    name: "Time To Empty",
    unit_of_measurement: "h",
  };
  configs[`time_to_full`] = {
    device_class: "duration",
    name: "Time To Full",
    unit_of_measurement: "h",
  };
  configs[`voltage`] = {
    device_class: "voltage",
    name: "Voltage",
    state_class: "measurement",
    unit_of_measurement: "V",
  };

  for (let ci = 0; ci < module.cellCount; ci++) {
    const cellNumberString = (ci + 1).toString().padStart(2, "0");

    configs[`cell_${cellNumberString}_temperature`] = {
      device_class: "temperature",
      entity_category: "diagnostic",
      name: `Cell ${cellNumberString} Temperature`,
      state_class: "measurement",
      unit_of_measurement: "°C",
    };
    configs[`cell_${cellNumberString}_voltage`] = {
      device_class: "voltage",
      entity_category: "diagnostic",
      name: `Cell ${cellNumberString} Voltage`,
      state_class: "measurement",
      unit_of_measurement: "V",
    };
  }
  for (const [property, props] of Object.entries(configs)) {
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
  const stateMap: Record<string, unknown> = {};

  stateMap[`amperage`] = battery.amperage.toFixed(2);
  stateMap[`energy`] = battery.energy.toFixed(2);
  stateMap[`energy_capacity`] = (
    battery.energyCapacity * battery.voltage
  ).toFixed(3);
  stateMap[`energy_remaining`] = (
    battery.energyRemaining * battery.voltage
  ).toFixed(3);
  stateMap[`model`] = battery.model;
  stateMap[`power`] = battery.power.toFixed(2);
  stateMap[`serial`] = battery.serial;
  stateMap[`time_to_empty`] =
    battery.timeToEmpty !== 0 ? battery.timeToEmpty.toFixed(2) : "unavailable";
  stateMap[`time_to_full`] =
    battery.timeToFull !== 0 ? battery.timeToFull.toFixed(2) : "unavailable";
  stateMap[`voltage`] = battery.voltage.toFixed(1);

  for (let mi = 0; mi < battery.modules.length; mi++) {
    const module = battery.modules[mi];

    const moduleNumberString = (mi + 1).toString().padStart(2, "0");
    stateMap[`module_${moduleNumberString}_cell_count`] = module.cellCount;
    stateMap[`module_${moduleNumberString}_cycle_number`] = module.cycleNumber;

    for (let ci = 0; ci < module.cellCount; ci++) {
      const cellNumberString = (ci + 1).toString().padStart(2, "0");
      stateMap[
        `module_${moduleNumberString}_cell_${cellNumberString}_temperature`
      ] = module.cellTemperatures[ci].toFixed(1);
      stateMap[
        `module_${moduleNumberString}_cell_${cellNumberString}_voltage`
      ] = module.cellVoltages[ci].toFixed(1);
    }
  }

  for (const [key, value] of Object.entries(stateMap)) {
    const topic = `battery/battery/${battery.serial}/${key}`;
    const payload = `${value}`;

    log.info(`publishing: ${topic}: ${payload}`);
    await mqttConn.publish(topic, payload);
  }
};
const publishModuleStates = async (
  mqttConn: Mqtt.AsyncMqttClient,
  module: Module
) => {
  const stateMap: Record<string, unknown> = {};

  stateMap[`amperage`] = module.amperage.toFixed(2);
  stateMap[`cell_count`] = module.cellCount;
  stateMap[`cycle_number`] = module.cycleNumber;
  stateMap[`energy`] = module.energy.toFixed(2);
  stateMap[`energy_capacity`] = (
    module.energyCapacity * module.voltage
  ).toFixed(2);
  stateMap[`energy_remaining`] = (
    module.energyRemaining * module.voltage
  ).toFixed(2);
  stateMap[`manufacturer_name`] = module.manufacturerName;
  stateMap[`model`] = module.model;
  stateMap[`power`] = module.power.toFixed(3);
  stateMap[`serial`] = module.serial;
  stateMap[`time_to_empty`] = module.timeToEmpty?.toFixed(2) ?? "unavailable";
  stateMap[`time_to_full`] = module.timeToFull?.toFixed(2) ?? "unavailable";
  stateMap[`voltage`] = module.voltage.toFixed(1);

  for (let ci = 0; ci < module.cellCount; ci++) {
    const cellNumberString = (ci + 1).toString().padStart(2, "0");
    stateMap[`cell_${cellNumberString}_temperature`] =
      module.cellTemperatures[ci].toFixed(1);
    stateMap[`cell_${cellNumberString}_voltage`] =
      module.cellVoltages[ci].toFixed(1);
  }

  for (const [key, value] of Object.entries(stateMap)) {
    const topic = `battery/modules/${module.serial}/${key}`;
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

    log.info(`begin configuring modules`);
    let modules: Array<Module> = [];
    for (const server of servers) {
      log.info(`configuring HA for server ${server}`);

      const serverData = await queryServer(modbusConn, server);

      const module = moduleOf(serverData);
      await publishModuleConfigs(mqttConn, module);

      modules = [...modules, module];
    }
    log.info(`finished configuring modules`);

    log.info(`begin configuring battery`);
    const battery = batteryOf(modules);
    await publishBatteryConfigs(mqttConn, battery);
    log.info(`finished configuring battery`);

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

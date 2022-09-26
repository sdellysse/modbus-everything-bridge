import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import CRC32 from "crc-32";
import { log, queryModbus, runForever, uniqueStrings } from "./utils";

process.env.MODBUSTCP_HOST = "debian.lan";
process.env.MODBUSTCP_PORT = "502";
process.env.MQTT_URL = "tcp://debian.lan:1883";
process.env.SERVERS = "[48, 49, 50, 51]";

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
          chargeRemaining: 0.001 * numberAt(5044, 2, "unsigned"),
          chargeCapacity: 0.001 * numberAt(5046, 2, "unsigned"),
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
  const amps = data.amperage;
  const volts = data.voltage;
  const watts = volts * amps;
  const chargeCapacity = data.chargeCapacity * volts;
  const chargeRemaining = data.chargeRemaining * volts;

  return {
    ...data,
    amps,
    amps_in: amps > 0 ? amps : 0,
    amps_out: amps < 0 ? Math.abs(amps) : 0,
    cellCount: data.cellVoltageCount,
    chargeCapacity,
    chargePercentage: 100.0 * (chargeRemaining / chargeCapacity),
    chargeRemaining,
    serial: data.serial.replace(/[^A-Za-z0-9]/g, ""),
    timeToFull: watts > 0 ? (chargeCapacity - chargeRemaining) / watts : 0,
    timeToEmpty: watts < 0 ? Math.abs(chargeRemaining / watts) : 0,
    volts,
    watts,
    watts_in: watts > 0 ? watts : 0,
    watts_out: watts < 0 ? Math.abs(watts) : 0,
  };
};
type Module = Awaited<ReturnType<typeof moduleOf>>;

const batteryOf = (modules: Array<Module>) => {
  const sortedModules = [...modules].sort((l, r) =>
    l.serial.localeCompare(r.serial)
  );

  let sums = {
    amps: 0,
    amps_in: 0,
    amps_out: 0,
    chargeCapacity: 0,
    chargePercentage: 0,
    chargeRemaining: 0,
    model: <Array<string>>[],
    serial: <Array<string>>[],
    timeToEmpty: 0,
    timeToFull: 0,
    volts: 0,
    watts: 0,
    watts_in: 0,
    watts_out: 0,
  };
  for (const module of sortedModules) {
    sums = {
      amps: sums.amps + module.amps,
      amps_in: sums.amps_in + module.amps_in,
      amps_out: sums.amps_out + module.amps_out,
      chargePercentage: sums.chargePercentage + module.chargePercentage,
      chargeCapacity: sums.chargeCapacity + module.chargeCapacity,
      chargeRemaining: sums.chargeRemaining + module.chargeRemaining,
      model: [...sums.model, module.model],
      serial: [...sums.serial, module.serial],
      timeToEmpty: sums.timeToEmpty + module.timeToEmpty,
      timeToFull: sums.timeToFull + module.timeToFull,
      volts: sums.volts + module.volts,
      watts: sums.watts + module.watts,
      watts_in: sums.watts_in + module.watts_in,
      watts_out: sums.watts_out + module.watts_out,
    };
  }

  return {
    ...sums,
    chargePercentage: sums.chargePercentage / modules.length,
    model: `${modules.length.toString().padStart(2, "0")}x${Math.abs(
      CRC32.str(uniqueStrings(sums.model).join("."))
    )}`,
    modules,
    serial: `${modules.length.toString().padStart(2, "0")}x${Math.abs(
      CRC32.str(uniqueStrings(sums.serial).join("."))
    )}`,
    timeToEmpty: sums.timeToEmpty / modules.length,
    timeToFull: sums.timeToFull / modules.length,
    volts: sums.volts / modules.length,
  };
};
type Battery = Awaited<ReturnType<typeof batteryOf>>;

const publishBatteryConfigs = async (
  mqttConn: Mqtt.AsyncMqttClient,
  battery: Battery
) => {
  const configs: Record<string, { name: string } & Record<string, unknown>> =
    {};
  configs[`amps`] = {
    device_class: "current",
    name: "Amps",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`amps_in`] = {
    device_class: "current",
    name: "Amps In",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`amps_out`] = {
    device_class: "current",
    name: "Amps Out",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`charge_capacity`] = {
    device_class: "energy",
    name: "Charge Capacity",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`charge_percentage`] = {
    device_class: "battery",
    name: "Charge Percentage",
    state_class: "measurement",
    unit_of_measurement: "%",
  };
  configs[`charge_remaining`] = {
    device_class: "energy",
    name: "Charge Remaining",
    state_class: "measurement",
    unit_of_measurement: "Wh",
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
  configs[`volts`] = {
    device_class: "voltage",
    name: "Volts",
    state_class: "measurement",
    unit_of_measurement: "V",
  };
  configs[`watts`] = {
    device_class: "current",
    name: "Watts",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`watts_in`] = {
    device_class: "current",
    name: "Watts In",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`watts_out`] = {
    device_class: "current",
    name: "Watts Out",
    state_class: "measurement",
    unit_of_measurement: "W",
  };

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
  configs[`amps`] = {
    device_class: "current",
    name: "Amps",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`amps_in`] = {
    device_class: "current",
    name: "Amps In",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`amps_out`] = {
    device_class: "current",
    name: "Amps Out",
    state_class: "measurement",
    unit_of_measurement: "A",
  };
  configs[`cell_count`] = {
    entity_category: "diagnostic",
    name: `Cell Count`,
    state_class: "measurement",
  };
  configs[`charge_capacity`] = {
    device_class: "energy",
    name: "Charge Capacity",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`charge_percentage`] = {
    device_class: "battery",
    name: "Charge Percentage",
    state_class: "measurement",
    unit_of_measurement: "%",
  };
  configs[`charge_remaining`] = {
    device_class: "energy",
    name: "Charge Remaining",
    state_class: "measurement",
    unit_of_measurement: "Wh",
  };
  configs[`cycle_number`] = {
    entity_category: "diagnostic",
    name: `Cycle Number`,
    state_class: "measurement",
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
  configs[`volts`] = {
    device_class: "voltage",
    name: "Volts",
    state_class: "measurement",
    unit_of_measurement: "V",
  };
  configs[`watts`] = {
    device_class: "current",
    name: "Watts",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`watts_in`] = {
    device_class: "current",
    name: "Watts In",
    state_class: "measurement",
    unit_of_measurement: "W",
  };
  configs[`watts_out`] = {
    device_class: "current",
    name: "Watts Out",
    state_class: "measurement",
    unit_of_measurement: "W",
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
    configs[`cell_${cellNumberString}_volts`] = {
      device_class: "voltage",
      entity_category: "diagnostic",
      name: `Cell ${cellNumberString} Volts`,
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

  stateMap[`amps`] = battery.amps.toFixed(2);
  stateMap[`amps_in`] = battery.amps_in.toFixed(2);
  stateMap[`amps_out`] = battery.amps_out.toFixed(2);
  stateMap[`charge_capacity`] = battery.chargeCapacity.toFixed(3);
  stateMap[`charge_percentage`] = battery.chargePercentage.toFixed(2);
  stateMap[`charge_remaining`] = battery.chargeRemaining.toFixed(3);
  stateMap[`model`] = battery.model;
  stateMap[`serial`] = battery.serial;
  stateMap[`time_to_empty`] =
    battery.timeToEmpty !== 0 ? battery.timeToEmpty.toFixed(2) : "unavailable";
  stateMap[`time_to_full`] =
    battery.timeToFull !== 0 ? battery.timeToFull.toFixed(2) : "unavailable";
  stateMap[`volts`] = battery.volts.toFixed(1);
  stateMap[`watts`] = battery.watts.toFixed(2);
  stateMap[`watts_in`] = battery.watts_in.toFixed(2);
  stateMap[`watts_out`] = battery.watts_out.toFixed(2);

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

  stateMap[`amps`] = module.amps.toFixed(2);
  stateMap[`amps_in`] = module.amps_in.toFixed(2);
  stateMap[`amps_out`] = module.amps_out.toFixed(2);
  stateMap[`cell_count`] = module.cellCount;
  stateMap[`charge_capacity`] = module.chargeCapacity.toFixed(2);
  stateMap[`charge_percentage`] = module.chargePercentage.toFixed(2);
  stateMap[`charge_remaining`] = module.chargeRemaining.toFixed(2);
  stateMap[`cycle_number`] = module.cycleNumber;
  stateMap[`manufacturer_name`] = module.manufacturerName;
  stateMap[`model`] = module.model;
  stateMap[`serial`] = module.serial;
  stateMap[`time_to_empty`] =
    module.timeToEmpty !== 0 ? module.timeToEmpty.toFixed(2) : "unavailable";
  stateMap[`time_to_full`] =
    module.timeToFull !== 0 ? module.timeToFull.toFixed(2) : "unavailable";
  stateMap[`volts`] = module.volts.toFixed(1);
  stateMap[`watts`] = module.watts.toFixed(2);
  stateMap[`watts_in`] = module.watts_in.toFixed(2);
  stateMap[`watts_out`] = module.watts_out.toFixed(2);

  for (let ci = 0; ci < module.cellCount; ci++) {
    const cellNumberString = (ci + 1).toString().padStart(2, "0");
    stateMap[`cell_${cellNumberString}_temperature`] =
      module.cellTemperatures[ci].toFixed(1);
    stateMap[`cell_${cellNumberString}_volts`] =
      module.cellVoltages[ci].toFixed(1);
  }

  for (const [key, value] of Object.entries(stateMap)) {
    const topic = `battery/modules/${module.serial}/${key}`;
    const payload = `${value}`;

    log.info(`publishing: ${topic}: ${payload}`);
    await mqttConn.publish(topic, payload);
  }
};

(async () => {
  const servers = JSON.parse(process.env.SERVERS!);

  log.info(`begin setup`);
  log.info(`Connecting to MQTT`);
  const mqttConn = await Mqtt.connectAsync(process.env.MQTT_URL!);
  log.info(`Connected to MQTT`);

  log.info(`Connecting to MODBUS`);
  const modbusConn = new Modbus();
  await modbusConn.connectTCP(process.env.MODBUSTCP_HOST!, {
    port: parseInt(process.env.MODBUSTCP_PORT!, 10),
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

  log.info(`begin main loop`);
  for (;;) {
    let modules: Array<Module> = [];
    for (const server of servers) {
      const serverData = await queryServer(modbusConn, server);
      const module = moduleOf(serverData);

      await publishModuleStates(mqttConn, module);
      modules = [...modules, module];
    }

    const battery = batteryOf(modules);
    await publishBatteryStates(mqttConn, battery);
  }
})();

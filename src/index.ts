import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import CRC32 from "crc-32";
import { log, queryModbus, uniqueStrings } from "./utils";

process.env.MODBUSTCP_HOST = "debian.lan";
process.env.MODBUSTCP_PORT = "502";
process.env.MQTT_URL = "tcp://debian.lan:1883";
process.env.MQTT_PREFIX = "battery2mqtt";
process.env.SERVER_COUNT = "4";
process.env.SERVER_1_MODBUS_ADDRESS = "48";
process.env.SERVER_2_MODBUS_ADDRESS = "49";
process.env.SERVER_3_MODBUS_ADDRESS = "50";
process.env.SERVER_4_MODBUS_ADDRESS = "51";
process.env.HOME_ASSISTANT_DISCOVERY_ENABLE = "true";
process.env.HOME_ASSISTANT_DISCOVERY_PREFIX = "homeassistant";

const queryModbusServer = async (modbusConn: Modbus, server: number) => {
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
type ServerData = Awaited<ReturnType<typeof queryModbusServer>>;

const moduleOf = (data: ServerData) => {
  const amps = data.amperage;
  const volts = data.voltage;
  const watts = volts * amps;
  const chargeCapacity = data.chargeCapacity * volts;
  const chargeRemaining = data.chargeRemaining * volts;

  return {
    amps,
    ampsIn: amps > 0 ? amps : 0,
    ampsOut: amps < 0 ? Math.abs(amps) : 0,
    cellCount: data.cellVoltageCount,
    cellTemperatures: data.cellTemperatures,
    cellVoltages: data.cellVoltages,
    chargeCapacity,
    chargePercentage: 100.0 * (chargeRemaining / chargeCapacity),
    chargeRemaining,
    cycleNumber: data.cycleNumber,
    manufacturerName: data.manufacturerName,
    model: data.model,
    serial: data.serial.replace(/[^A-Za-z0-9]/g, ""),
    timeToFull: watts > 0 ? (chargeCapacity - chargeRemaining) / watts : 0,
    timeToEmpty: watts < 0 ? Math.abs(chargeRemaining / watts) : 0,
    volts,
    watts,
    wattsIn: watts > 0 ? watts : 0,
    wattsOut: watts < 0 ? Math.abs(watts) : 0,
  };
};
type Module = Awaited<ReturnType<typeof moduleOf>>;

const batteryOf = (modules: Array<Module>) => {
  const sortedModules = [...modules].sort((l, r) =>
    l.serial.localeCompare(r.serial)
  );

  let sums = {
    amps: 0,
    ampsIn: 0,
    ampsOut: 0,
    chargeCapacity: 0,
    chargePercentage: 0,
    chargeRemaining: 0,
    model: <Array<string>>[],
    serial: <Array<string>>[],
    timeToEmpty: 0,
    timeToFull: 0,
    volts: 0,
    watts: 0,
    wattsIn: 0,
    wattsOut: 0,
  };
  for (const module of sortedModules) {
    sums = {
      amps: sums.amps + module.amps,
      ampsIn: sums.ampsIn + module.ampsIn,
      ampsOut: sums.ampsOut + module.ampsOut,
      chargePercentage: sums.chargePercentage + module.chargePercentage,
      chargeCapacity: sums.chargeCapacity + module.chargeCapacity,
      chargeRemaining: sums.chargeRemaining + module.chargeRemaining,
      model: [...sums.model, module.model],
      serial: [...sums.serial, module.serial],
      timeToEmpty: sums.timeToEmpty + module.timeToEmpty,
      timeToFull: sums.timeToFull + module.timeToFull,
      volts: sums.volts + module.volts,
      watts: sums.watts + module.watts,
      wattsIn: sums.wattsIn + module.wattsIn,
      wattsOut: sums.wattsOut + module.wattsOut,
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

type HomeAssistantConfigMapping = {
  unique_id: string;
  state_topic: string;
  name: string;
  config: Record<string, unknown>;
};
const homeAssistantConfigFns = {
  publishBattery: async (mqttConn: Mqtt.AsyncMqttClient, battery: Battery) => {
    let mappings: Array<HomeAssistantConfigMapping> = [];
    mappings = [
      ...mappings,
      {
        state_topic: "amps",
        unique_id: "amps",
        name: "Amps",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: "amps_in",
        state_topic: "amps_in",
        name: "Amps In",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `amps_out`,
        state_topic: "amps_out",
        name: "Amps Out",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_capacity`,
        state_topic: "charge_capacity",
        name: "Charge Capacity",
        config: {
          device_class: "energy",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_percentage`,
        state_topic: "charge_percentage",
        name: "Charge Percentage",
        config: {
          device_class: "battery",
          state_class: "measurement",
          unit_of_measurement: "%",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_remaining`,
        state_topic: "charge_remaining",
        name: "Charge Remaining",
        config: {
          device_class: "energy",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `time_to_empty`,
        state_topic: `time_to_empty`,
        name: "Time To Empty",
        config: {
          device_class: "duration",
          unit_of_measurement: "h",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `time_to_full`,
        state_topic: `time_to_full`,
        name: "Time To Full",
        config: {
          device_class: "duration",
          unit_of_measurement: "h",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `volts`,
        state_topic: `volts`,
        name: "Volts",
        config: {
          device_class: "voltage",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: "watts",
        state_topic: `watts`,
        name: "Watts",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `watts_in`,
        state_topic: `watts_in`,
        name: "Watts In",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `watts_out`,
        state_topic: `watts_out`,
        name: "Watts Out",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];

    for (const mapping of mappings) {
      const haPrefix = process.env.HOME_ASSISTANT_DISCOVERY_PREFIX!;
      const topic = `${haPrefix}/sensor/battery_${battery.serial}_${mapping.unique_id}/config`;

      const mqttPrefix = process.env.MQTT_PREFIX!;
      const payload = JSON.stringify(
        {
          ...mapping.config,
          device: {
            identifiers: [battery.serial],
            model: battery.model,
            name: `Battery ${battery.serial}`,
          },
          name: `Battery ${battery.serial} ${mapping.name}`,
          object_id: `battery_${battery.serial}_${mapping.unique_id}`,
          state_topic: `${mqttPrefix}/battery/${battery.serial}/${mapping.state_topic}`,
          unique_id: `battery_${battery.serial}_${mapping.unique_id}`,
        },
        undefined,
        4
      );

      log.info(`publishing retained: ${topic}: '${payload}'`);
      await mqttConn.publish(topic, payload, { retain: true });
    }
  },

  publishModule: async (mqttConn: Mqtt.AsyncMqttClient, module: Module) => {
    let mappings: Array<HomeAssistantConfigMapping> = [];

    mappings = [
      ...mappings,
      {
        state_topic: "amps",
        unique_id: "amps",
        name: "Amps",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: "amps_in",
        state_topic: "amps_in",
        name: "Amps In",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `amps_out`,
        state_topic: "amps_out",
        name: "Amps Out",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cell_count`,
        state_topic: "cell_count",
        name: `Cell Count`,
        config: {
          entity_category: "diagnostic",
          state_class: "measurement",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_capacity`,
        state_topic: "charge_capacity",
        name: "Charge Capacity",
        config: {
          device_class: "energy",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_percentage`,
        state_topic: "charge_percentage",
        name: "Charge Percentage",
        config: {
          device_class: "battery",
          state_class: "measurement",
          unit_of_measurement: "%",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge_remaining`,
        state_topic: "charge_remaining",
        name: "Charge Remaining",
        config: {
          device_class: "energy",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cycle_number`,
        state_topic: `cycle_number`,
        name: `Cycle Number`,
        config: {
          entity_category: "diagnostic",
          state_class: "measurement",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `time_to_empty`,
        state_topic: `time_to_empty`,
        name: "Time To Empty",
        config: {
          device_class: "duration",
          unit_of_measurement: "h",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `time_to_full`,
        state_topic: `time_to_full`,
        name: "Time To Full",
        config: {
          device_class: "duration",
          unit_of_measurement: "h",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `volts`,
        state_topic: `volts`,
        name: "Volts",
        config: {
          device_class: "voltage",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: "watts",
        state_topic: `watts`,
        name: "Watts",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `watts_in`,
        state_topic: `watts_in`,
        name: "Watts In",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `watts_out`,
        state_topic: `watts_out`,
        name: "Watts Out",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];

    for (let ci = 0; ci < module.cellCount; ci++) {
      const cellNumberString = (ci + 1).toString().padStart(2, "0");

      mappings = [
        ...mappings,
        {
          unique_id: `cell_${cellNumberString}_temperature`,
          state_topic: `cells/${cellNumberString}/temperature`,
          name: `Cell ${cellNumberString} Temperature`,
          config: {
            device_class: "temperature",
            entity_category: "diagnostic",
            state_class: "measurement",
            unit_of_measurement: "°C",
          },
        },
      ];
      mappings = [
        ...mappings,
        {
          unique_id: `cell_${cellNumberString}_volts`,
          state_topic: `cells/${cellNumberString}/volts`,
          name: `Cell ${cellNumberString} Volts`,
          config: {
            device_class: "voltage",
            entity_category: "diagnostic",
            state_class: "measurement",
            unit_of_measurement: "V",
          },
        },
      ];
    }
    for (const mapping of mappings) {
      const haPrefix = process.env.HOME_ASSISTANT_DISCOVERY_PREFIX!;
      const topic = `${haPrefix}/sensor/batterymodule_${module.serial}_${mapping.unique_id}/config`;

      const mqttPrefix = process.env.MQTT_PREFIX!;
      const payload = JSON.stringify(
        {
          ...mapping,
          device: {
            identifiers: [module.serial],
            manufacturer: module.manufacturerName,
            model: module.model,
            name: `Battery Module ${module.serial}`,
          },
          name: `Battery Module ${module.serial} ${mapping.name}`,
          object_id: `batterymodule_${module.serial}_${mapping.unique_id}`,
          state_topic: `${mqttPrefix}/modules/${module.serial}/${mapping.state_topic}`,
          unique_id: `batterymodule_${module.serial}_${mapping.unique_id}`,
        },
        undefined,
        4
      );

      log.info(`publishing retained: ${topic}: ${payload}`);
      await mqttConn.publish(topic, payload, { retain: true });
    }
  },
};

const stateFns = {
  publishBattery: async (mqttConn: Mqtt.AsyncMqttClient, battery: Battery) => {
    const stateMap: Record<string, unknown> = {};

    stateMap[`amps`] = battery.amps.toFixed(2);
    stateMap[`amps_in`] = battery.ampsIn.toFixed(2);
    stateMap[`amps_out`] = battery.ampsOut.toFixed(2);
    stateMap[`charge_capacity`] = battery.chargeCapacity.toFixed(3);
    stateMap[`charge_percentage`] = battery.chargePercentage.toFixed(2);
    stateMap[`charge_remaining`] = battery.chargeRemaining.toFixed(3);
    stateMap[`model`] = battery.model;
    stateMap[`serial`] = battery.serial;
    stateMap[`time_to_empty`] =
      battery.timeToEmpty !== 0
        ? battery.timeToEmpty.toFixed(2)
        : "unavailable";
    stateMap[`time_to_full`] =
      battery.timeToFull !== 0 ? battery.timeToFull.toFixed(2) : "unavailable";
    stateMap[`volts`] = battery.volts.toFixed(1);
    stateMap[`watts`] = battery.watts.toFixed(2);
    stateMap[`watts_in`] = battery.wattsIn.toFixed(2);
    stateMap[`watts_out`] = battery.wattsOut.toFixed(2);

    for (const [key, value] of Object.entries(stateMap)) {
      const topic = `${process.env.MQTT_PREFIX!}/battery/${
        battery.serial
      }/${key}`;
      const payload = `${value}`;

      log.info(`publishing: ${topic}: ${payload}`);
      await mqttConn.publish(topic, payload);
    }
  },
  publishModule: async (mqttConn: Mqtt.AsyncMqttClient, module: Module) => {
    const stateMap: Record<string, unknown> = {};

    stateMap[`amps`] = module.amps.toFixed(2);
    stateMap[`amps_in`] = module.ampsIn.toFixed(2);
    stateMap[`amps_out`] = module.ampsOut.toFixed(2);
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
    stateMap[`watts_in`] = module.wattsIn.toFixed(2);
    stateMap[`watts_out`] = module.wattsOut.toFixed(2);

    for (let ci = 0; ci < module.cellCount; ci++) {
      const cellNumberString = (ci + 1).toString().padStart(2, "0");
      stateMap[`cells/${cellNumberString}/temperature`] =
        module.cellTemperatures[ci].toFixed(1);
      stateMap[`cells/${cellNumberString}/volts`] =
        module.cellVoltages[ci].toFixed(1);
    }

    for (const [key, value] of Object.entries(stateMap)) {
      const topic = `${process.env.MQTT_PREFIX!}/modules/${
        module.serial
      }/${key}`;
      const payload = `${value}`;

      log.info(`publishing: ${topic}: ${payload}`);
      await mqttConn.publish(topic, payload);
    }
  },
};

const main = async () => {
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

  if (process.env.HOME_ASSISTANT_DISCOVERY_ENABLE! === "true") {
    log.info(`begin configuring modules`);

    let modules: Array<Module> = [];
    for (let si = 1; si <= parseInt(process.env.SERVER_COUNT!); si++) {
      const server = parseInt(process.env[`SERVER_${si}_MODBUS_ADDRESS`]!, 10);
      log.info(`configuring HA for server ${server}`);

      const serverData = await queryModbusServer(modbusConn, server);
      const module = moduleOf(serverData);

      await homeAssistantConfigFns.publishModule(mqttConn, module);

      modules = [...modules, module];
    }
    log.info(`finished configuring modules`);

    log.info(`begin configuring battery`);
    const battery = batteryOf(modules);
    await homeAssistantConfigFns.publishBattery(mqttConn, battery);
    log.info(`finished configuring battery`);
  }

  log.info(`begin main loop`);
  for (;;) {
    let modules: Array<Module> = [];
    for (let si = 1; si <= parseInt(process.env.SERVER_COUNT!); si++) {
      const server = parseInt(process.env[`SERVER_${si}_MODBUS_ADDRESS`]!, 10);

      const serverData = await queryModbusServer(modbusConn, server);
      const module = moduleOf(serverData);

      await stateFns.publishModule(mqttConn, module);
      modules = [...modules, module];
    }

    const battery = batteryOf(modules);
    await stateFns.publishBattery(mqttConn, battery);
  }
};

main();

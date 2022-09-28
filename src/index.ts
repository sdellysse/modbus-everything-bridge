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
          chargeAh: 0.001 * numberAt(5044, 2, "unsigned"),
          capacityAh: 0.001 * numberAt(5046, 2, "unsigned"),
          cycle: 1.0 * numberAt(5048, 1, "unsigned"),
          chargeVoltageLimit: 0.1 * numberAt(5049, 1, "unsigned"),
          dischargeVoltageLimit: 0.1 * numberAt(5050, 1, "unsigned"),
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
  const amperage = data.amperage;
  const capacity = data.capacityAh * data.voltage;

  // For some reason, my modules report 4 for cellVoltageCount, but 3 for
  // cellTemperatureCount, even though there are actually 4 cell temperature
  // sensors. For now, I'm gonna ignore cellTempCount, but if anyone else
  // ever uses this I'll see what we can do.
  const cells = new Array(data.cellVoltageCount)
    .fill(undefined)
    .map((_value, ci) => ({
      cellNumber: ci + 1,
      temperature: data.cellTemperatures[ci],
      voltage: data.cellVoltages[ci],
    }));

  const cellTemperatureMax = Math.max(...cells.map((cell) => cell.temperature));
  const cellTemperatureMin = Math.min(...cells.map((cell) => cell.temperature));
  const cellTemperatureVariance = cellTemperatureMax - cellTemperatureMin;

  const cellVoltageMax = Math.max(...cells.map((cell) => cell.voltage));
  const cellVoltageMin = Math.min(...cells.map((cell) => cell.voltage));
  const cellVoltageVariance = cellVoltageMax - cellVoltageMin;

  const charge = data.chargeAh * data.voltage;
  const cycle = data.cycle;
  const manufacturerName = data.manufacturerName;
  const model = data.model;
  const serial = data.serial.replace(/[^A-Za-z0-9]/g, "");
  const soc = 100.0 * (data.chargeAh / data.capacityAh);

  const temperatureSum = cells
    .map((cell) => cell.temperature)
    .reduce((acc, temperature) => acc + temperature, 0);
  const temperature = temperatureSum / cells.length;
  const timeToFull =
    amperage > 0 ? (data.capacityAh - data.chargeAh) / amperage : 0;
  const timeToEmpty = amperage < 0 ? Math.abs(data.chargeAh / amperage) : 0;

  const voltage = data.voltage;
  const wattage = data.amperage * data.voltage;

  return {
    amperage,
    capacity,
    cells,
    cellTemperatureMax,
    cellTemperatureMin,
    cellTemperatureVariance,
    cellVoltageMax,
    cellVoltageMin,
    cellVoltageVariance,
    charge,
    cycle,
    manufacturerName,
    model,
    serial,
    soc,
    temperature,
    timeToFull,
    timeToEmpty,
    voltage,
    wattage,
  };
};
type Module = Awaited<ReturnType<typeof moduleOf>>;

const batteryOf = (modules: Array<Module>) => {
  const sortedModules = [...modules].sort((l, r) =>
    l.serial.localeCompare(r.serial)
  );

  let sums = {
    amperage: 0,
    capacity: 0,
    charge: 0,
    model: <Array<string>>[],
    modules: <Array<Module>>[],
    serial: <Array<string>>[],
    soc: 0,
    temperature: 0,
    timeToEmpty: 0,
    timeToFull: 0,
    voltage: 0,
    wattage: 0,
  };
  for (const module of sortedModules) {
    sums = {
      amperage: sums.amperage + module.amperage,
      capacity: sums.capacity + module.capacity,
      charge: sums.charge + module.charge,
      model: [...sums.model, module.model],
      modules: [...sums.modules, module],
      serial: [...sums.serial, module.serial],
      soc: sums.soc + module.soc,
      temperature: sums.temperature + module.temperature,
      timeToEmpty: sums.timeToEmpty + module.timeToEmpty,
      timeToFull: sums.timeToFull + module.timeToFull,
      voltage: sums.voltage + module.voltage,
      wattage: sums.wattage + module.wattage,
    };
  }

  const cellTemperatureMax = Math.max(
    ...modules.map((module) => module.cellTemperatureMax)
  );
  const cellTemperatureMin = Math.min(
    ...modules.map((module) => module.cellTemperatureMin)
  );
  const cellTemperatureVariance = cellTemperatureMax - cellTemperatureMin;

  const cellVoltageMax = Math.max(
    ...modules.map((module) => module.cellVoltageMax)
  );
  const cellVoltageMin = Math.min(
    ...modules.map((module) => module.cellVoltageMin)
  );
  const cellVoltageVariance = cellVoltageMax - cellVoltageMin;

  const model = `${modules.length.toString().padStart(2, "0")}x${Math.abs(
    CRC32.str(uniqueStrings(sums.model).join("."))
  )}`;

  const moduleAmperageMax = Math.max(
    ...modules.map((module) => module.amperage)
  );
  const moduleAmperageMin = Math.min(
    ...modules.map((module) => module.amperage)
  );
  const moduleAmperageVariance = moduleAmperageMax - moduleAmperageMin;

  const moduleCapacityMax = Math.max(
    ...modules.map((module) => module.capacity)
  );
  const moduleCapacityMin = Math.min(
    ...modules.map((module) => module.capacity)
  );
  const moduleCapacityVariance = moduleCapacityMax - moduleCapacityMin;

  const moduleChargeMax = Math.max(...modules.map((module) => module.charge));
  const moduleChargeMin = Math.min(...modules.map((module) => module.charge));
  const moduleChargeVariance = moduleChargeMax - moduleChargeMin;

  const moduleVoltageMax = Math.max(...modules.map((module) => module.voltage));
  const moduleVoltageMin = Math.min(...modules.map((module) => module.voltage));
  const moduleVoltageVariance = moduleVoltageMax - moduleVoltageMin;

  const moduleWattageMax = Math.max(...modules.map((module) => module.wattage));
  const moduleWattageMin = Math.min(...modules.map((module) => module.wattage));
  const moduleWattageVariance = moduleWattageMax - moduleWattageMin;

  const serial = `${modules.length.toString().padStart(2, "0")}x${Math.abs(
    CRC32.str(uniqueStrings(sums.serial).join("."))
  )}`;

  const soc = sums.soc / modules.length;
  const temperature = sums.temperature / modules.length;
  const timeToEmpty = sums.timeToEmpty / modules.length;
  const timeToFull = sums.timeToFull / modules.length;
  const voltage = sums.voltage / modules.length;

  return {
    ...sums,
    cellTemperatureMax,
    cellTemperatureMin,
    cellTemperatureVariance,
    cellVoltageMax,
    cellVoltageMin,
    cellVoltageVariance,
    model,
    moduleAmperageMax,
    moduleAmperageMin,
    moduleAmperageVariance,
    moduleCapacityMax,
    moduleCapacityMin,
    moduleCapacityVariance,
    moduleChargeMax,
    moduleChargeMin,
    moduleChargeVariance,
    moduleVoltageMax,
    moduleVoltageMin,
    moduleVoltageVariance,
    moduleWattageMax,
    moduleWattageMin,
    moduleWattageVariance,
    serial,
    soc,
    temperature,
    timeToEmpty,
    timeToFull,
    voltage,
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
        state_topic: "amperage",
        unique_id: "amperage",
        name: "Amperage",
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
        unique_id: `capacity`,
        state_topic: "capacity",
        name: "Capacity",
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
        unique_id: `cell_temperature_max`,
        state_topic: `cell_temperature_max`,
        name: "Cell Temperature Maximum",
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
        unique_id: `cell_temperature_min`,
        state_topic: `cell_temperature_min`,
        name: "Cell Temperature Minimum",
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
        unique_id: `cell_temperature_variance`,
        state_topic: `cell_temperature_variance`,
        name: "Cell Temperature Variance",
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
        unique_id: `cell_voltage_max`,
        state_topic: `cell_voltage_max`,
        name: "Cell Voltage Maximum",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cell_voltage_min`,
        state_topic: `cell_voltage_min`,
        name: "Cell Voltage Minimum",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cell_voltage_variance`,
        state_topic: `cell_voltage_variance`,
        name: "Cell Voltage Variance",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge`,
        state_topic: "charge",
        name: "Charge",
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
        unique_id: `module_amperage_max`,
        state_topic: `module_amperage_max`,
        name: "Module Amperage Maximum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_amperage_min`,
        state_topic: `module_amperage_min`,
        name: "Module Amperage Minimum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_amperage_variance`,
        state_topic: `module_amperage_variance`,
        name: "Module Amperage Variance",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "A",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_capacity_max`,
        state_topic: `module_capacity_max`,
        name: "Module Capacity Maximum",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_capacity_min`,
        state_topic: `module_capacity_min`,
        name: "Module Capacity Minimum",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_capacity_variance`,
        state_topic: `module_capacity_variance`,
        name: "Module Capacity Variance",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "Wh",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_charge_max`,
        state_topic: `module_charge_max`,
        name: "Module Charge Maximum",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_charge_min`,
        state_topic: `module_charge_min`,
        name: "Module Charge Minimum",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_charge_variance`,
        state_topic: `module_charge_variance`,
        name: "Module Charge Variance",
        config: {
          device_class: "energy",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_voltage_max`,
        state_topic: `module_voltage_max`,
        name: "Module Voltage Maximum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_voltage_min`,
        state_topic: `module_voltage_min`,
        name: "Module Voltage Minimum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_voltage_variance`,
        state_topic: `module_voltage_variance`,
        name: "Module Voltage Variance",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_wattage_max`,
        state_topic: `module_wattage_max`,
        name: "Module Wattage Maximum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_wattage_min`,
        state_topic: `module_wattage_min`,
        name: "Module Wattage Minimum",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `module_wattage_variance`,
        state_topic: `module_wattage_variance`,
        name: "Module Wattage Variance",
        config: {
          device_class: "current",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `soc`,
        state_topic: "soc",
        name: "SOC",
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
        unique_id: `voltage`,
        state_topic: `voltage`,
        name: "Voltage",
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
        unique_id: "wattage",
        state_topic: `wattage`,
        name: "Wattage",
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
            manufacturer: "battery2mqtt",
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
        state_topic: "amperage",
        unique_id: "amperage",
        name: "Amperage",
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
        unique_id: `capacity`,
        state_topic: "capacity",
        name: "Capacity",
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
        unique_id: `cell_temperature_max`,
        state_topic: `cell_temperature_max`,
        name: "Cell Temperature Maximum",
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
        unique_id: `cell_temperature_min`,
        state_topic: `cell_temperature_min`,
        name: "Cell Temperature Minimum",
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
        unique_id: `cell_temperature_variance`,
        state_topic: `cell_temperature_variance`,
        name: "Cell Temperature Variance",
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
        unique_id: `cell_voltage_max`,
        state_topic: `cell_voltage_max`,
        name: "Cell Voltage Maximum",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cell_voltage_min`,
        state_topic: `cell_voltage_min`,
        name: "Cell Voltage Minimum",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `cell_voltage_variance`,
        state_topic: `cell_voltage_variance`,
        name: "Cell Voltage Variance",
        config: {
          device_class: "voltage",
          entity_category: "diagnostic",
          state_class: "measurement",
          unit_of_measurement: "V",
        },
      },
    ];
    mappings = [
      ...mappings,
      {
        unique_id: `charge`,
        state_topic: "charge",
        name: "Charge",
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
        unique_id: `soc`,
        state_topic: "soc",
        name: "SOC",
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
        unique_id: `voltage`,
        state_topic: `voltage`,
        name: "Voltage",
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
        unique_id: "wattage",
        state_topic: `wattage`,
        name: "Wattage",
        config: {
          device_class: "current",
          state_class: "measurement",
          unit_of_measurement: "W",
        },
      },
    ];

    for (const cell of module.cells) {
      const cellNumberString = cell.cellNumber.toString().padStart(2, "0");

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
          unique_id: `cell_${cellNumberString}_voltage`,
          state_topic: `cells/${cellNumberString}/voltage`,
          name: `Cell ${cellNumberString} Voltage`,
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
    const stateMap = {
      amperage: battery.amperage.toFixed(2),
      capacity: battery.capacity.toFixed(3),
      cell_temperature_max: battery.cellTemperatureMax.toFixed(2),
      cell_temperature_min: battery.cellTemperatureMin.toFixed(2),
      cell_temperature_variance: battery.cellTemperatureVariance.toFixed(2),
      cell_voltage_max: battery.cellVoltageMax.toFixed(2),
      cell_voltage_min: battery.cellVoltageMin.toFixed(2),
      cell_voltage_variance: battery.cellVoltageVariance.toFixed(2),
      charge: battery.charge.toFixed(3),
      model: battery.model,
      module_amperage_max: battery.moduleAmperageMax.toFixed(2),
      module_amperage_min: battery.moduleAmperageMin.toFixed(2),
      module_amperage_variance: battery.moduleAmperageVariance.toFixed(2),
      module_capacity_max: battery.moduleCapacityMax.toFixed(2),
      module_capacity_min: battery.moduleCapacityMin.toFixed(2),
      module_capacity_variance: battery.moduleCapacityVariance.toFixed(2),
      module_charge_max: battery.moduleChargeMax.toFixed(2),
      module_charge_min: battery.moduleChargeMin.toFixed(2),
      module_charge_variance: battery.moduleChargeVariance.toFixed(2),
      module_voltage_max: battery.moduleVoltageMax.toFixed(2),
      module_voltage_min: battery.moduleVoltageMin.toFixed(2),
      module_voltage_variance: battery.moduleVoltageVariance.toFixed(2),
      module_wattage_max: battery.moduleWattageMax.toFixed(2),
      module_wattage_min: battery.moduleWattageMin.toFixed(2),
      module_wattage_variance: battery.moduleWattageVariance.toFixed(2),
      serial: battery.serial,
      soc: battery.soc.toFixed(2),
      time_to_empty:
        battery.timeToEmpty !== 0
          ? battery.timeToEmpty.toFixed(2)
          : "unavailable",
      time_to_full:
        battery.timeToFull !== 0
          ? battery.timeToFull.toFixed(2)
          : "unavailable",
      voltage: battery.voltage.toFixed(1),
      wattage: battery.wattage.toFixed(2),
    };

    for (const [key, value] of Object.entries(stateMap)) {
      const mqttPrefix = process.env.MQTT_PREFIX!;
      const topic = `${mqttPrefix}/battery/${battery.serial}/${key}`;
      const payload = `${value}`;

      log.info(`publishing: ${topic}: ${payload}`);
      await mqttConn.publish(topic, payload);
    }
  },
  publishModule: async (mqttConn: Mqtt.AsyncMqttClient, module: Module) => {
    const stateMap = {
      amperage: module.amperage.toFixed(2),
      capacity: module.capacity.toFixed(2),
      cell_temperature_max: module.cellTemperatureMax.toFixed(2),
      cell_temperature_min: module.cellTemperatureMin.toFixed(2),
      cell_temperature_variance: module.cellTemperatureVariance.toFixed(2),
      cell_voltage_max: module.cellVoltageMax.toFixed(2),
      cell_voltage_min: module.cellVoltageMin.toFixed(2),
      cell_voltage_variance: module.cellVoltageVariance.toFixed(2),
      charge: module.charge.toFixed(2),
      cycle: module.cycle,
      manufacturer_name: module.manufacturerName,
      model: module.model,
      serial: module.serial,
      soc: module.soc.toFixed(2),
      time_to_empty:
        module.timeToEmpty !== 0
          ? module.timeToEmpty.toFixed(2)
          : "unavailable",
      time_to_full:
        module.timeToFull !== 0 ? module.timeToFull.toFixed(2) : "unavailable",
      voltage: module.voltage.toFixed(1),
      wattage: module.wattage.toFixed(2),

      ...Object.fromEntries(
        module.cells.flatMap((cell) => {
          const cellNumberString = cell.cellNumber.toString().padStart(2, "0");
          return [
            [
              `cells/${cellNumberString}/temperature`,
              cell.temperature.toFixed(1),
            ],
            [`cells/${cellNumberString}/voltage`, cell.voltage.toFixed(1)],
          ];
        })
      ),
    };
    for (const [key, value] of Object.entries(stateMap)) {
      const mqttPrefix = process.env.MQTT_PREFIX!;
      const topic = `${mqttPrefix}/modules/${module.serial}/${key}`;
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

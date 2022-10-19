import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import { log, pMapSeries } from "./utils";

const config = {
  mqtt: {
    url: "tcp://scratch0.lan:1883",
    prefix: "modbus",
  },

  modbus: {
    ttyUSB0: {
      connection: "RTU",
      device: "/dev/ttyUSB0",
      options: {
        baudRate: 9600,
      },

      blocks: [
        { server: 48, range: [5000, 5034] },
        { server: 49, range: [5000, 5034] },
        { server: 50, range: [5000, 5034] },
        { server: 51, range: [5000, 5034] },
        { server: 48, range: [5035, 5053] },
        { server: 49, range: [5035, 5053] },
        { server: 50, range: [5035, 5053] },
        { server: 51, range: [5035, 5053] },
        { server: 48, range: [5100, 5142] },
        { server: 49, range: [5100, 5142] },
        { server: 50, range: [5100, 5142] },
        { server: 51, range: [5100, 5142] },
      ],
    },
  },
};

(async () => {
    const mqttConn = await Mqtt.connectAsync(config.mqtt.url, {
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


    const modbusConnMap = Object.fromEntries(await pMapSeries(Object.entries(config.modbus), async ([name, def]) => {
        if (def.connection === "RTU") {
            const modbusConn = new Modbus();
            await modbusConn.connectRTUBuffered(def.device, def.options);
            return [name, modbusConn];
        }

        throw new Error(`Unknown connection type: ${def.connection}`);
    }));

    const modbusConnMap2: Record<string, Modbus> = {};
    for (const [name, def] of Object.entries(config.modbus)) {
        if (def.connection === "RTU") {
            const modbusConn = new Modbus();
            await modbusConn.connectRTUBuffered(def.device, def.options);
            modbusConnMap2[name] = modbusConn;
            continue;
        }

        throw new Error(`Unknown connection type: ${def.connection}`);
    }

    //     const modbusConnMap = for map (const [name, def] of Object.entries(config.modbus)) {
    //         if (def.connection === "RTU") {
    //             const modbusConn = new Modbus();
    //             await modbusConn.connectRTUBuffered(def.device, def.options);
    //             continue with modbusConn;
    //         }
    //
    //         throw new Error(`Unknown connection type: ${def.connection}`);
    //     }
})();

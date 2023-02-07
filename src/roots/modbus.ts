import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import { wait } from "../utils";
import { z } from "zod";
import fs from "node:fs/promises";
import util from "node:util";

const main = async () => {
  const argsSchema = z.object({
    values: z.object({
      config: z.string().default(`${__dirname}/../etc/modbus.json`),
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
    }),
    modbus: z.object({
      type: z.literal("rtu"),
      device: z.string(),
      baudRate: z.number().default(9600),
    }),

    servers: z.array(
      z.object({
        address: z.number(),
        attributes: z.unknown().optional(),
        queries: z.array(
          z.object({
            interval: z.literal("continuous"),
            register: z.number(),
            length: z.number().default(1),
          })
        ),
      })
    ),
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
  const mqttPublish = async (topic: string, payload: Buffer) => {
    if (
      topic in published &&
      Buffer.compare(published[topic]!, payload) === 0
    ) {
      // log.info(`skipping pub ${topic}`);
      return;
    }

    await mqttConn.publish(topic, payload, {
      qos: 0,
      retain: true,
    });
    published[topic] = payload;
    // log.info(`published ${topic}`);
  };

  const modbusConn = new Modbus();
  if (false) {
  } else if (config.modbus.type === "rtu") {
    await modbusConn.connectRTUBuffered("/dev/ttyModbusRtuUsb", {
      baudRate: config.modbus.baudRate,
    });
  } else {
    const exhaustivenessCheck: never = config.modbus.type;
    throw new Error(`Missing modbus init for type: ${exhaustivenessCheck}`);
  }

  for (;;) {
    for (const server of config.servers) {
      modbusConn.setID(server.address);

      await mqttPublish(
        `${config.mqtt.prefix}/servers/${server.address}/attributes.json`,
        Buffer.from(JSON.stringify(server.attributes ?? {}))
      );

      for (const query of server.queries) {
        await mqttPublish(
          `${config.mqtt.prefix}/servers/${server.address}/queries/register_${query.register}_length_${query.length}/data`,
          (
            await modbusConn.readHoldingRegisters(query.register, query.length)
          ).buffer
        );
        await wait(10);
      }
    }
  }
};

main().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

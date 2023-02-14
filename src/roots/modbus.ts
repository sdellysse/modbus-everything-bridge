import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import { wait } from "../utils";
import { z } from "zod";
import fs from "node:fs/promises";
import { parseEnv } from "znv";
import winston from "winston";

(async () => {
  const env = parseEnv(process.env, {
    LOGLEVEL: z.string().default("info"),
    CONFIGFILE: z.string().default(`${__dirname}/../etc/modbus.json`),
  });

  const logger = winston.createLogger({
    level: env.LOGLEVEL,
    format: winston.format.simple(),
    transports: [new winston.transports.Console()],
  });
  logger.debug("env", env);

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
        attributes: z.record(z.unknown()).default({}),
        enabled: z.boolean().default(true),
        queries: z.array(
          z.object({
            attributes: z.record(z.unknown()).default({}),
            enabled: z.boolean().default(true),
            interval: z.literal("continuous"),
            name: z.string(),
            register: z.number(),
            length: z.number().default(1),
          })
        ),
      })
    ),
  });

  const config = configSchema.parse(
    JSON.parse(await fs.readFile(env.CONFIGFILE, { encoding: "utf-8" }))
  );
  logger.debug("config", config);

  logger.debug(`connecting to ${config.mqtt.server}`);
  const mqttConn = await Mqtt.connectAsync(config.mqtt.server, {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${config.mqtt.prefix}/status`,
    },
  });
  logger.info(`connected to mqtt broker "${config.mqtt.server}"`);

  logger.debug("publishing status");
  await mqttConn.publish(`${config.mqtt.prefix}/status`, "online", {
    qos: 0,
    retain: true,
  });
  logger.info("published status");

  const published: Record<string, Buffer> = {};
  const mqttPublish = async (topic: string, payload: Buffer | string) => {
    logger.debug(`publishing "${topic}" with "${payload}`);

    const buffer = typeof payload === "string" ? Buffer.from(payload) : payload;
    if (topic in published && Buffer.compare(published[topic]!, buffer) === 0) {
      logger.debug("skipping publish, dupe");
      return;
    }

    await mqttConn.publish(topic, buffer, {
      qos: 0,
      retain: true,
    });
    published[topic] = buffer;
    logger.debug("published");
  };

  const modbusConn = new Modbus();
  if (false) {
  } else if (config.modbus.type === "rtu") {
    const crashTimeout = setTimeout(() => {
      logger.error(`Timeout waiting for modbus to connect`);
    }, 30_000);
    await modbusConn.connectRTUBuffered(config.modbus.device, {
      baudRate: config.modbus.baudRate,
    });
    clearTimeout(crashTimeout);
    logger.info(
      `Connected to RTU "${config.modbus.device}", baudRate "${config.modbus.baudRate}" `
    );
  } else {
    const exhaustivenessCheck: never = config.modbus.type;
    throw new Error(`Missing modbus init for type: ${exhaustivenessCheck}`);
  }

  logger.info("begin main loop");
  for (;;) {
    for (const server of config.servers) {
      if (!server.enabled) {
        continue;
      }

      modbusConn.setID(server.address);

      await mqttPublish(
        `${config.mqtt.prefix}/servers/${server.address}/attributes.json`,
        Buffer.from(
          JSON.stringify({
            ...server.attributes,
            address: server.address,
          })
        )
      );

      for (const query of server.queries) {
        if (!query.enabled) {
          continue;
        }

        await mqttPublish(
          `${config.mqtt.prefix}/servers/${server.address}/queries/${query.name}/attributes.json`,
          Buffer.from(
            JSON.stringify({
              ...query.attributes,
              register: query.register,
              length: query.length,
            })
          )
        );

        const crashTimeout = setTimeout(() => {
          logger.error(
            `Timeout waiting for query "${query.name}" on "${server.address}"`
          );
          process.exit(1);
        }, 30_000);
        const modbusResponse = await modbusConn.readHoldingRegisters(
          query.register,
          query.length
        );
        clearTimeout(crashTimeout);

        await mqttPublish(
          `${config.mqtt.prefix}/servers/${server.address}/queries/${query.name}/data`,
          modbusResponse.buffer
        );
        await wait(10);
      }
    }
  }
})().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

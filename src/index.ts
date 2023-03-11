import winston from "winston";
import { z } from "zod";
import * as modbus from "./modbus";
import * as mqtt from "./mqtt";
import * as http from "./http";

type OptionsDefaultExport = {
  http?: {
    port: number;
  };
  logger: winston.Logger;
  modbus: {
    uri: string;
  };
  mqtt?: {
    prefix: string;
    uri: string;
  };
};
const defaultExport = async ({logger, ...options}: OptionsDefaultExport) => {
  logger.debug("options", options);

  logger.debug(`connection to modbus via ${options.modbus.uri}`);
  const modbusConn = await modbus.connect({
    logger,
    uri: options.modbus.uri,
  });
  logger.info(`connected to modbus via ${options.modbus.uri}`);
  const useConn = modbus.interlockConn(modbusConn);

  if (options.http !== undefined) {
    logger.debug(`starting http server on port ${options.http.port}`);
    const httpServer = await http.listen(options.http.port);
    logger.info(`started http server on port ${options.http.port}`);

    httpServer.post("/read_register", async (req, res) => {
      logger.debug(`http: POST /read_register: ${JSON.stringify(req.body)}`);

      const body = z
        .object({
          address: z.number(),
          register: z.number(),
          length: z.number(),
        })
        .parse(req.body);

      const bytes = await useConn(
        logger,
        async (conn) =>
          await modbus.readRegister({
            ...body,
            conn,
            logger,
          })
      );

      res.status(200);
      res.header("Content-Type", "application/octet-stream");
      res.send(bytes);
      res.end();
    });
  }

  if (options.mqtt !== undefined) {
    const mqttConn = await mqtt.connect({
      logger,
      prefix: options.mqtt.prefix,
      uri: options.mqtt.uri,
    });

    mqttConn.on("message", async (topic, payload, packet) => {
      if (topic === `${options.mqtt?.prefix}/request.read_register`) {
        const responseTopic = z
          .string()
          .parse(packet.properties?.responseTopic);
        const message = z
          .object({
            address: z.number(),
            register: z.number(),
            length: z.number(),
          })
          .parse(JSON.parse(payload.toString("utf-8")));

        const bytes = await useConn(logger, async (conn) =>
          modbus.readRegister({
            ...message,
            conn,
            logger,
          })
        );

        await mqttConn.publish(responseTopic, bytes, {
          properties: {
            contentType: "application/octet-stream",
            correlationData:
              packet.properties?.correlationData ?? Buffer.from(""),
          },
          qos: 2,
        });

        return;
      }
    });

    await mqttConn.subscribe([`${options.mqtt.prefix}/request.read_register`]);
  }
};
export default defaultExport;

if (require.main === module) {
  (async () => {
    const envSchema = (() => {
      const common = z.object({
        LOGLEVEL: z.string().default("info"),
        HTTP_PORT: z.string().default("5280").transform(Number).pipe(z.number()),
        MODBUS_URI: z.string(),
      });

      const mqttProvided = z.object({
        MQTT_URI: z.string().url(),
        MQTT_PREFIX: z.string().default("modbus"),
      });

      const mqttOmitted = z.object({
        MQTT_URI: z.undefined(),
        MQTT_PREFIX: z.undefined(),
      });

      return z.union([common.merge(mqttProvided), common.merge(mqttOmitted)]);
    })();

    const env = envSchema.parse(process.env);

    const logger = winston.createLogger({
      level: env.LOGLEVEL,
      format: winston.format.simple(),
      transports: [new winston.transports.Console()],
    });

    await defaultExport({
      logger,
      modbus: {
        uri: env.MODBUS_URI,
      },
      ...(env.HTTP_PORT !== undefined
        ? {
            http: {
              port: env.HTTP_PORT,
            },
          }
        : {}),
      ...(env.MQTT_URI !== undefined
        ? {
            mqtt: {
              prefix: env.MQTT_PREFIX,
              uri: env.MQTT_URI,
            },
          }
        : {}),
    });
  })().catch((error) => {
    console.log(error);
    console.log(JSON.stringify(error, undefined, 4));
    process.exit(1);
  });
}

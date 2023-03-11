import winston from "winston";
import { z } from "zod";
import * as modbus from "./modbus";
import * as http from "./http";

type OptionsDefaultExport = {
  http: {
    port: number;
  };
  logger: winston.Logger;
  modbus: {
    uri: string;
  };
};
const defaultExport = async ({ logger, ...options }: OptionsDefaultExport) => {
  logger.debug("options", options);

  logger.debug(`connection to modbus via ${options.modbus.uri}`);
  const modbusConn = await modbus.connect({
    logger,
    uri: options.modbus.uri,
  });
  logger.info(`connected to modbus via ${options.modbus.uri}`);
  const useConn = modbus.interlockConn(modbusConn);

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
};
export default defaultExport;

if (require.main === module) {
  (async () => {
    const envSchema = z.object({
      LOGLEVEL: z.string().default("info"),
      HTTP_PORT: z.string().default("5280").transform(Number).pipe(z.number()),
      MODBUS_URI: z.string(),
    });

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
      http: {
        port: env.HTTP_PORT,
      },
    });
  })().catch((error) => {
    console.log(error);
    console.log(JSON.stringify(error, undefined, 4));
    process.exit(1);
  });
}

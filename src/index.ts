import winston from "winston";
import { z } from "zod";
import * as modbus from "./modbus";
import * as http from "./http";
import * as cmdTs from "cmd-ts";

const cli = cmdTs.command({
	name: "modbus-http",

	args: {
		httpPort: cmdTs.option({
			type: cmdTs.number,
			long: "http-port",
			defaultValue: 21224,
			defaultValueIsSerializable: true,
		}),

		logLevel: cmdTs.option({
			type: cmdTs.string,
			long: "log-level",
			defaultValue: "info",
			defaultValueIsSerializable: true,
		}),

		modbusUri: cmdTs.option({
			type: cmdTs.string,
			long: "modbus-uri",
		}),
	},

	handler: async (args) => {
		const logger = winston.createLogger({
			level: args.loglevel
			format: winston.format.simple(),
			transports: [new winston.transports.Console()],
		});

		logger.debug("args", args);

		logger.debug(`connection to modbus via ${args.modbusUri}`);
		const modbusConn = await modbus.connect({
			logger,
			uri: args.modbusUri,
		});
		logger.info(`connected to modbus via ${args.modbusUri}`);

		const useConn = modbus.interlockConn(modbusConn);

		logger.debug(`starting http server on port ${args.httpPort}`);
		const httpServer = await http.listen(args.httpPort);
		logger.info(`started http server on port ${args.httpPort}`);

		httpServer.route({
			method: "POST",
			path: "/read_register",
			
			schemas: {
				body: z.object({
					address: z.number(),
					register: z.number(),
					length: z.number(),
				}),
			},

			handler: async (req) => {
				const bytes = await useConn(
					logger,
					async (conn) => await modbus.readRegister({
						...req.bodyParams,
						conn,
						logger,
					}),
				);

				return {
					code: 200,
					headers: [
						["Content-Type", "application/octet-stream"],
						["Content-Length", bytes.length],
					],
					body: bytes,
				};
			},
		});
	},
});

cmdTs.run(cmdTs.binary(cli), process.argv)


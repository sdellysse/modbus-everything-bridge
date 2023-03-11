import winston from "winston";
import { withTimeout } from "./utility";
import Modbus from "modbus-serial";
import { z } from "zod";

type OptionsConnect = {
  logger: winston.Logger;
  uri: string;
};
export const connect = async ({ logger, uri }: OptionsConnect) => {
  const [modbusProtocolType, ...modbusProtocolParams] = z
    .tuple([z.union([z.literal("rtu"), z.literal("fake")])])
    .rest(z.unknown())
    .parse(uri.split(":"));

  if (modbusProtocolType === "fake") {
    return <Modbus>{
      setID: (id: number) => {
        logger.debug(`modbus: fake: setID: ${id}`);
      },
      readHoldingRegisters: async (register: number, length: number) => {
        logger.debug(
          `modbus: fake: readHoldingRegisters: ${register} ${length}`
        );

        return {
          buffer: Buffer.from("fake modbus response"),
        };
      },
    };
  }

  if (modbusProtocolType === "rtu") {
    const [device, ...pairs] = z
      .tuple([z.string()])
      .rest(z.string())
      .parse(modbusProtocolParams);

    const rtuOptions = z
      .array(
        z
          .string()
          .refine(
            (str) =>
              str.includes("=") && !str.startsWith("=") && !str.endsWith("=")
          )
      )
      .transform((strings) =>
        strings
          .map((string) => string.split("="))
          .reduce(
            (acc, [key, value]) => ({
              ...acc,
              [key!]: value!,
            }),
            <Record<string, string>>{}
          )
      )
      .parse(pairs);

    logger.debug(`Connecting to modbus-rtu "${device}"`);
    const retval = await withTimeout(
      logger,
      `Timeout waiting for modbus to connect`,
      30_000,
      async () => {
        const conn = new Modbus();
        await conn.connectRTUBuffered(device, {
          ...rtuOptions,
        });
        return conn;
      }
    );
    logger.info(`Connected to modbus-rtu "${device}"`);

    return retval;
  }

  throw new Error(`bad uri: ${uri}`);
};

type OptionsReadRegister = {
  address: number;
  conn: Modbus;
  length: number;
  logger: winston.Logger;
  register: number;
};
export const readRegister = async ({
  address,
  conn,
  length,
  logger,
  register,
}: OptionsReadRegister) =>
  await withTimeout(
    logger,
    `timeout in readRegister: ${address} ${register} ${length}`,
    30000,
    async () => {
      conn.setID(address);
      return (await conn.readHoldingRegisters(register, length)).buffer;
    }
  );

type InterlockContext = {
  conn: Modbus;
  locked: boolean;
};
const useInterlockedConn = async <FnReturnType>(
  ctx: InterlockContext,
  logger: winston.Logger,
  fn: (conn: Modbus) => Promise<FnReturnType>
) => {
  await withTimeout(
    logger,
    "Timeout acquiring lock in ModbusInterlocker",
    30_000,
    async () => {
      while (ctx.locked) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    }
  );

  ctx.locked = true;
  const retval = await fn(ctx.conn);
  ctx.locked = false;
  return retval;
};

export const interlockConn = (conn: Modbus) => {
  const ctx: InterlockContext = {
    conn,
    locked: false,
  };

  return async <FnReturnType>(
    logger: winston.Logger,
    fn: (conn: Modbus) => Promise<FnReturnType>
  ) => await useInterlockedConn(ctx, logger, fn);
};

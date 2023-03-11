import winston from "winston";

export const withTimeout = async <FnReturn>(
    logger: winston.Logger,
    message: string,
    timeout: number,
    fn: () => Promise<FnReturn>
  ) => {
    const crashTimer = setTimeout(() => {
      logger.error(message);
      process.exit(1);
    }, timeout);

    const retval = await fn();
    clearTimeout(crashTimer);
    return retval;
  };
export const wait = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

export const log = Object.assign(
  (level: string, message: string) =>
    console.log(`${new Date().toISOString()} [${level}] ${message}`),
  {
    info: (message: string) => log("INFO", message),
    warn: (message: string) => log("WARN", message),
    error: (message: string) => log("ERROR", message),
  }
);

export const bufferParsersOf = (data: Buffer, startRegister: number) => {
  const offsetOf = (register: number) => {
    if (register < startRegister) {
      throw new Error(`bad register: ${register}`);
    }

    return (register - startRegister) * 2;
  };

  const numberAt = (
    register: number,
    length: 1 | 2,
    signed: "signed" | "unsigned"
  ) => {
    const fnMap = <const>{
      "1": {
        unsigned: (register: number) => data.readUInt16BE(offsetOf(register)),
        signed: (register: number) => data.readInt16BE(offsetOf(register)),
      },
      "2": {
        unsigned: (register: number) => data.readUInt32BE(offsetOf(register)),
        signed: (register: number) => data.readInt32BE(offsetOf(register)),
      },
    };

    const fn = fnMap[`${length}`][`${signed}`];
    return fn(register);
  };

  const upperByteAt = (register: number, signed: "signed" | "unsigned") => {
    const fnMap = <const>{
      unsigned: (register: number) => data.readUInt8(offsetOf(register)),
      signed: (register: number) => data.readInt8(offsetOf(register)),
    };

    const fn = fnMap[`${signed}`];
    return fn(register);
  };

  const lowerByteAt = (register: number, signed: "signed" | "unsigned") => {
    const fnMap = <const>{
      unsigned: (register: number) => data.readUInt8(offsetOf(register + 1)),
      signed: (register: number) => data.readInt8(offsetOf(register + 1)),
    };

    const fn = fnMap[`${signed}`];
    return fn(register);
  };

  const asciiAt = (register: number, length: number) => {
    const startIndex = offsetOf(register);
    const endIndex = offsetOf(register) + length * 2;
    return data.toString("ascii", startIndex, endIndex).replace(/\x00+$/, "");
  };

  return {
    asciiAt,
    lowerByteAt,
    numberAt,
    offsetOf,
    upperByteAt,
  };
};

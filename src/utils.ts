export const wait = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

type OptionsOffsetOfRegister = {
  startRegister: number;
  register: number;
};
export const offsetOfRegister = (
  data: Buffer,
  { startRegister, register }: OptionsOffsetOfRegister
) => {
  const offset = (register - startRegister) * 2;
  if (offset < 0 || offset >= data.length) {
    throw new Error(
      `bad offset request: ${data.length}, ${startRegister}, ${register}`
    );
  }

  return offset;
};

type OptionsNumberFromBuffer = {
  register: number;
  length: 1 | 2;
  signed: "signed" | "unsigned";
  startRegister: number;
};
export const numberFromBuffer = (
  data: Buffer,
  { register, length, signed, startRegister }: OptionsNumberFromBuffer
) => {
  const offset = offsetOfRegister(data, { startRegister, register });

  if (length === 1 && signed === "unsigned") {
    return data.readUInt16BE(offset);
  }
  if (length === 1 && signed === "signed") {
    return data.readInt16BE(offset);
  }
  if (length === 2 && signed === "unsigned") {
    return data.readUInt32BE(offset);
  }
  if (length === 2 && signed === "signed") {
    return data.readInt32BE(offset);
  }

  throw new Error(`bad number request: ${length}, ${signed}`);
};

type OptionsAsciiFromBuffer = {
  startRegister: number;
  register: number;
  length: number;
};
export const asciiFromBuffer = (
  data: Buffer,
  { register, length, startRegister }: OptionsAsciiFromBuffer
) => {
  const startOffset = offsetOfRegister(data, { startRegister, register });
  const endOffset = startOffset + length * 2;
  return data.toString("ascii", startOffset, endOffset).replace(/\x00+$/, "");
};

export const bufferParsersOf = (data: Buffer, startRegister: number) => {
  const offsetOf = (register: OptionsOffsetOfRegister["register"]) =>
    offsetOfRegister(data, {
      startRegister,
      register,
    });

  const numberAt = (
    register: OptionsNumberFromBuffer["register"],
    length: OptionsNumberFromBuffer["length"],
    signed: OptionsNumberFromBuffer["signed"]
  ) =>
    numberFromBuffer(data, {
      register,
      length,
      signed,
      startRegister,
    });

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

  const asciiAt = (register: number, length: number) =>
    asciiFromBuffer(data, { register, length, startRegister });

  return {
    asciiAt,
    lowerByteAt,
    numberAt,
    offsetOf,
    upperByteAt,
  };
};

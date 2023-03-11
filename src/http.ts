import bodyParser from "body-parser";
import createExpress from "express";

export const listen = async (port: number) => {
  const express = createExpress();
  express.use(bodyParser.json());

  await new Promise<void>((resolve) =>
    express.listen(port, () => {
      resolve();
    })
  );

  return express;
};

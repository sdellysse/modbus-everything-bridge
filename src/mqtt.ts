import winston from "winston";
import Mqtt from "async-mqtt";

type OptionsConnect = {
  logger: winston.Logger;
  prefix: string;
  uri: string;
};
export const connect = async ({
  logger,
  prefix,
  uri,
}: OptionsConnect) => {
  logger.debug(`Connecting to mqtt broker ${uri}`);
  const mqttConn = await Mqtt.connectAsync(uri, {
    protocolVersion: 5,
    properties: {
        requestProblemInformation: true,
        requestResponseInformation: true,
    },
    will: {
      payload: "offline",
      qos: 2,
      retain: true,
      topic: `${prefix}/status`,
    },
  });
  logger.info(`Connected to mqtt broker ${uri}`);

  logger.debug("publishing mqtt lwt:online");
  await mqttConn.publish(`${prefix}/status`, "online", {
    qos: 2,
    retain: true,
  });
  logger.info("published mqtt lwt:online");

  return mqttConn;
};

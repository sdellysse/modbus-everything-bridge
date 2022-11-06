import Modbus from "modbus-serial";
import Mqtt from "async-mqtt";
import { wait } from "../utils";

const main = async () => {
  const mqttPrefix = "modbus";
  const mqttConn = await Mqtt.connectAsync("tcp://service-mqtt.lan:1883", {
    will: {
      payload: "offline",
      qos: 0,
      retain: true,
      topic: `${mqttPrefix}/status`,
    },
  });
  await mqttConn.publish(`${mqttPrefix}/status`, "online", {
    qos: 0,
    retain: true,
  });
  const published: Record<string, Buffer> = {};
  const mqttPublish = async (topic: string, payload: Buffer) => {
    if (
      topic in published &&
      Buffer.compare(published[topic]!, payload) === 0
    ) {
      // log.info(`skipping pub ${topic}`);
      return;
    }

    await mqttConn.publish(topic, payload, {
      qos: 0,
      retain: true,
    });
    published[topic] = payload;
    // log.info(`published ${topic}`);
  };

  const modbusConn = new Modbus();
  await modbusConn.connectRTUBuffered("/dev/ttyModbusRtuUsb", {
    baudRate: 9600,
  });

  for (;;) {
    const blocks = [
      { server: 48, range: [5000, 5034] },
      { server: 49, range: [5000, 5034] },
      { server: 50, range: [5000, 5034] },
      { server: 51, range: [5000, 5034] },
      { server: 48, range: [5035, 5053] },
      { server: 49, range: [5035, 5053] },
      { server: 50, range: [5035, 5053] },
      { server: 51, range: [5035, 5053] },
      { server: 48, range: [5100, 5142] },
      { server: 49, range: [5100, 5142] },
      { server: 50, range: [5100, 5142] },
      { server: 51, range: [5100, 5142] },
    ];
    for (const block of blocks) {
      modbusConn.setID(block.server);

      const topic = `${mqttPrefix}/blocks/${block.server}::${block.range[0]}..${block.range[1]}`;
      const payload = (
        await modbusConn.readHoldingRegisters(
          block.range[0]!,
          block.range[1]! - block.range[0]!
        )
      ).buffer;

      await mqttPublish(topic, payload);
      await wait(10);
    }
  }
};

main().catch((error) => {
  console.log(error);
  console.log(JSON.stringify(error, undefined, 4));
  process.exit(1);
});

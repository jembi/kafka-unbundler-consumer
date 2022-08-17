const { Kafka, logLevel } = require('kafkajs');

interface Message {
  offset: string;
  timestamp: string;
  key: string;
  value: string;
}

const kafkaHost = process.env.KAFKA_HOST || 'localhost';
const kafkaPort = process.env.KAFKA_PORT || '9092';

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${kafkaHost}:${kafkaPort}`],
  clientId: 'kafka-unbundler-consumer',
});

const topic = '2xx';
const consumer = kafka.consumer({ groupId: 'kafka-unbundler-consumer' });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({
      topic,
      partition,
      message,
    }: {
      topic: string;
      partition: string;
      message: Message;
    }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);

      const bundle: {
        entry: Array<{ resource: { resourceType: string, id: string } }>;
      } = JSON.parse(message.value);

      let resourceMap: {[key: string]: Array<{key: string, value: string}>} = {};

      bundle.entry.forEach(entry => {
        if (!resourceMap[entry.resource.resourceType]) {
          resourceMap[entry.resource.resourceType] = [];
        }
        resourceMap[entry.resource.resourceType].push({
          key: entry.resource.id,
          value: JSON.stringify(entry),
        });
      });

      Object.keys(resourceMap).forEach(resourceType => {
        producer
          .send({
            topic: resourceType.toLowerCase(),
            messages: resourceMap[resourceType],
          })
          .catch((e: Error) =>
            console.error(`[kafka-unbundler-consumer] ${e.message}`, e)
          );
      });
    },
  });
};

run().catch((e: Error) => console.error(`[kafka-unbundler-consumer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
  process.on(type, async (e: Error) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});

import { Kafka, logLevel } from 'kafkajs';
import { splitResources } from './utils';
import { Bundle, ResourceMap } from './types';

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

const runs = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value?.toString()}`);

      const bundle: Bundle = JSON.parse(message.value?.toString() ?? '');

      const resourceMap: ResourceMap = splitResources(bundle);

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

runs().catch((e: Error) =>
  console.error(`[kafka-unbundler-consumer] ${e.message}`, e)
);

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

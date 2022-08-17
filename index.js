const { Kafka, logLevel } = require('kafkajs');

const kafkaHost = process.env.KAFKA_HOST || 'localhost';
const kafkaPort = process.env.KAFKA_PORT || '9092';

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${kafkaHost}:${kafkaPort}`],
  clientId: 'kafka-processor-unbundler',
});

const topic = '2xx';
const consumer = kafka.consumer({ groupId: 'kafka-processor-unbundler' });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);

      const bundle = JSON.parse(message.value);

      resourceMap = {};
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
          .catch(e =>
            console.error(`[kafka-processor-unbundler] ${e.message}`, e)
          );
      });
    },
  });
};

run().catch(e => console.error(`[kafka-processor-unbundler] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
  process.on(type, async e => {
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

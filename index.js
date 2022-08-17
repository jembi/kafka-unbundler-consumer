"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const { Kafka, logLevel } = require('kafkajs');
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
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    yield producer.connect();
    yield consumer.subscribe({ topic, fromBeginning: true });
    yield consumer.run({
        eachMessage: ({ topic, partition, message, }) => __awaiter(void 0, void 0, void 0, function* () {
            const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
            console.log(`- ${prefix} ${message.key}#${message.value}`);
            const bundle = JSON.parse(message.value);
            let resourceMap = {};
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
                    .catch((e) => console.error(`[kafka-unbundler-consumer] ${e.message}`, e));
            });
        }),
    });
});
run().catch((e) => console.error(`[kafka-unbundler-consumer] ${e.message}`, e));
const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];
errorTypes.forEach(type => {
    process.on(type, (e) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            console.log(`process.on ${type}`);
            console.error(e);
            yield consumer.disconnect();
            yield producer.disconnect();
            process.exit(0);
        }
        catch (_) {
            process.exit(1);
        }
    }));
});
signalTraps.forEach(type => {
    process.once(type, () => __awaiter(void 0, void 0, void 0, function* () {
        try {
            yield consumer.disconnect();
            yield producer.disconnect();
        }
        finally {
            process.kill(process.pid, type);
        }
    }));
});

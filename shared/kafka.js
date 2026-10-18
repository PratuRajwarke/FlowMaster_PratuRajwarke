const { Kafka, logLevel } = require('kafkajs');

const TOPICS = {
  COMMANDS: 'workflow.commands',
  RESULTS: 'workflow.results',
  COMPENSATE: 'workflow.compensate',
};

function createKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    logLevel: logLevel.ERROR,
  });
}

module.exports = { createKafka, TOPICS };

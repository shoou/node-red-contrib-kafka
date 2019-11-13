const kafka = require('kafka-node');

module.exports = function(RED) {
  function kafkaIn(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const { kafkaHost, autoCommit, topics, debug, delayConsumerCreation, connectTimeout } = config;
    const client = new kafka.KafkaClient({ kafkaHost, connectTimeout });
    const consumerOptions = { autoCommit };

    let consumerCreationDelay = 0;
    if (delayConsumerCreation) {
      consumerCreationDelay = parseInt(connectTimeout) || 60000;
      node.log(`Consumer creation delayed by ${consumerCreationDelay} millisecondes`);
    }

    const createConsumer = () => {
      const consumer = new kafka.Consumer(
        client,
        topics.split(',').map(topic => ({ topic })),
        consumerOptions,
      );

      node.log('Consumer created.');
      node.status({ fill: 'green', shape: 'dot', text: `connected to ${kafkaHost}`});

      consumer.on('message', message => {
        if (debug) {
          console.log(message);
          node.log(message);
        }

        node.send({ payload: message });
      });

      consumer.on('error', error => {
        console.error(error);
        node.error(error);
      });
    };

    setTimeout(createConsumer, consumerCreationDelay);
  }

  RED.nodes.registerType('kafka-in', kafkaIn);

  function kafkaOut(config) {
    RED.nodes.createNode(this,config);
    const node = this;

    const { kafkaHost, topics, debug, connectTimeout } = config;
    const client = new kafka.Client({ kafkaHost });
    const producer = kafka.Producer(client);

    node.on('input', message => {
      const payloads = topics.join(',').map(topic => ({ topic, messages: message.payload }));

      producer.send(payloads, (error, data) => {
        if (error) {
          node.error(err);
        }

        if (debug) {
          console.log(`Message sent to Kafka: ${data}`);
          node.log(`Message sent to Kafka: ${data}`);
        }
      });
    });
  }

  RED.nodes.registerType('kafka-out', kafkaOut);
}
